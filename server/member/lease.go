// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// lease helps use etcd lease by providing Grant, Close and auto renewing the lease.
type lease struct {
	rawLease clientv3.Lease
	// timeout is the rpc timeout and always equals to the ttlSec.
	timeout time.Duration
	ttlSec  int64
	// logger will be updated after Grant is called.
	logger *zap.Logger

	// The fields below are initialized after Grant is called.
	ID clientv3.LeaseID

	expireTimeL sync.RWMutex
	// expireTime helps determine the lease whether is expired.
	expireTime time.Time
}

func newLease(rawLease clientv3.Lease, ttlSec int64) *lease {
	return &lease{
		rawLease: rawLease,
		timeout:  time.Duration(ttlSec) * time.Second,
		ttlSec:   ttlSec,
		logger:   log.GetLogger(),
	}
}

func (l *lease) Grant(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	leaseResp, err := l.rawLease.Grant(ctx, l.ttlSec)
	if err != nil {
		return ErrGrantLease.WithCause(err)
	}

	l.ID = leaseResp.ID
	l.logger = log.With(zap.Int64("lease-id", int64(leaseResp.ID)))

	expiredAt := time.Now().Add(time.Second * time.Duration(leaseResp.TTL))
	l.setExpireTime(expiredAt)

	l.logger.Debug("lease is granted", zap.Time("expired-at", expiredAt))
	return nil
}

func (l *lease) Close(ctx context.Context) error {
	// Check whether the lease was granted.
	if l.ID == 0 {
		return nil
	}

	// Release and reset all the resources.
	l.setExpireTime(time.Time{})
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	if _, err := l.rawLease.Revoke(ctx, l.ID); err != nil {
		return ErrRevokeLease.WithCause(err)
	}
	if err := l.rawLease.Close(); err != nil {
		return ErrCloseLease.WithCause(err)
	}

	return nil
}

// KeepAlive renews the lease until timeout for renewing lease.
func (l *lease) KeepAlive(ctx context.Context) {
	// Used to receive the renewed event.
	renewed := make(chan bool, 1)
	ctx1, cancelRenewBg := context.WithCancel(ctx)
	// Used to join with the renew goroutine in the background.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		l.renewLeaseBg(ctx1, renewed)
		wg.Done()
	}()

L:
	for {
		select {
		case alive := <-renewed:
			l.logger.Debug("received renew result", zap.Bool("renew-alive", alive))
			if !alive {
				break L
			}
		case <-time.After(l.timeout):
			l.logger.Warn("lease timeout, stop keeping lease alive")
			break L
		case <-ctx.Done():
			l.logger.Info("stop keeping lease alive because ctx is done")
			break L
		}
	}

	cancelRenewBg()
	wg.Wait()
}

// IsExpired is goroutine safe.
func (l *lease) IsExpired() bool {
	expiredAt := l.getExpireTime()
	return time.Now().After(expiredAt)
}

func (l *lease) setExpireTime(newExpireTime time.Time) {
	l.expireTimeL.Lock()
	defer l.expireTimeL.Unlock()

	l.expireTime = newExpireTime
}

// `setExpireTimeIfNewer` updates the l.expireTime only if the newExpireTime is after l.expireTime.
// Returns true if l.expireTime is updated.
func (l *lease) setExpireTimeIfNewer(newExpireTime time.Time) bool {
	l.expireTimeL.Lock()
	defer l.expireTimeL.Unlock()

	if newExpireTime.After(l.expireTime) {
		l.expireTime = newExpireTime
		return true
	}

	return false
}

func (l *lease) getExpireTime() time.Time {
	l.expireTimeL.RLock()
	defer l.expireTimeL.RUnlock()

	return l.expireTime
}

// `renewLeaseBg` keeps the lease alive by periodically call `lease.KeepAliveOnce`.
// The l.expireTime will be updated during renewing and the renew lease result (whether alive) will be told to caller by `renewed` channel.
func (l *lease) renewLeaseBg(ctx context.Context, renewed chan<- bool) {
	l.logger.Info("start renewing lease background", zap.Int64("lease-id", int64(l.ID)))
	defer l.logger.Info("stop renewing lease background", zap.Int64("lease-id", int64(l.ID)))

	keepaliveStream, err := l.rawLease.KeepAlive(ctx, l.ID)
	l.logger.Error("fail to keep lease alive", zap.Int64("lease-id", int64(l.ID)), zap.Error(err))
	for {
		start := time.Now()
		select {
		case resp, ok := <-keepaliveStream:
			if !ok {
				l.logger.Error("keepalive stream is closed", zap.Int64("lease-id", int64(l.ID)))
				renewed <- false
				return
			}
			if resp.TTL < 0 {
				l.logger.Warn("lease is expired", zap.Int64("lease-id", int64(l.ID)))
				renewed <- false
				return
			}

			expireAt := start.Add(time.Duration(resp.TTL) * time.Second)
			updated := l.setExpireTimeIfNewer(expireAt)
			renewed <- true
			l.logger.Debug("got next expired time", zap.Time("expired-at", expireAt), zap.Bool("updated", updated), zap.Int64("lease-id", int64(l.ID)))
		case <-ctx.Done():
			l.logger.Info("stop renewing lease background because ctx is done", zap.Int64("lease-id", int64(l.ID)))
			return
		}
	}
}
