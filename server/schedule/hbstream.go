// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package schedule

import (
	"context"
	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	defaultHeartbeatMsgCap      int = 1024
	defaultHeartbeatSendTimeout     = time.Duration(5) * time.Second
)

type sendReq struct {
	ctx  context.Context
	node string
	msg  *metapb.NodeHeartbeatResponse
}

// HeartbeatStreams manages all the streams connected by ceresdb node.
type HeartbeatStreams struct {
	ctx     context.Context
	cancel  context.CancelFunc
	bgJobWg *sync.WaitGroup

	reqCh       chan *sendReq
	mu          *sync.RWMutex
	nodeStreams map[string]HeartbeatSender
}

func NewHeartbeatStreams(ctx context.Context) *HeartbeatStreams {
	ctx, cancel := context.WithCancel(ctx)
	h := &HeartbeatStreams{
		ctx:     ctx,
		cancel:  cancel,
		bgJobWg: &sync.WaitGroup{},

		reqCh:       make(chan *metapb.NodeHeartbeatResponse, defaultHeartbeatMsgCap),
		mu:          &sync.RWMutex{},
		nodeStreams: make(map[string]HeartbeatSender),
	}

	go h.runBgJob()

	return h
}

type HeartbeatSender interface {
	Send(response *metapb.NodeHeartbeatResponse) error
}

func (h *HeartbeatStreams) runBgJob() {
	h.bgJobWg.Add(1)
	defer h.bgJobWg.Done()

	for {
		select {
		case hbMsg := <-h.reqCh:
			err := h.sendMsgOnce(hbMsg)
			if err != nil {
				log.Error("fail to send msg", zap.Error(err), zap.String("node", hbMsg.node), zap.Any("msg", hbMsg.msg))
			}
		case <-h.ctx.Done():
			log.Warn("exit from background jobs")
			return
		}
	}
}

func (h *HeartbeatStreams) sendMsgOnce(hbMsg *sendReq) error {
	sender := h.getStream(hbMsg.node)
	if sender == nil {
		return ErrStreamNotAvailable
	}

	ctx, cancel := context.WithCancel(hbMsg.ctx)
	timer := time.AfterFunc(defaultHeartbeatSendTimeout, func() { cancel() })
	if err := sender.Send(hbMsg.msg); err != nil {
		return ErrStreamSendMsg
	}

	return nil
}

// getStream finds and returns the sender bound to the node, and it is nil if not exist.
func (h *HeartbeatStreams) getStream(node string) HeartbeatSender {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.nodeStreams[node]
}

func (h *HeartbeatStreams) Bind(node string, sender HeartbeatSender) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nodeStreams[node] = sender
}

func (h *HeartbeatStreams) Unbound(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.nodeStreams, node)
}

// SendMsgAsync sends messages to node and this procedure is asynchronous.
func (h *HeartbeatStreams) SendMsgAsync(ctx context.Context, node string, msg *metapb.NodeHeartbeatResponse) error {
	hbMsg := &sendReq{
		ctx,
		node,
		msg,
	}

	select {
	case h.reqCh <- hbMsg:
		return nil
	case <-ctx.Done():
		return ErrStreamSendTimeout
	case <-h.ctx.Done():
		return ErrHeartbeatStreamsClosed
	}
}
