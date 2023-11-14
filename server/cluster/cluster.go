/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler/manager"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultProcedurePrefixKey = "procedure"
	defaultAllocStep          = 500
)

type Cluster struct {
	logger   *zap.Logger
	metadata *metadata.ClusterMetadata

	procedureFactory *coordinator.Factory
	procedureManager procedure.Manager
	schedulerManager manager.SchedulerManager
}

func NewCluster(logger *zap.Logger, metadata *metadata.ClusterMetadata, client *clientv3.Client, rootPath string) (*Cluster, error) {
	procedureStorage := procedure.NewEtcdStorageImpl(client, rootPath, uint32(metadata.GetClusterID()))
	procedureManager, err := procedure.NewManagerImpl(logger, metadata)
	if err != nil {
		return nil, errors.WithMessage(err, "create procedure manager")
	}
	dispatch := eventdispatch.NewDispatchImpl()
	procedureFactory := coordinator.NewFactory(logger, id.NewAllocatorImpl(logger, client, defaultProcedurePrefixKey, defaultAllocStep), dispatch, procedureStorage)

	schedulerManager := manager.NewManager(logger, procedureManager, procedureFactory, metadata, client, rootPath, metadata.GetEnableSchedule(), metadata.GetTopologyType(), metadata.GetProcedureExecutingBatchSize())

	return &Cluster{
		logger:           logger,
		metadata:         metadata,
		procedureFactory: procedureFactory,
		procedureManager: procedureManager,
		schedulerManager: schedulerManager,
	}, nil
}

func (c *Cluster) Start(ctx context.Context) error {
	if err := c.procedureManager.Start(ctx); err != nil {
		return errors.WithMessage(err, "start procedure manager")
	}
	if err := c.schedulerManager.Start(ctx); err != nil {
		return errors.WithMessage(err, "start scheduler manager")
	}
	return nil
}

func (c *Cluster) Stop(ctx context.Context) error {
	if err := c.procedureManager.Stop(ctx); err != nil {
		return errors.WithMessage(err, "stop procedure manager")
	}
	if err := c.schedulerManager.Stop(ctx); err != nil {
		return errors.WithMessage(err, "stop scheduler manager")
	}
	return nil
}

func (c *Cluster) GetMetadata() *metadata.ClusterMetadata {
	return c.metadata
}

func (c *Cluster) GetProcedureManager() procedure.Manager {
	return c.procedureManager
}

func (c *Cluster) GetProcedureFactory() *coordinator.Factory {
	return c.procedureFactory
}

func (c *Cluster) GetSchedulerManager() manager.SchedulerManager {
	return c.schedulerManager
}

func (c *Cluster) GetShards() []storage.ShardID {
	return c.metadata.GetShards()
}

func (c *Cluster) GetShardNodes() metadata.GetShardNodesResult {
	return c.metadata.GetShardNodes()
}
