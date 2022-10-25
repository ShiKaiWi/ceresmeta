// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type metaData struct {
	cluster         *clusterpb.Cluster
	clusterTopology *clusterpb.ClusterTopology
}

type Cluster struct {
	clusterID uint32

	// RWMutex is used to protect following fields.
	// TODO: Encapsulated maps as a specific struct.
	lock     sync.RWMutex
	metaData *metaData
	// The two fields describes the whole topology of the cluster.
	// TODO: merge `shardsCache` & `nodeShardsCache` into the whole topology.
	shardsCache     map[uint32]*Shard        // shardID -> shard
	nodeShardsCache map[string]*ShardsOfNode // nodeName -> shards of the node

	// Manage tables by schema.
	schemasCache map[string]*Schema // schemaName -> schema

	// Manage the registered nodes from heartbeat.
	registeredNodesCache map[string]*RegisteredNode // nodeName -> node

	storage       storage.Storage
	kv            clientv3.KV
	schemaIDAlloc id.Allocator
	tableIDAlloc  id.Allocator
	shardIDAlloc  id.Allocator
}

// FIXME: For now, not all the returned values by cluster methods are deep-copied, which may lead to data race, let's do deep copy for returned values.

func (c *Cluster) GetClusterShardView() ([]*clusterpb.Shard, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// FIXME: Should use the shardsCache & nodeShardsCache to build the view.
	shardView := c.metaData.clusterTopology.ShardView
	newShardView := make([]*clusterpb.Shard, 0, len(shardView))
	// TODO: We need to use the general deep copy tool method to replace.
	for _, shard := range shardView {
		copyShard := &clusterpb.Shard{
			Id:        shard.Id,
			ShardRole: shard.ShardRole,
			Node:      shard.Node,
		}
		newShardView = append(newShardView, copyShard)
	}
	return newShardView, nil
}

func (c *Cluster) GetClusterID() uint32 {
	return c.clusterID
}

func NewCluster(meta *clusterpb.Cluster, storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) *Cluster {
	cluster := &Cluster{
		clusterID:            meta.GetId(),
		metaData:             &metaData{cluster: meta},
		shardsCache:          map[uint32]*Shard{},
		nodeShardsCache:      map[string]*ShardsOfNode{},
		schemasCache:         map[string]*Schema{},
		registeredNodesCache: map[string]*RegisteredNode{},
		schemaIDAlloc:        id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocSchemaIDPrefix), idAllocatorStep),
		tableIDAlloc:         id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocTableIDPrefix), idAllocatorStep),
		// TODO: Load ShardTopology when cluster create, pass exist shardID to allocator.
		shardIDAlloc: id.NewReusableAllocatorImpl([]uint64{}, MinShardID),

		storage: storage,
		kv:      kv,
	}

	return cluster
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

// Initialize the cluster topology and shard topology of the cluster.
// It will be used when we create the cluster.
func (c *Cluster) init(ctx context.Context) error {
	clusterTopologyPb := &clusterpb.ClusterTopology{
		ClusterId: c.clusterID, Version: 0,
		State: clusterpb.ClusterTopology_EMPTY,
	}
	if clusterTopologyPb, err := c.storage.CreateClusterTopology(ctx, clusterTopologyPb); err != nil {
		return errors.WithMessagef(err, "create cluster topology, clusterTopology:%v", clusterTopologyPb)
	}

	c.metaData.clusterTopology = clusterTopologyPb
	return nil
}

// Load data from storage to memory.
func (c *Cluster) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	shards, shardIDs, err := c.loadClusterTopologyLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "load cluster topology")
	}

	shardTopologies, err := c.loadShardTopologyLocked(ctx, shardIDs)
	if err != nil {
		return errors.WithMessage(err, "load shard topology")
	}

	schemas, err := c.loadSchemaLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "load schema")
	}

	nodes, err := c.loadNodeLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "load node")
	}

	tables, err := c.loadTableLocked(ctx, schemas)
	if err != nil {
		return errors.WithMessage(err, "load table")
	}

	if err := c.loadCacheLocked(shards, shardTopologies, schemas, nodes, tables); err != nil {
		return errors.WithMessage(err, "load cache")
	}
	return nil
}

func (c *Cluster) loadCacheLocked(
	shards map[uint32][]*clusterpb.Shard,
	shardTopologies map[uint32]*clusterpb.ShardTopology,
	schemasLoaded map[string]*clusterpb.Schema,
	nodesLoaded map[string]*clusterpb.Node,
	tablesLoaded map[string]map[uint64]*clusterpb.Table,
) error {
	// Load schemaCache: load all the schemas.
	for _, schema := range schemasLoaded {
		c.updateSchemaCacheLocked(schema)
	}

	// Load schemasCache: load the tables.
	for schemaName, tables := range tablesLoaded {
		for _, table := range tables {
			_, ok := c.schemasCache[schemaName]
			if ok {
				c.schemasCache[schemaName].tableMap[table.GetName()] = &Table{
					schema: schemasLoaded[schemaName],
					meta:   table,
				}
			} else {
				c.schemasCache[schemaName] = &Schema{
					meta: schemasLoaded[schemaName],
					tableMap: map[string]*Table{table.GetName(): {
						schema: schemasLoaded[schemaName],
						meta:   table,
					}},
				}
			}
		}
	}

	// Update nodeShardsCache.
	for shardID, shardPbs := range shards {
		for _, shard := range shardPbs {
			shardsOfNode, ok := c.nodeShardsCache[shard.GetNode()]
			if !ok {
				shardsOfNode = &ShardsOfNode{
					Endpoint: shard.GetNode(),
					ShardIDs: []uint32{},
				}
				c.nodeShardsCache[shard.GetNode()] = shardsOfNode
			}
			shardsOfNode.ShardIDs = append(shardsOfNode.ShardIDs, shardID)
		}
	}

	// Load registeredNodeCache.
	for _, node := range nodesLoaded {
		registerNode := NewRegisteredNode(node, []*ShardInfo{})
		c.registeredNodesCache[node.Name] = registerNode
	}

	// Load shardsCache.
	for shardID, shardTopology := range shardTopologies {
		tables := make(map[uint64]*Table, len(shardTopology.TableIds))

		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range tablesLoaded {
				table, ok := tableMap[tableID]
				if ok {
					tables[tableID] = &Table{
						schema: schemasLoaded[schemaName],
						meta:   table,
					}
				}
			}
		}
		// TODO: assert shardID
		// TODO: check shard not found by shardID
		shardMetaList := shards[shardID]
		var nodeMetas []*clusterpb.Node
		for _, shardMeta := range shardMetaList {
			if node := c.registeredNodesCache[shardMeta.Node]; node != nil {
				nodeMetas = append(nodeMetas, node.meta)
			}
		}
		c.shardsCache[shardID] = &Shard{
			meta:    shards[shardID],
			nodes:   nodeMetas,
			tables:  tables,
			version: shardTopology.Version,
		}
	}

	return nil
}

func (c *Cluster) updateSchemaCacheLocked(schemaPb *clusterpb.Schema) *Schema {
	schema := &Schema{meta: schemaPb, tableMap: make(map[string]*Table, 0)}
	c.schemasCache[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(shardID uint32, schema *Schema, tablePb *clusterpb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta, shardID: shardID}
	schema.tableMap[tablePb.GetName()] = table
	c.shardsCache[tablePb.GetShardId()].tables[table.GetID()] = table
	return table
}

func (c *Cluster) updateShardVersionLocked(shardID uint32, prevVersion, newVersion uint64) (*ShardVersionUpdate, error) {
	shard, ok := c.shardsCache[shardID]
	if !ok {
		return nil, ErrShardNotFound
	}

	if shard.version != prevVersion {
		panic(fmt.Sprintf("shardId:%d, storage version:%d, memory version:%d", shardID, prevVersion, shard.version))
	}

	shard.version = newVersion
	return &ShardVersionUpdate{
		ShardID:     shardID,
		CurrVersion: newVersion,
		PrevVersion: prevVersion,
	}, nil
}

func (c *Cluster) getShardTopologyFromStorage(ctx context.Context, shardID uint32) (*clusterpb.ShardTopology, error) {
	shardTopologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, []uint32{shardID})
	if err != nil {
		return nil, errors.WithMessage(err, "get shard topology from storage")
	}
	if len(shardTopologies) != 1 {
		return nil, ErrGetShardTopology.WithCausef("shard has more than one shard topology, shardID:%d, shardTopologies:%v",
			shardID, shardTopologies)
	}
	return shardTopologies[0], nil
}

func (c *Cluster) createTableOnShardLocked(ctx context.Context, shardID uint32, schema *Schema, tablePb *clusterpb.Table) (*CreateTableResult, error) {
	// Update shardTopology in storage.
	shardTopology, err := c.getShardTopologyFromStorage(ctx, shardID)
	if err != nil {
		return nil, err
	}
	shardTopology.TableIds = append(shardTopology.TableIds, tablePb.GetId())
	prevVersion := shardTopology.Version
	shardTopology.Version = prevVersion + 1
	if err = c.storage.PutShardTopology(ctx, c.clusterID, prevVersion, shardTopology); err != nil {
		return nil, errors.WithMessage(err, "put shard topology")
	}

	// Update tableCache in memory.
	table, shardVersion, err := c.createTableInCache(shardID, schema, tablePb, prevVersion, shardTopology.Version)
	if err != nil {
		return nil, errors.WithMessage(err, "create table in cache")
	}

	return &CreateTableResult{
		Table:              table,
		ShardVersionUpdate: shardVersion,
	}, nil
}

func (c *Cluster) createTableInCache(shardID uint32, schema *Schema, tablePb *clusterpb.Table, prevVersion, newVersion uint64) (*Table, *ShardVersionUpdate, error) {
	table := c.updateTableCacheLocked(shardID, schema, tablePb)
	shardVersion, err := c.updateShardVersionLocked(shardID, prevVersion, newVersion)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "update shard topology version")
	}
	return table, shardVersion, nil
}

func (c *Cluster) dropTableUpdateCacheLocked(ctx context.Context, shardID uint32, schema *Schema, table *Table) (*DropTableResult, error) {
	// Update shardTopology in storage.
	shardTopology, err := c.getShardTopologyFromStorage(ctx, shardID)
	if err != nil {
		return nil, err
	}

	// Remove table in shardTopology.
	found := false
	for i, id := range shardTopology.TableIds {
		if id == table.GetID() {
			found = true
			shardTopology.TableIds = append(shardTopology.TableIds[:i], shardTopology.TableIds[i+1:]...)
			break
		}
	}

	if !found {
		panic(fmt.Sprintf("shard topology dose not contain table, schema:%s, shard topology:%v, table id:%d", schema.GetName(), shardTopology, table.GetID()))
	}

	prevVersion := shardTopology.Version
	shardTopology.Version = prevVersion + 1
	if err = c.storage.PutShardTopology(ctx, c.clusterID, prevVersion, shardTopology); err != nil {
		return nil, errors.WithMessage(err, "put shard topology")
	}

	// Update tableCache in memory.
	shardVersion, err := c.dropTableInCache(shardID, schema, table, prevVersion, shardTopology.Version)
	if err != nil {
		return nil, errors.WithMessage(err, "drop table in cache")
	}

	return &DropTableResult{
		ShardVersionUpdate: shardVersion,
	}, nil
}

func (c *Cluster) dropTableInCache(shardID uint32, schema *Schema, table *Table, prevVersion, newVersion uint64) (*ShardVersionUpdate, error) {
	// Drop table in schemaCache.
	schema.dropTableLocked(table.GetName())
	// Drop table in shardCache.
	shard, err := c.getShardByIDLocked(shardID)
	if err != nil {
		return nil, errors.WithMessage(err, "update shard topology version")
	}
	shard.dropTableLocked(table.GetID())
	// Update shard topology version.
	shardVersion, err := c.updateShardVersionLocked(shardID, prevVersion, newVersion)
	if err != nil {
		return nil, errors.WithMessage(err, "update shard topology version")
	}

	return shardVersion, nil
}

func (c *Cluster) getTableShardIDLocked(tableID uint64) (uint32, error) {
	for id, shard := range c.shardsCache {
		if _, ok := shard.tables[tableID]; ok {
			return id, nil
		}
	}
	return 0, ErrShardNotFound.WithCausef("get table shardID, tableID:%d", tableID)
}

func (c *Cluster) GetTables(_ context.Context, shardIDs []uint32, nodeName string) (map[uint32]*ShardTablesWithRole, error) {
	// TODO: refactor more fine-grained locks
	c.lock.RLock()
	defer c.lock.RUnlock()

	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shard, ok := c.shardsCache[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
		}

		shardRole := clusterpb.ShardRole_FOLLOWER
		found := false
		for i, n := range shard.nodes {
			if nodeName == n.GetName() {
				found = true
				shardRole = shard.meta[i].ShardRole
				break
			}
		}
		if !found {
			return nil, ErrNodeNotFound.WithCausef("nodeName not found in current shard, shardID:%d, nodeName:%s", shardID, nodeName)
		}

		tables := make([]*Table, 0, len(shard.tables))
		for _, table := range shard.tables {
			tables = append(tables, table)
		}
		shardTables[shardID] = &ShardTablesWithRole{shard: &ShardInfo{
			ID:      shardID,
			Role:    shardRole,
			Version: shard.version,
		}, tables: tables}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string) (*DropTableResult, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	table, ok := schema.getTable(tableName)
	if !ok {
		return nil, ErrTableNotFound
	}

	if err := c.storage.DeleteTable(ctx, c.clusterID, schema.GetID(), tableName); err != nil {
		return nil, errors.WithMessagef(err, "storage drop table, clusterID:%d, schema:%v, tableName:%s",
			c.clusterID, schema, tableName)
	}

	shardID := table.GetShardID()

	// Update shardTopology in storage.
	result, err := c.dropTableUpdateCacheLocked(ctx, shardID, schema, table)
	if err != nil {
		return nil, errors.WithMessagef(err, "drop table update cache, clusterID:%d, schema:%v, tableName:%s",
			c.clusterID, schema, tableName)
	}
	return result, nil
}

// GetOrCreateSchema the second output parameter bool: Returns true if the schema was newly created.
func (c *Cluster) GetOrCreateSchema(ctx context.Context, schemaName string) (*Schema, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if provided schema exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if exists {
		return schema, true, nil
	}

	schemaID, err := c.allocSchemaID(ctx)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "cluster AllocSchemaID, schemaName:%s", schemaName)
	}

	// Save schema in storage.
	schemaPb := &clusterpb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	schemaPb, err = c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, false, errors.WithMessage(err, "cluster CreateSchema")
	}

	// Update schemasCache in memory.
	schema = c.updateSchemaCacheLocked(schemaPb)
	return schema, false, nil
}

func (c *Cluster) GetTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	c.lock.RLock()
	schema, ok := c.schemasCache[schemaName]
	if !ok {
		c.lock.RUnlock()
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	table, exists := schema.getTable(tableName)
	if exists {
		c.lock.RUnlock()
		return table, true, nil
	}
	c.lock.RUnlock()

	// Search Table in storage.
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.WithMessage(err, "get table from storage")
	}
	if exists {
		c.lock.Lock()
		defer c.lock.Unlock()

		shardID, err := c.getTableShardIDLocked(tablePb.GetId())
		if err != nil {
			return nil, false, errors.WithMessage(err, "get shard id")
		}
		table = c.updateTableCacheLocked(shardID, schema, tablePb)
		return table, true, nil
	}

	return nil, false, nil
}

func (c *Cluster) CreateTable(ctx context.Context, nodeName string, schemaName string, tableName string) (*CreateTableResult, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check provided schema if exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	// check if exists
	_, exists = c.getTableLocked(schemaName, tableName)
	if exists {
		return nil, ErrTableAlreadyExists
	}

	shardID, err := c.pickOneShardOnNode(nodeName)
	if err != nil {
		return nil, errors.WithMessagef(err, "pick one shard on node, clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", c.Name(), schemaName, tableName, nodeName)
	}

	// Alloc table id.
	tableID, err := c.allocTableID(ctx)
	if err != nil {
		return nil, errors.WithMessagef(err, "alloc table id, schemaName:%s, tableName:%s", schemaName, tableName)
	}

	// Save table in storage.
	tablePb := &clusterpb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	tablePb, err = c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err != nil {
		return nil, errors.WithMessage(err, "storage create table")
	}
	result, err := c.createTableOnShardLocked(ctx, shardID, schema, tablePb)
	if err != nil {
		return nil, errors.WithMessagef(err, "create table update topology, clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", c.Name(), schemaName, tableName, nodeName)
	}

	return result, nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) (map[uint32][]*clusterpb.Shard, []uint32, error) {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "cluster loadClusterTopologyLocked")
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return nil, nil, ErrClusterTopologyNotFound.WithCausef("cluster:%v", c)
	}

	shardMap := map[uint32][]*clusterpb.Shard{}
	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
	}

	shardIDs := make([]uint32, 0, len(shardMap))
	for id := range shardMap {
		shardIDs = append(shardIDs, id)
	}

	return shardMap, shardIDs, nil
}

func (c *Cluster) loadShardTopologyLocked(ctx context.Context, shardIDs []uint32) (map[uint32]*clusterpb.ShardTopology, error) {
	topologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, shardIDs)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadShardTopologyLocked")
	}
	shardTopologyMap := make(map[uint32]*clusterpb.ShardTopology, len(shardIDs))
	for _, topology := range topologies {
		shardTopologyMap[topology.GetShardId()] = topology
	}
	return shardTopologyMap, nil
}

func (c *Cluster) loadSchemaLocked(ctx context.Context) (map[string]*clusterpb.Schema, error) {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadSchemaLocked")
	}
	schemaMap := make(map[string]*clusterpb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	return schemaMap, nil
}

func (c *Cluster) loadNodeLocked(ctx context.Context) (map[string]*clusterpb.Node, error) {
	nodes, err := c.storage.ListNodes(ctx, c.clusterID)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadNodeLocked")
	}

	nameNodes := make(map[string]*clusterpb.Node, len(nodes))
	for _, node := range nodes {
		nameNodes[node.Name] = node
	}
	return nameNodes, nil
}

func (c *Cluster) loadTableLocked(ctx context.Context, schemas map[string]*clusterpb.Schema) (map[string]map[uint64]*clusterpb.Table, error) {
	tables := make(map[string]map[uint64]*clusterpb.Table)
	for _, schema := range schemas {
		tablePbs, err := c.storage.ListTables(ctx, c.clusterID, schema.Id)
		if err != nil {
			return nil, errors.WithMessage(err, "cluster loadTableLocked")
		}
		for _, table := range tablePbs {
			if t, ok := tables[schema.GetName()]; ok {
				t[table.GetId()] = table
			} else {
				tables[schema.GetName()] = map[uint64]*clusterpb.Table{table.GetId(): table}
			}
		}
	}
	return tables, nil
}

func (c *Cluster) getSchemaLocked(schemaName string) (*Schema, bool) {
	schema, ok := c.schemasCache[schemaName]
	return schema, ok
}

func (c *Cluster) getTableLocked(schemaName string, tableName string) (*Table, bool) {
	table, ok := c.schemasCache[schemaName].tableMap[tableName]
	return table, ok
}

// GetShardByID return immutable `Shard`.
func (c *Cluster) GetShardByID(id uint32) (*Shard, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.getShardByIDLocked(id)
}

// GetShardByID return immutable `Shard`.
func (c *Cluster) getShardByIDLocked(id uint32) (*Shard, error) {
	shard, ok := c.shardsCache[id]
	if !ok {
		return nil, ErrShardNotFound.WithCausef("cluster GetShardByIDLocked, shardID:%d", id)
	}
	return shard, nil
}

func (c *Cluster) GetShardIDsByNode(nodeName string) ([]uint32, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	shardsOfNode, ok := c.nodeShardsCache[nodeName]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("cluster get shard ids by node, nodeName:%s", nodeName)
	}
	return shardsOfNode.ShardIDs, nil
}

func (c *Cluster) RegisterNode(ctx context.Context, nodeInfo *metaservicepb.NodeInfo) error {
	// FIXME: add specific method to do conversion from `metaservicepb.NodeInfo` to `clusterpb.Node`.
	nodeStats := &clusterpb.NodeStats{
		Lease:       nodeInfo.Lease,
		Zone:        nodeInfo.Zone,
		NodeVersion: nodeInfo.BinaryVersion,
	}
	nodePb := &clusterpb.Node{NodeStats: nodeStats, Name: nodeInfo.GetEndpoint()}

	newNode, err := c.storage.CreateOrUpdateNode(ctx, c.clusterID, nodePb)
	if err != nil {
		return errors.WithMessagef(err, "cluster RegisterNode, nodeName:%s", nodeInfo.GetEndpoint())
	}

	shardInfos := make([]*ShardInfo, 0, len(nodeInfo.ShardInfos))
	for _, shardInfo := range nodeInfo.ShardInfos {
		shardInfo := &ShardInfo{
			ID:      shardInfo.Id,
			Role:    shardInfo.Role,
			Version: shardInfo.Version,
		}
		shardInfos = append(shardInfos, shardInfo)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	newRegisterNode := NewRegisteredNode(newNode, shardInfos)
	c.registeredNodesCache[nodeInfo.GetEndpoint()] = newRegisterNode

	return nil
}

func (c *Cluster) GetRegisteredNodes() []*RegisteredNode {
	c.lock.RLock()
	defer c.lock.RUnlock()

	nodes := make([]*RegisteredNode, 0, len(c.registeredNodesCache))
	for _, node := range c.registeredNodesCache {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Cluster) GetRegisteredNode(nodeName string) (*RegisteredNode, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	registeredNode, ok := c.registeredNodesCache[nodeName]
	return registeredNode, ok
}

func (c *Cluster) allocSchemaID(ctx context.Context) (uint32, error) {
	id, err := c.schemaIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster alloc schema id failed")
	}
	return uint32(id), nil
}

func (c *Cluster) allocTableID(ctx context.Context) (uint64, error) {
	id, err := c.tableIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc table id failed")
	}
	return id, nil
}

func (c *Cluster) AllocShardID(ctx context.Context) (uint32, error) {
	id, err := c.shardIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster alloc shard id failed")
	}
	return uint32(id), nil
}

func (c *Cluster) pickOneShardOnNode(nodeName string) (uint32, error) {
	if shardsOfNode, ok := c.nodeShardsCache[nodeName]; ok {
		shardIDs := shardsOfNode.ShardIDs
		if len(shardIDs) == 0 {
			return 0, ErrNodeShardsIsEmpty.WithCausef("nodeName:%s", nodeName)
		}

		idx := rand.Int31n(int32(len((shardIDs)))) // #nosec G404
		return shardIDs[idx], nil
	}
	return 0, ErrNodeNotFound.WithCausef("nodeName:%s", nodeName)
}

func (c *Cluster) RouteTables(_ context.Context, schemaName string, tableNames []string) (*RouteTablesResult, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	schema, ok := c.schemasCache[schemaName]
	if !ok {
		routeEntries := make(map[string]*RouteEntry)

		return &RouteTablesResult{
			Version:      c.metaData.clusterTopology.Version,
			RouteEntries: routeEntries,
		}, nil
	}

	routeEntries := make(map[string]*RouteEntry, len(tableNames))
	for _, tableName := range tableNames {
		table, exists := schema.getTable(tableName)
		if exists {
			shard, err := c.GetShardByID(table.GetShardID())
			if err != nil {
				return nil, errors.WithMessage(err, fmt.Sprintf("shard not found, shardID:%d", table.GetShardID()))
			}

			nodeShards := make([]*NodeShard, 0, len(shard.nodes))
			for i, node := range shard.nodes {
				nodeShards = append(nodeShards, &NodeShard{
					Endpoint: node.GetName(),
					ShardInfo: &ShardInfo{
						ID:   shard.meta[i].GetId(),
						Role: shard.meta[i].GetShardRole(),
					},
				})
			}

			routeEntries[tableName] = &RouteEntry{
				Table: &TableInfo{
					ID:         table.GetID(),
					Name:       table.GetName(),
					SchemaID:   table.GetSchemaID(),
					SchemaName: table.GetSchemaName(),
				},
				NodeShards: nodeShards,
			}
		}
	}

	return &RouteTablesResult{
		Version:      c.metaData.clusterTopology.Version,
		RouteEntries: routeEntries,
	}, nil
}

func (c *Cluster) GetNodeShards(_ context.Context) (*GetNodeShardsResult, error) {
	nodeShards := make([]*NodeShard, 0, len(c.nodeShardsCache))

	c.lock.RLock()
	defer c.lock.RUnlock()

	for nodeName, shardsOfNode := range c.nodeShardsCache {
		for _, shardID := range shardsOfNode.ShardIDs {
			shard, ok := c.shardsCache[shardID]
			if !ok {
				return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
			}

			shardMeta, ok := shard.FindShardByNode(nodeName)
			if !ok {
				return nil, ErrShardNotFound.WithCausef("fail to find shard from the cache")
			}

			nodeShards = append(nodeShards, &NodeShard{
				Endpoint: nodeName,
				ShardInfo: &ShardInfo{
					ID:      shardID,
					Role:    shardMeta.ShardRole,
					Version: shard.GetVersion(),
				},
			})
		}
	}

	return &GetNodeShardsResult{
		ClusterTopologyVersion: c.metaData.clusterTopology.GetVersion(),
		NodeShards:             nodeShards,
	}, nil
}

func (c *Cluster) GetClusterVersion() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.clusterTopology.Version
}

func (c *Cluster) GetClusterMinNodeCount() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.cluster.MinNodeCount
}

func (c *Cluster) GetTotalShardNum() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.cluster.ShardTotal
}

func (c *Cluster) GetClusterState() clusterpb.ClusterTopology_ClusterState {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.clusterTopology.State
}

func (c *Cluster) CreateShardTopologies(ctx context.Context, state clusterpb.ClusterTopology_ClusterState, shardTopologies []*clusterpb.ShardTopology, shardView []*clusterpb.Shard) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, err := c.storage.CreateShardTopologies(ctx, c.clusterID, shardTopologies)
	if err != nil {
		return errors.WithMessage(err, "cluster create shard topologies failed")
	}

	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.GetClusterID())
	if err != nil {
		return errors.WithMessage(err, "UpdateClusterTopology failed")
	}
	clusterTopology.ShardView = shardView
	clusterTopology.State = state

	if err = c.storage.PutClusterTopology(ctx, c.clusterID, c.metaData.clusterTopology.Version, clusterTopology); err != nil {
		return err
	}
	c.metaData.clusterTopology = clusterTopology

	return c.updateTopologyCache(shardTopologies, shardView)
}

func (c *Cluster) updateTopologyCache(shardTopologies []*clusterpb.ShardTopology, shardView []*clusterpb.Shard) error {
	shardsByID := make(map[uint32]*clusterpb.Shard, len(shardView))
	for _, shard := range shardView {
		shardsByID[shard.Id] = shard
	}

	newNodeShardsCache := map[string]*ShardsOfNode{}
	newShardsCache := make(map[uint32]*Shard, len(shardTopologies))

	for _, shardTopology := range shardTopologies {
		shard, ok := shardsByID[shardTopology.ShardId]
		if !ok {
			return ErrShardNotFound.WithCausef("updateTopologyCache missing shard in shardView, shard_id:%d", shardTopology.ShardId)
		}
		nodeName := shard.Node

		nodeShards, ok := newNodeShardsCache[nodeName]
		if !ok {
			nodeShards = &ShardsOfNode{
				Endpoint: nodeName,
				ShardIDs: []uint32{},
			}
			newNodeShardsCache[nodeName] = nodeShards
		}
		nodeShards.ShardIDs = append(nodeShards.ShardIDs, shardTopology.ShardId)

		cachedShard, ok := newShardsCache[shardTopology.ShardId]
		if !ok {
			cachedShard = &Shard{
				meta:    []*clusterpb.Shard{},
				nodes:   []*clusterpb.Node{},
				tables:  map[uint64]*Table{},
				version: shardTopology.Version,
			}
			newShardsCache[shardTopology.ShardId] = cachedShard
		}
		cachedShard.meta = append(cachedShard.meta, shard)
		// TODO: Here shardsCache should not contain the register node information (shardsCache will be refactored in the future),
		//  so here no need to set these missing fields in clusterpb.Node.
		nodePb := &clusterpb.Node{
			Name: shard.Node,
		}
		cachedShard.nodes = append(cachedShard.nodes, nodePb)
	}

	for nodeName, shardsOfNode := range newNodeShardsCache {
		c.nodeShardsCache[nodeName] = shardsOfNode
	}

	for shardID, shard := range newShardsCache {
		c.shardsCache[shardID] = shard
	}

	return nil
}
