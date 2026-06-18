# Operations

## 概述

Operation 表示一次 RedisCluster CR spec 变化对应的高层生命周期操作，例如 `Create`、`Delete`、`ScaleOut`、`ScaleIn`、`UpdateMemorySize`。

Operation 本身不对应 Controller 中的硬编码状态机，而是由 LLM 生成的有序 Action Plan 表达。Controller 负责校验 Plan，并按照 Plan 中的 Action 顺序逐步执行。

## Plan结构

Plan 使用 JSON 表达：

```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "create-001",
  "operation": "Create",
  "targetGeneration": 1,
  "summary": "Create Redis Cluster with 2 shards and 1 replica per shard",
  "steps": []
}
```

- `dslVersion`: DSL 版本
- `planId`: 当前 Plan 的唯一标识
- `operation`: 当前执行的操作类型，例如 `Create`、`Delete`、`ScaleOut`、`ScaleIn`、`UpdateMemorySize`
- `targetGeneration`: 当前 Plan 基于哪一版 RedisCluster CR spec 生成，对应 RedisCluster CR 的 `metadata.generation`
- `summary`: LLM 生成的计划摘要，用于解释 Plan 目的
- `steps`: 有序 Action 列表

## Create

Create Operation 表示针对一个已创建的 RedisCluster CR，在 K8S 集群中创建对应的完整 Redis Cluster。

Create 的目标状态：

- Master 数量等于 `spec.shards`
- 每个 Master 的 Replica 数量等于 `spec.replicasPerShard`
- 所有 Redis 节点使用 `spec.image` 和 `spec.memorySize`
- slots 完整覆盖 `0-16383`
- Redis Cluster 状态为 `ok`
- 每个持有 slots 的 Master 至少有一个健康 Replica

对于如下 RedisCluster CR：

```yaml
spec:
  shards: 2
  replicasPerShard: 1
  image: redis:7.2
  memorySize: 2Gi
```

LLM 可以生成如下 Create Plan：

```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "create-001",
  "operation": "Create",
  "targetGeneration": 1,
  "summary": "Create Redis Cluster with 2 shards and 1 replica per shard",
  "steps": [
    {
      "id": "ensure-redis-0",
      "action": "EnsureNode",
      "params": {
        "namespace": "example",
        "pod": "redis-0",
        "image": "redis:7.2",
        "memorySize": "2Gi"
      }
    },
    {
      "id": "ensure-redis-1",
      "action": "EnsureNode",
      "params": {
        "namespace": "example",
        "pod": "redis-1",
        "image": "redis:7.2",
        "memorySize": "2Gi"
      }
    },
    {
      "id": "ensure-redis-2",
      "action": "EnsureNode",
      "params": {
        "namespace": "example",
        "pod": "redis-2",
        "image": "redis:7.2",
        "memorySize": "2Gi"
      }
    },
    {
      "id": "ensure-redis-3",
      "action": "EnsureNode",
      "params": {
        "namespace": "example",
        "pod": "redis-3",
        "image": "redis:7.2",
        "memorySize": "2Gi"
      }
    },
    {
      "id": "wait-redis-0",
      "action": "WaitNodeReady",
      "params": {
        "namespace": "example",
        "pod": "redis-0"
      }
    },
    {
      "id": "wait-redis-1",
      "action": "WaitNodeReady",
      "params": {
        "namespace": "example",
        "pod": "redis-1"
      }
    },
    {
      "id": "wait-redis-2",
      "action": "WaitNodeReady",
      "params": {
        "namespace": "example",
        "pod": "redis-2"
      }
    },
    {
      "id": "wait-redis-3",
      "action": "WaitNodeReady",
      "params": {
        "namespace": "example",
        "pod": "redis-3"
      }
    },
    {
      "id": "meet-redis-1",
      "action": "MeetNode",
      "params": {
        "namespace": "example",
        "sourcePod": "redis-0",
        "targetPod": "redis-1"
      }
    },
    {
      "id": "meet-redis-2",
      "action": "MeetNode",
      "params": {
        "namespace": "example",
        "sourcePod": "redis-0",
        "targetPod": "redis-2"
      }
    },
    {
      "id": "meet-redis-3",
      "action": "MeetNode",
      "params": {
        "namespace": "example",
        "sourcePod": "redis-0",
        "targetPod": "redis-3"
      }
    },
    {
      "id": "replicate-redis-1",
      "action": "ReplicateNode",
      "params": {
        "namespace": "example",
        "masterPod": "redis-0",
        "replicaPod": "redis-1"
      }
    },
    {
      "id": "replicate-redis-3",
      "action": "ReplicateNode",
      "params": {
        "namespace": "example",
        "masterPod": "redis-2",
        "replicaPod": "redis-3"
      }
    },
    {
      "id": "add-slots-redis-0",
      "action": "AddSlots",
      "params": {
        "namespace": "example",
        "pod": "redis-0",
        "slots": "0-8191"
      }
    },
    {
      "id": "add-slots-redis-2",
      "action": "AddSlots",
      "params": {
        "namespace": "example",
        "pod": "redis-2",
        "slots": "8192-16383"
      }
    },
    {
      "id": "verify-cluster",
      "action": "VerifyCluster",
      "params": {
        "expectedShards": 2,
        "expectedReplicasPerShard": 1,
        "requireClusterStateOk": true,
        "requireFullSlotCoverage": true,
        "requireAllSlotOwnersHaveReplicas": true
      }
    }
  ]
}
```

### Create安全校验

Controller 在执行 Create Plan 前必须做 schema 校验和安全校验：

- `operation` 必须为 `Create`
- `targetGeneration` 必须等于当前 RedisCluster CR 的 `metadata.generation`
- 所有 Action 必须属于白名单
- 所有 step 的 `id` 不能重复
- `EnsureNode` 的节点数量必须等于 `spec.shards * (1 + spec.replicasPerShard)`
- 所有 `AddSlots` 的 slots 必须完整覆盖 `0-16383`
- Plan 最后必须包含 `VerifyCluster`
