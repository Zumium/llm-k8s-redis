# Operations

## 概述

Operation 是文档中用于描述 RedisCluster CR spec 变化类型的分类，例如 `Create`、`Delete`、`ScaleOut`、`ScaleIn`、`UpdateMemorySize`。

Operation 不再是 Plan DSL 或 status 字段。LLM 只根据当前状态和目标状态生成有序 Action Plan；Controller 根据 action shape 和当前/目标状态确定性校验 Plan，并按照 Plan 中的 Action 顺序逐步执行。

## Plan结构

Plan 使用 JSON 表达：

```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "create-001",
  "targetGeneration": 1,
  "summary": "Create Redis Cluster with 2 shards and 1 replica per shard",
  "steps": []
}
```

- `dslVersion`: DSL 版本
- `planId`: 当前 Plan 的唯一标识
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

- `targetGeneration` 必须等于当前 RedisCluster CR 的 `metadata.generation`
- 所有 Action 必须属于白名单
- 所有 step 的 `id` 不能重复
- `EnsureNode` 的节点数量必须等于 `spec.shards * (1 + spec.replicasPerShard)`
- 所有 `AddSlots` 的 slots 必须完整覆盖 `0-16383`
- Plan 最后必须包含 `VerifyCluster`

## ScaleOut

ScaleOut Operation 表示在一个已经 Ready、健康、slot 完整覆盖的 Redis Cluster 上增加 Shard 数量，或增加每个 Shard 的 Replica 数量。一次 ScaleOut 只能调整 `spec.shards` 或 `spec.replicasPerShard` 其中一个参数，不能同时调整两个参数。

第一版 ScaleOut 只支持如下 spec 变化：

- `spec.shards` 大于当前 observed topology 中的 Master 数量，且 `spec.replicasPerShard` 保持不变
- 或 `spec.replicasPerShard` 大于当前 observed topology 中每个 Master 的健康 Replica 数量，且 `spec.shards` 保持不变
- `spec.image` 与现有节点镜像保持不变
- `spec.memorySize` 与现有节点内存规格保持不变

如果用户同时修改 `shards` 和 `replicasPerShard`，降低 `shards`、降低 `replicasPerShard`，或同时修改 `image` / `memorySize`，Controller 必须拒绝当前变更并将原因写入 condition。缩容由 `ScaleIn` 处理，镜像或内存变更由 `UpdateMemorySize` / 后续升级类 Operation 处理。

ScaleOut 的目标状态：

- Master 数量等于新的 `spec.shards`
- 每个 Master 的健康 Replica 数量等于新的 `spec.replicasPerShard`
- 所有 Redis 节点使用原有 `spec.image` 和 `spec.memorySize`
- slots 完整覆盖 `0-16383`
- slots 在所有 Master 间按确定性策略尽量均衡分布
- Redis Cluster 状态为 `ok`
- 每个持有 slots 的 Master 至少有一个健康 Replica

### Pod命名规则

所有由 Controller 管理的 Redis Pod 必须命名为 `redis-<N>`，其中 `<N>` 为从 0 开始连续递增的非负整数。Create 时 Pod 必须从 `redis-0` 开始连续命名；ScaleOut 时新 Pod 必须从当前最大 ordinal + 1 开始连续命名。不允许 LLM 自行发明其他名称或跳过 ordinal。

例如 Create 2 个 shard、每个 shard 1 个 replica，需要 4 个节点，必须命名为 `redis-0`、`redis-1`、`redis-2`、`redis-3`。

例如当前 topology 使用 `redis-0` 到 `redis-3`，扩容需要 3 个新节点时，新节点必须是 `redis-4`、`redis-5`、`redis-6`。

如果现有 topology 中出现非 `redis-<N>` 命名的 managed Pod，或 Pod 命名不连续，第一版必须拒绝执行，等待人工修复。

### Slot均衡规则

ScaleOut 后的 slot 目标分布由 Controller 规则确定，不由 LLM 自由决定。

Controller 按如下顺序排列最终 Master：

1. 当前 `status.topology.shards` 中已有 Master 的顺序
2. ScaleOut Plan 中新增 Master 的 `EnsureNode` 顺序

然后将 `0-16383` 按 Master 数量尽量均分，前面的 Master 在不能整除时多持有 1 个 slot。LLM 生成的 `MigrateSlots` 必须把当前 slot owner 迁移到这个目标分布；Validator 必须能从当前 topology 和目标 spec 复现相同的迁移矩阵。

### ScaleOut计划结构

当增加 Shard 时，Plan 必须先保证所有新 Master 都至少拥有一个健康 Replica，再开始迁移 slots。一个典型顺序是：

1. 对所有新增 Redis 节点执行 `EnsureNode`
2. 对所有新增 Redis 节点执行 `WaitNodeReady`
3. 用已有健康节点作为 seed，对所有新增节点执行 `MeetNode`
4. 对新增 Master 的 Replica 执行 `ReplicateNode`
5. 对新增 Master 执行 `MigrateSlots`，从已有 Master 迁入按均衡规则计算出的 slots
6. 最后执行 `VerifyCluster`

当只增加 `replicasPerShard` 时，Plan 不应迁移 slots，也不应创建新的 Master。一个典型顺序是：

1. 对所有新增 Replica 节点执行 `EnsureNode`
2. 对所有新增 Replica 节点执行 `WaitNodeReady`
3. 用已有健康节点作为 seed，对所有新增 Replica 节点执行 `MeetNode`
4. 对新增 Replica 执行 `ReplicateNode`，为每个已有 Master 补齐目标 Replica 数量
5. 最后执行 `VerifyCluster`

ScaleOut 不使用 `AddSlots` 为新 Master 抢占已有 slots。`AddSlots` 只用于 Create 或未分配 slots 的场景；已有 slot 的重新分布必须通过 `MigrateSlots` 完成。

### ScaleOut安全校验

Controller 在执行 ScaleOut Plan 前必须做 schema 校验和安全校验：

- `targetGeneration` 必须等于当前 RedisCluster CR 的 `metadata.generation`
- 当前 topology 必须存在、健康、完整覆盖 slots，且没有 migrating/importing slot
- 当前 topology 中所有 managed Pod 名称必须符合 `redis-N`
- `spec.shards` 或 `spec.replicasPerShard` 必须且只能有一个增加，另一个必须保持不变，二者都不能减少
- `image` 和 `memorySize` 不能在 ScaleOut 中变化
- 所有新增 Pod 必须从当前最大 `redis-N + 1` 开始连续命名
- 所有新增 Pod 必须先 `EnsureNode`、再 `WaitNodeReady`、再 `MeetNode`
- 每个最终 Master 必须在迁移 slots 前拥有至少一个 Replica
- 每个新增或补齐的 Replica 必须通过 `ReplicateNode` 指向合法 Master
- 所有 `MigrateSlots` 的 source/target 必须是 Master，且 target Master 必须已有健康 Replica
- `MigrateSlots` 的 slots 必须精确匹配 Controller 按均衡规则计算出的迁移矩阵
- 如果只增加 `replicasPerShard`，Plan 不允许包含 `MigrateSlots` 或 `AddSlots`
- Plan 最后必须包含 `VerifyCluster`
