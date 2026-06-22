# Operations

## 概述

Operation 是文档中用于描述 RedisCluster CR spec 变化类型的分类，例如 `Create`、`Delete`、`ScaleOut`、`ScaleIn`、`UpdateMemorySize`。

Operation 不再是 Plan DSL 或 status 字段。LLM 只根据当前状态和目标状态生成有序 Action Plan；Controller 根据 action shape 和当前/目标状态确定性校验 Plan，并按照 Plan 中的 Action 顺序逐步执行。

Controller 同样用这套机制处理实际集群偏离健康态的 Repair 场景。Repair 不修改用户 `spec`；LLM 仍只根据当前状态和目标 `spec` 生成一个 Action Plan，Controller 做确定性校验并逐步执行。

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

所有由 Controller 管理的 Redis Pod 必须命名为 `redis-<N>`，其中 `<N>` 为非负整数。Pod 名全局只能使用一次：Create 时 Pod 必须从 `redis-0` 开始连续创建初始节点；Create 后的任何新增 Pod，包括 ScaleOut 和 Repair，都必须使用历史最大 ordinal + 1 开始的连续新序列。不允许 LLM 自行发明其他名称、复用已删除 Pod 名，或填补历史 ordinal 空洞。

例如 Create 2 个 shard、每个 shard 1 个 replica，需要 4 个节点，必须命名为 `redis-0`、`redis-1`、`redis-2`、`redis-3`。

例如当前 topology 使用 `redis-0` 到 `redis-3`，扩容需要 3 个新节点时，新节点必须是 `redis-4`、`redis-5`、`redis-6`。

例如 `redis-1` 曾被删除并通过 Repair 替换为 `redis-4`，后续再新增 2 个节点时，新节点必须是 `redis-5`、`redis-6`，不能重新使用 `redis-1`。

如果现有 topology 中出现非 `redis-<N>` 命名的 managed Pod，或发现 managed Pod 名被复用，第一版必须拒绝执行，等待人工修复。历史删除造成的 ordinal 空洞不视为非法 topology。

### Slot均衡规则

ScaleOut 后的 slot 目标分布由 Controller 规则确定，不由 LLM 自由决定。

Controller 按如下顺序排列最终 Master：

1. 当前 `status.topology.shards` 中已有 Master 的顺序
2. ScaleOut Plan 中新增 Master 的 `EnsureNode` 顺序

然后将 `0-16383` 按 Master 数量尽量均分，前面的 Master 在不能整除时多持有 1 个 slot。LLM 生成的 `MigrateSlots` 必须把当前 slot owner 迁移到这个目标分布；Validator 必须能从当前 topology 和目标 spec 复现相同的迁移矩阵。迁移矩阵可能包含 existing Master 到 existing Master 的迁移，不只包含迁入新增 Master 的迁移。

### ScaleOut计划结构

当增加 Shard 时，Plan 必须先保证所有新 Master 都至少拥有一个健康 Replica，再开始迁移 slots。一个典型顺序是：

1. 对所有新增 Redis 节点执行 `EnsureNode`
2. 对所有新增 Redis 节点执行 `WaitNodeReady`
3. 用已有健康节点作为 seed，对所有新增节点执行 `MeetNode`
4. 对新增 Master 的 Replica 执行 `ReplicateNode`
5. 执行 `MigrateSlots`，覆盖按均衡规则计算出的所有 source/target slot 迁移
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
- 当前 topology 中所有 managed Pod 名称必须符合 `redis-N`，且不能复用历史上已经使用过的 Pod 名
- `spec.shards` 或 `spec.replicasPerShard` 必须且只能有一个增加，另一个必须保持不变，二者都不能减少
- `image` 和 `memorySize` 不能在 ScaleOut 中变化
- 所有新增 Pod 必须从历史最大 `redis-N + 1` 开始连续命名，不能填补历史 ordinal 空洞
- 所有新增 Pod 必须先 `EnsureNode`、再 `WaitNodeReady`、再 `MeetNode`
- 每个最终 Master 必须在迁移 slots 前拥有至少一个 Replica
- 每个新增或补齐的 Replica 必须通过 `ReplicateNode` 指向合法 Master
- 所有 `MigrateSlots` 的 source/target 必须是 Master，且 target Master 必须已有健康 Replica
- `MigrateSlots` 的 slots 必须精确匹配 Controller 按均衡规则计算出的迁移矩阵
- 如果只增加 `replicasPerShard`，Plan 不允许包含 `MigrateSlots` 或 `AddSlots`
- Plan 最后必须包含 `VerifyCluster`

## ScaleIn

ScaleIn Operation 表示在一个已经 Ready、健康、slot 完整覆盖的 Redis Cluster 上减少节点数量。第一版只支持减少每个 Shard 的 Replica 数量，不支持减少 Shard / Master 数量。

Replica ScaleIn 的目标状态：

- Master 数量保持等于当前 `spec.shards`
- 每个 Master 的健康 Replica 数量等于新的 `spec.replicasPerShard`
- `spec.replicasPerShard` 最小值为 1
- slots 完整覆盖 `0-16383`，且 slot owner 不变
- Redis Cluster 状态为 `ok`

当只减少 `replicasPerShard` 时，Plan 不应创建节点，也不应迁移 slots。一个典型顺序是：

1. 对每个多余 Replica 执行 `ForgetNode`
2. 对同一个 Replica 执行 `DeleteNode`
3. 最后执行 `VerifyCluster`

Replica ScaleIn 安全校验：

- `targetGeneration` 必须等于当前 RedisCluster CR 的 `metadata.generation`
- `spec.shards` 必须保持不变
- `spec.replicasPerShard` 必须小于当前 Replica 数量，且不能小于 1
- Plan 只能包含 `ForgetNode`、`DeleteNode`、`VerifyCluster`
- 被删除节点必须是当前 topology 中的 Replica，不能是 Master
- 每个被删除 Replica 必须先 `ForgetNode`，再 `DeleteNode`
- 每个 Shard 删除的 Replica 数必须精确收敛到新的 `spec.replicasPerShard`
- Plan 最后必须包含 `VerifyCluster`

## Repair

Repair Operation 表示实际 K8S/Redis 状态偏离了安全不变量。它只是文档中的状态差异分类，不是 Plan DSL 或 Controller 分支。Controller 不先恢复到单独的中间 baseline；它会废弃与当前实际状态不再匹配的 activePlan，并重新规划一个从当前状态直接收敛到当前 `spec` 的 Action Plan。

第一版 Repair 优先覆盖单个 managed Pod 被删除的场景：

- Replica Pod 被删除
- Master Pod 被删除，且 Redis Cluster 存在可提升或已提升的健康 Replica
- activePlan 执行过程中出现上述 Pod 删除

Repair 不处理 Master 被删除且没有健康 Replica 的自动数据恢复。该场景可能涉及数据丢失，Controller 必须将集群标记为 degraded，并阻止 LLM 生成会重新分配 slots、伪造 slot owner 或创建空 Master 接管数据的计划。

Repair 场景的目标状态：

- 最终状态必须匹配当前 RedisCluster `spec`
- Redis Cluster 状态为 `ok`
- slots 完整覆盖 `0-16383`
- 每个持有 slots 的 Master 至少有一个健康 Replica
- 所有新建 Redis 节点使用当前 `spec.image` 和 `spec.memorySize`
- replacement Pod 使用历史最大 ordinal + 1 的新名称，不复用被删除 Pod 名
- 已被删除的旧 Redis nodeId 不再残留在保留节点的 cluster view 中

### Replica Pod删除

当被删除的 Pod 是 Replica 时，Plan 应补齐该 Shard 的副本数。新 Pod 必须使用新的单调递增名称，不能复用被删除 Pod 名，并重新执行 `EnsureNode`、`WaitNodeReady`、`MeetNode` 和 `ReplicateNode`。旧 nodeId 应通过带 `lastKnownNodeId` 的 `ForgetNode` 从 Redis Cluster 中清理，最后执行 `VerifyCluster`。

### Master Pod删除

当被删除的 Pod 是 Master 且存在健康 Replica 时，Plan 应接受 Redis Cluster 的当前事实。如果 Redis 已完成 failover，被提升的 Replica 就是该 Shard 的当前 Master；Controller 不要求恢复旧 Pod 的 Master 身份。Plan 应围绕当前 slot owner 使用新的单调递增 Pod 名补齐 Replica，并通过带 `lastKnownNodeId` 的 `ForgetNode` 清理旧 Master nodeId，最后执行 `VerifyCluster`。

如果 Redis 尚未完成 failover，Plan 可以先通过不改变拓扑的动作等待或验证实际状态，但不能把 slots 主动分配给一个没有数据来源的新节点。只有在 live Redis 状态显示 slots 已由健康 Master 持有后，才允许继续补副本和清理旧节点。

### activePlan中的异常

当 Create、ScaleOut 或其他 activePlan 正在执行时发现单 Pod 删除，当前 activePlan 已不再代表实际状态。Controller 应替换或失败当前 activePlan，并基于新的 observed state 和当前 `spec` 重新向 LLM 请求一个收敛 Plan。

### Repair安全校验

Controller 在执行这类 Plan 前必须做 schema 校验和安全校验：

- `targetGeneration` 必须等于当前 RedisCluster CR 的 `metadata.generation`
- Plan 的最终 `VerifyCluster` 必须匹配当前 `spec.shards`、`spec.replicasPerShard`、`image` 和 `memorySize`
- Plan 不允许包含 `AddSlots`，除非 Validator 能证明目标 slots 当前未被任何 Redis Master 持有。该例外不适用于 Master 删除且没有健康 Replica 的单 Pod drift，只能用于被证明无数据风险的未分配 slots 场景
- Plan 不允许包含 `MigrateSlots`，除非 Validator 能证明这是从健康 Master 到有健康 Replica 的 Master 的安全迁移；单 Pod 删除修复默认不应迁移 slots
- Master 删除且没有健康 Replica 时，Plan 必须被拒绝
- replacement Pod 必须使用历史最大 ordinal + 1 的新名称，不能复用被删除 Pod 名或填补历史 ordinal 空洞
- `ForgetNode` 只能清理已不存在、已失去角色、或不再持有 slots 的旧节点
- 用于清理已删除 Pod 的 `ForgetNode` 必须携带 `lastKnownNodeId`
- Plan 最后必须包含 `VerifyCluster`，并按当前 `spec` 校验
