# 一个基于LLM的K8S Controller for Redis方案

## 动机

Redis Cluster 的扩缩容、升级和故障恢复不是单步资源收敛，而是带有强顺序约束和安全不变量的长事务。传统 K8S Controller 需要将所有中间状态和失败补偿路径硬编码为复杂状态机，维护成本高且容易遗漏边界场景。本方案引入 LLM 作为受约束的规划器，根据当前集群快照和目标规格生成操作计划；Controller 负责对计划进行确定性校验，并以幂等的小步骤执行，从而在保持安全性的前提下降低多步骤 reconciliation 的实现复杂度。

## 目标
实现一个K8S Controller，通过它可以在集群中对Redis Cluster做生命周期管理：
- 创建
- 删除
- 升降配置

## 模型
Redis Cluster有两个基本概念：
1. Shard: 数据在分片间分布，增加Shard能横向扩容Redis集群的容量
2. Replica: 一个Shard里有多个Replica，复制Shard主节点的数据，Master DOWN时可以被提升为新主

一个Redis Cluster最少有1Shard+1Replica，共两个节点

Redis Cluster在变配时**不允许**存在单Master的情况，即使只是中间状态

每个 Redis Cluster 使用一个独立的 K8S namespace。namespace 的名字就是 Redis Cluster 集群名，同一个 Redis Cluster 的所有 Pod 都必须位于该 namespace 中。

## CRD设计

RedisCluster CRD 定义 RedisCluster 这种资源类型的 schema。用户创建的 RedisCluster CR 承载具体的 `spec` 和 `status`：`spec` 表达用户期望的 Redis Cluster 拓扑和运行配置；`status` 表达 Controller 观察到的实际拓扑、健康状态和当前 reconciliation 计划。

### Spec

- `shards`: Redis Cluster 的分片数量，对应 Master 数量，最小值为 1
- `replicasPerShard`: 每个 Shard 的 Replica 数量，最小值为 1
- `image`: Redis 镜像版本
- `memorySize`: Redis 节点内存大小，用于配置每个 Redis 实例的内存资源

RedisCluster CR 示例：

```yaml
apiVersion: redis.example.com/v1alpha1
kind: RedisCluster
metadata:
  name: example
spec:
  shards: 3
  replicasPerShard: 1
  image: redis:7.2
  memorySize: 2Gi
```

其中 `shards`、`replicasPerShard`、`image` 和 `memorySize` 是当前版本创建 Redis Cluster 的核心字段。`memorySize` 用于支持 Redis 节点内存规格的升降配置。

### Status

- `observedGeneration`: Controller 已处理的 RedisCluster CR `metadata.generation`
- `phase`: 当前生命周期阶段，例如 `Creating`、`Ready`、`Scaling`、`Upgrading`、`Deleting`、`Failed`
- `conditions`: 标准 K8S condition，用于表达 `Ready`、`Healthy`、`PlanAccepted` 等状态
- `topology`: 当前 Redis Cluster 实际拓扑，包括 Shard、Master、Replica、Redis nodeId 和 slot 分布
- `activePlan`: 当前正在执行的 reconciliation plan，包括操作类型、当前步骤和每个步骤状态

RedisCluster CR status 示例：

```yaml
status:
  phase: Ready
  observedGeneration: 3
  conditions:
    - type: Ready
      status: "True"
      reason: ClusterHealthy
      message: Redis cluster is healthy
    - type: Planned
      status: "True"
      reason: PlanAccepted
      message: Current plan passed validation
  topology:
    shards:
      - id: shard-0
        master:
          pod: example-redis-0
          nodeId: abc
          slots: "0-5460"
          ready: true
        replicas:
            pod: example-redis-1
            nodeId: def
            ready: true
      - id: shard-1
        master:
          pod: example-redis-2
          nodeId: ghi
          slots: "5461-10922"
          ready: true
        replicas:
            pod: example-redis-3
            nodeId: jkl
            ready: true
  activePlan:
    id: create-001
    operation: Create
    status: Completed
    currentStep: verify-cluster
    steps:
      - id: ensure-redis-0
        action: EnsureNode
        status: Completed
      - id: wait-redis-0
        action: WaitNodeReady
        status: Completed
      - id: replicate-redis-1
        action: ReplicateNode
        status: Completed
      - id: verify-cluster
        action: VerifyCluster
        status: Completed
```

### 设计原则

- `spec` 只描述用户意图，不直接暴露每个 Shard 的 Master/Replica Pod 名称等内部编排细节；`status.topology` 描述 Controller 观察到并维护的实际拓扑。

- LLM 生成的计划不应直接执行，而应固化为受限的 `activePlan`，由 Controller 进行确定性校验后，以幂等的小步骤逐步执行。这样可以避免每次 reconcile 都重新生成计划导致计划漂移，也方便在故障恢复时继续执行或重新规划。

## DSL设计

LLM 不直接控制底层的各个对象，而是以 DSL 的方式生成变更计划，随后 Controller 按照计划对 Redis Cluster 执行动作，最终达到目标状态。

Plan 的格式以 JSON 为基础。每个 Plan 由若干个 Action 组成。Action 是预先定义的动作，带有可配置的参数，由预编写的代码实现。

DSL 不暴露任意 Shell 命令、Redis 命令或 K8S patch，只允许使用 Controller 支持的白名单 Action。

Action 的设计原则是尽量简单、稳定、可复用。复杂的编排逻辑由 LLM 生成 plan 表达；Controller 不为每个 Operation 手写复杂状态机，只负责执行单个 Action、更新执行状态，并在执行前后做确定性校验。

同时，复杂性可以交给 LLM，安全性不能交给 LLM。Controller 必须拒绝任何违反安全不变量的 plan。

Controller 每次 reconcile 只推进一个 Pending step。每个 step 执行完成后，Controller 重新观察 K8S 与 Redis Cluster 实际状态，更新 `status.activePlan`，再进入下一轮 reconcile。

详细的 Operation 设计见 `docs/OPERATIONS.md`，Action 设计见 `docs/ACTIONS.md`。
