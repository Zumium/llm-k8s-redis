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
- `conditions`: 标准 K8S condition，用于表达 `Ready`、`Healthy`、`PlanAccepted` 等状态
- `topology`: 当前 Redis Cluster 实际拓扑，包括 Shard、Master、Replica、Redis nodeId 和 slot 分布
- `activePlan`: 当前正在执行的 reconciliation plan，包括当前步骤和每个步骤状态

RedisCluster CR status 示例：

```yaml
status:
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
          pod: redis-0
          nodeId: abc
          slots: "0-5460"
          ready: true
        replicas:
            pod: redis-1
            nodeId: def
            ready: true
      - id: shard-1
        master:
          pod: redis-2
          nodeId: ghi
          slots: "5461-10922"
          ready: true
        replicas:
            pod: redis-3
            nodeId: jkl
            ready: true
  activePlan:
    id: create-001
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

### Topology的刷新

因为Redis Cluster的Topo结构可能会自己发生变化（例如因为发生了failover），Topology需要刷新，尽量及时反馈底层的状态。

Controller 采用**低负载的 lazy refresh**策略：

- **复用现有 reconcile 触发**：K8S Pod/Namespace/RedisCluster 事件已经会触发 reconcile，因此 K8S 侧变化（Pod Ready、IP 变更等）能免费触发一次 topology 观察。
- **Pod 集合 drift 绕过 stale gate**：每次 reconcile 入口先 list 当前 managed Pods（名字 + Ready 状态），与 `status.topology` 记录的 Pod 集合比对；不一致则**立即**触发 `ObserveTopology`，不等 stale threshold。这样 Pod 删除/Ready 翻转能在事件触发的下一次 reconcile 就反映到 `status.topology`，而不是等 60s 慢 requeue。
- **慢 requeue 兜底 Redis 侧静默漂移**：当 `activePlan` 处于 `Completed` 或 `Failed`（即没有正在执行的 plan step）时，reconcile 结束后会按 `--topology-refresh-interval`（默认 60s）重新入队，确保手动 failover、slot 迁移、节点掉线等 Redis 内部变化最多在 60s 内被反映到 `status.topology`。
- **stale gate 防止 Redis 被打爆**：当 Pod 集合未 drift 时，检查距上次观察是否超过 `--topology-stale-threshold`（默认 10s），未超过则跳过 Redis 调用；Pod 风暴时不会被重复 observe。stale threshold 退化为 idle 集群的周期性兜底，不再是 Pod 删除的延迟来源。
- **不覆盖历史状态**：观察失败（无 ready seed、PING 失败、`cluster_state` 非 ok 等）时，保留旧的 `status.topology` 不动，仅将 `Healthy` condition 置为 `False`，避免擦掉最后已知拓扑。
- **与 plan 执行互不干扰**：Pod drift precheck 在每次 reconcile 入口运行，即使已有 activePlan 也不跳过；只有周期性 observe-only lazy refresh 限制在没有 plan 正在运行时执行。plan 执行期间仍由 plan 自己的 `VerifyCluster` 等步骤负责维护 topology。
- **K8s 视角优先**：`rebuildTopology` 以 Redis `CLUSTER NODES` 为准挑选 master/replica，但 `Pod` 名和 `Ready` 字段来自 `podsByIP` 映射。当某个 Redis 节点对应的 Pod 已从 K8s 删除（但 Redis 集群超时未到、仍认为节点 healthy）时，该节点在 topology 中 `Pod=""` `Ready=false`，`NodeID`/`Slots` 保留——topology 立刻反映 K8s 视角的 Pod 消失，不等 Redis 侧 fail。

#### 状态陈旧 SLO

`status.topology` 是 Controller 对 Redis Cluster 的最后一次观察结果，不是强实时状态。Controller 对状态陈旧度给出以下 SLO：

- **K8S 侧变化**：由 Controller 管理的 Pod 创建、删除、Ready 状态变化、Pod IP 变化、Namespace 删除等，会通过 K8S watch 触发 reconcile。若这些变化导致 managed Pod 集合或 Ready 状态与 `status.topology` 不一致，下一次 reconcile 必须绕过 stale gate 并立即执行 `ObserveTopology`。目标是在 apiserver watch 正常投递后的一次 reconcile 内反映到 `status.topology` / `Healthy` condition。
- **Redis 侧静默漂移**：Redis Cluster 内部发生但不会产生 K8S 事件的变化，例如自动 failover、手工 slot 迁移、手工 `CLUSTER MEET` / `CLUSTER REPLICATE`、Redis 节点 link 状态变化等，由周期性 lazy refresh 发现。目标是在 `--topology-refresh-interval` 内反映，默认 60s。
- **Redis 观察防抖**：当没有 Pod drift 时，Controller 不应高频查询 Redis。两次 observe-only Redis 查询之间至少间隔 `--topology-stale-threshold`，默认 10s。Pod drift 可以绕过该阈值，因为它是 K8S watch 已确认的外部变化。
- **执行安全检查**：任何会改变 Redis 或 K8S 状态的 plan step 不依赖 `status.topology` 的新鲜度做安全判断。Executor 必须在每次执行 step 时重新读取 live K8S / Redis 状态，并以实时读取结果校验幂等性和安全不变量。
- **观察失败语义**：如果 `ObserveTopology` 因无 ready seed、Redis 不可达、`CLUSTER INFO` / `CLUSTER NODES` 失败、`cluster_state` 非 `ok` 等原因失败，旧的 `status.topology` 可以继续保留，但 `Healthy` condition 必须更新为 `False`，并通过 `topologyObservedAt` 表达最近一次观察尝试时间。

因此，本系统不承诺毫秒级实时感知；承诺的是 K8S 事件驱动的快速刷新、Redis 内部漂移的有界陈旧，以及执行路径上的 live-state 安全校验。

实现上新增 `Observer` interface（由 `ActionExecutor` 实现），把 `CLUSTER INFO` / `CLUSTER NODES` / Pod 映射的观察逻辑抽取为 `observeTopology`，供 `VerifyCluster` 和 lazy refresh 共用。`status.topologyObservedAt` 记录最后一次成功发起观察的时间戳，用于 stale gate。

### 异常恢复原则

当 K8S Pod 或 Redis Cluster 实际拓扑偏离目标安全态时，Controller 将其视为 observed drift。Drift 不修改用户 `spec`；Controller 会基于当前状态和目标 `spec` 重新规划，不引入单独的 Repair 目标。

Drift 收敛仍由 LLM 生成受限 Action Plan，Controller 和 Validator 负责确定性安全校验。LLM 可以决定步骤顺序，但不能绕过 Action 白名单、不能直接执行 Redis/K8S 任意命令，也不能把不安全拓扑强行声明为健康。

单个 Pod 删除场景遵循以下高层语义：

- **Replica Pod 被删除**：集群 slot 不应丢失，但对应 Shard 的副本数不足。Controller 应生成收敛 Plan，用新的单调递增 Pod 名补回一个 Replica，使其加入 Redis Cluster 并复制当前 Master，随后按旧 nodeId 清理 Redis node 记录并验证集群健康。
- **Master Pod 被删除且存在健康 Replica**：接受 Redis Cluster 的自动 failover 结果。被提升的 Replica 成为当前事实上的 Master，Controller 不强行恢复旧 Master 角色，而是用新的单调递增 Pod 名为新的 Master 补齐 Replica，并验证 slot 覆盖和副本安全不变量。
- **Master Pod 被删除且没有健康 Replica**：默认不自动接管 slots 或创建空 Master。该场景可能涉及数据丢失，Controller 应标记集群为不可自动修复的 degraded 状态，等待人工恢复或后续显式的数据恢复策略。
- **删除发生在 activePlan 执行中**：当前 activePlan 已不再代表实际状态。Controller 应替换或失败当前 Plan，并基于新的 topology 和当前 `spec` 重新规划一个收敛 Plan。

Drift 收敛路径必须以 live K8S / Redis 读取作为执行安全判断依据。`status.topology` 只表达最近观察结果，不能作为唯一事实来源。Redis Pod 名全局不复用；新 Pod 必须按新的 Redis 节点重新加入 cluster，旧节点通过最后已知 nodeId 清理。

## DSL设计

LLM 不直接控制底层的各个对象，而是以 DSL 的方式生成变更计划，随后 Controller 按照计划对 Redis Cluster 执行动作，最终达到目标状态。

Plan 的格式以 JSON 为基础。每个 Plan 由若干个 Action 组成。Action 是预先定义的动作，带有可配置的参数，由预编写的代码实现。

DSL 不暴露任意 Shell 命令、Redis 命令或 K8S patch，只允许使用 Controller 支持的白名单 Action。

Action 的设计原则是尽量简单、稳定、可复用。复杂的编排逻辑由 LLM 生成 plan 表达；Controller 不为每个 Operation 手写复杂状态机，只负责执行单个 Action、更新执行状态，并在执行前后做确定性校验。

同时，复杂性可以交给 LLM，安全性不能交给 LLM。Controller 必须拒绝任何违反安全不变量的 plan。

Controller 每次 reconcile 只推进一个 Pending step。每个 step 执行完成后，Controller 重新观察 K8S 与 Redis Cluster 实际状态，更新 `status.activePlan`，再进入下一轮 reconcile。

第一版增量扩缩容支持 `ScaleOut` 和 `ScaleIn`：一次只能调整 `shards` 或 `replicasPerShard` 其中一个参数，不在同一个 Operation 中处理二者同时变更、镜像变更或内存规格变更。所有 Redis Pod 必须命名为 `redis-<N>`，其中 `<N>` 为非负整数；Pod 名全局只能使用一次，Create 从 `redis-0` 开始创建初始连续序列，后续新增 Pod 必须使用历史最大 ordinal 加 1，不填补删除留下的空洞。Shard 扩容和缩容后的 slot 分布采用确定性的均衡策略，LLM 只能生成符合该策略的 `MigrateSlots` plan，Controller 必须在执行前复现并校验迁移矩阵。

详细的 Operation 设计见 `docs/OPERATIONS.md`，Action 设计见 `docs/ACTIONS.md`。
