# Actions

## 概述

Action 是 Controller 支持的最小可执行动作。Action 不按 Operation 命名，而按可复用能力命名。Create、ScaleOut、ScaleIn、UpdateMemorySize、Delete 都应尽量复用同一批 Action。

Controller 不提供 `RunCommand`、`ExecRawCommand`、`PatchPod` 等通用 Action，避免 LLM 绕过 Controller 的安全边界。

## Action白名单

第一版支持以下 Action：

- `EnsureNode`: 确保一个 Redis 节点对应的 K8S 资源存在
- `WaitNodeReady`: 等待一个 Redis 节点 Pod Ready，且 Redis 服务可访问
- `MeetNode`: 确保目标节点加入源节点所在的 Redis Cluster gossip 网络
- `ReplicateNode`: 确保一个 Redis 节点成为指定 Master 的 Replica
- `AddSlots`: 确保一段 slots 归属于指定 Master
- `MigrateSlots`: 将一段 slots 从源 Master 迁移到目标 Master
- `ForgetNode`: 确保一个 Redis 节点从 Redis Cluster 中移除
- `DeleteNode`: 删除一个 Redis 节点对应的 K8S 资源
- `VerifyCluster`: 验证 Redis Cluster 满足目标拓扑和安全约束

## 通用规则

每个 Action 必须是幂等的：

- 如果目标状态已经达成，Action 直接标记为 `Completed`
- 如果目标状态部分达成，Action 继续补齐剩余工作
- 如果实际状态违反安全不变量，Action 不应继续执行，Controller 应将 Plan 标记为 `Failed`

每个 Action 的 `params` 必须符合该 Action 的 JSON 参数结构。Controller 在执行前必须做 schema 校验。

Controller 每次 reconcile 只执行一个 Pending step。每个 step 执行完成后，Controller 重新观察 K8S 与 Redis Cluster 实际状态，更新 `status.activePlan`，再进入下一轮 reconcile。

## 实现原理

每个 Action 在代码中对应一个确定性的 executor。LLM 只负责生成 Action 序列和参数，不能直接执行 Redis 命令、K8S patch 或 Shell 命令。

Action executor 的通用执行流程：

1. 解析 `params`，按 Action schema 做参数校验
2. 根据 RedisCluster CR、K8S 资源和 Redis Cluster 实际状态构建当前状态快照
3. 校验当前 Action 的安全前置条件
4. 判断目标状态是否已经达成，如果已达成则直接返回 `Completed`
5. 执行最小必要变更，例如创建 K8S 资源、执行受控 Redis Cluster 操作
6. 重新观察实际状态，验证 Action 的目标状态是否达成
7. 更新 `status.activePlan.steps[*].status`、`message`、`lastTransitionTime`

Action executor 不应依赖上一次 reconcile 的内存状态。Controller 重启后，应只依赖 RedisCluster CR、K8S 实际资源、Redis Cluster 实际拓扑和 `status.activePlan` 恢复执行。

DSL 中所有需要定位 Pod 的地方必须显式携带 `namespace`。单 Pod Action 使用 `namespace` + `pod`；双 Pod Action 使用一个 `namespace` 加两个 pod 字段。每个 Redis Cluster 使用一个独立的 K8S namespace，namespace 的名字就是 Redis Cluster 集群名，同一个 Redis Cluster 的 Pod 不允许跨 namespace。Controller 负责将 namespace 和 pod 名称映射到具体的 K8S Pod、Pod IP、Redis endpoint 和 Redis nodeId。Redis nodeId 必须通过实际 Redis Cluster 状态发现，不能由 LLM 生成。

Action 的实现应该遵循“观察优先”的原则：执行前先检查实际状态，执行后再重新观察，不假设前一个 Action 一定成功。即使 Plan 中 step 顺序合法，executor 也必须在执行当前 Action 时重新验证安全不变量。

当 Action 发现实际状态已经偏离 Plan，且无法通过当前 Action 幂等修复时，应返回 `Failed`，由 Controller 停止继续执行当前 Plan，等待人工介入或新的 CR generation 将该 Plan 标记为 `Superseded`。

## EnsureNode

`EnsureNode` 确保一个 Redis 节点对应的 K8S 资源存在。

Params：

```json
{
  "namespace": "example",
  "pod": "redis-0",
  "image": "redis:7.2",
  "memorySize": "2Gi"
}
```

实现简述：

- Controller 根据 RedisCluster CR spec 和 `params.namespace`、`params.pod` 生成该 Redis 节点对应的 Pod
- 由 Controller 直接管理 Pod 对象
- 如果 Pod 不存在，则创建对应 Pod，并设置 Redis 镜像、内存规格、启动参数和必要的配置
- Controller 必须在 Pod 上设置 `ownerReference` 指向 RedisCluster CR，建立 K8S 层面的从属关系
- Controller 必须在 Pod 上设置 labels/annotations，标识其所属 RedisCluster、Pod 名称、计划角色等信息，便于后续查询和状态重建
- `memorySize`的值通过Redis命令`CONFIG SET maxmemory xxx`设置到节点中
- 如果 Pod 已存在，则修复 labels、annotations、ownerReference；若 image、command、args、memorySize 等不可变字段漂移，未入群 Pod 会删除后重建，疑似已入群 Pod 失败并交给安全删除/替换流程处理（查询命令`CONFIG GET maxmemory`）
- `EnsureNode` 只负责确保 Redis 进程资源存在，不负责 Redis Cluster meet、replica 关系或 slots 分配

安全校验：

- `namespace` + `pod` 必须是当前 Plan 中唯一的 Pod 标识
- `namespace` 必须等于 Redis Cluster 集群名
- `image` 必须与当前 RedisCluster CR spec 中的 `image` 一致
- `memorySize` 必须与当前 RedisCluster CR spec 中的 `memorySize` 一致

## WaitNodeReady

`WaitNodeReady` 等待一个 Redis 节点 Pod Ready，且 Redis 服务可访问。

Params：

```json
{
  "namespace": "example",
  "pod": "redis-0"
}
```

实现简述：

- Controller 根据 `namespace` + `pod` 找到对应 Pod
- 检查 Pod `Ready` condition 是否为 `True`
- 通过 Redis client 连接该节点 endpoint，执行 `PING`健康检查
- 对启用 cluster mode 的 Redis 节点，读取其 Redis nodeId 并写入或刷新状态快照
- 如果 Pod 未就绪或 Redis 不可访问，该 Action 保持 `Running`，等待下一轮 reconcile 重试

安全校验：

- `namespace` + `pod` 必须已被 `EnsureNode` 声明
- 对应 Pod 必须存在
- `namespace` 必须等于 Redis Cluster 集群名

## MeetNode

`MeetNode` 确保目标节点加入源节点所在的 Redis Cluster gossip 网络。

Params：

```json
{
  "namespace": "example",
  "sourcePod": "redis-0",
  "targetPod": "redis-1"
}
```

实现简述：

- Controller 根据 `namespace` + `sourcePod` 和 `namespace` + `targetPod` 解析 Redis endpoint
- 先从 source Pod 执行 `CLUSTER NODES` 或等价查询，判断 target Pod 是否已经在同一个 Redis Cluster 中
- 如果 target Pod 已存在于 cluster 拓扑中，则直接完成
- 如果尚未加入，则通过受控 Redis client 在 source Pod 上执行 `CLUSTER MEET targetHost targetPort`
- 执行后重新查询 cluster 拓扑，确认 target Pod 已进入 gossip 网络

安全校验：

- source Pod 和 target Pod 必须已被 `EnsureNode` 声明
- source Pod 和 target Pod 必须先经过 `WaitNodeReady`
- source Pod 和 target Pod 不能是同一个 Pod
- `namespace` 必须等于 Redis Cluster 集群名

## ReplicateNode

`ReplicateNode` 确保一个 Redis 节点成为指定 Master 的 Replica。

Params：

```json
{
  "namespace": "example",
  "masterPod": "redis-0",
  "replicaPod": "redis-1"
}
```

实现简述：

- Controller 根据 `namespace` + `masterPod` 和 `namespace` + `replicaPod` 查询 Redis nodeId
- 先检查 replica Pod 当前是否已经复制目标 master Pod
- 如果 replica 关系已经符合期望，则直接完成
- 如果尚未建立，则在 replica Pod 上执行受控的 `CLUSTER REPLICATE masterNodeId`
- 执行后重新查询 Redis Cluster 拓扑，确认 replica Pod 已成为目标 master Pod 的副本

安全校验：

- master Pod 和 replica Pod 必须已被 `EnsureNode` 声明
- master Pod 和 replica Pod 必须先经过 `WaitNodeReady`
- master Pod 和 replica Pod 不能是同一个 Pod
- replica Pod 不能已经持有 slots
- `namespace` 必须等于 Redis Cluster 集群名

## AddSlots

`AddSlots` 确保一段 slots 归属于指定 Master。

Params：

```json
{
  "namespace": "example",
  "pod": "redis-0",
  "slots": "0-8191"
}
```

实现简述：

- Controller 解析 `slots` 范围，展开为 Redis slot 集合或分批区间
- 查询当前 Redis Cluster slot 分布，判断目标 slots 是否已经全部归属于目标 Pod
- 对尚未分配的 slots，在目标 Pod 上执行受控的 `CLUSTER ADDSLOTS`
- 如果发现目标 slots 已被其他 Master 持有，`AddSlots` 不做迁移，应返回 `Failed`，由 `MigrateSlots` 处理迁移场景
- 执行后重新查询 slot 分布，确认目标 slots 已归属于目标 Pod

安全校验：

- `namespace` + `pod` 必须已被 `EnsureNode` 声明
- 目标 Pod 不能是 Replica
- `AddSlots` 之前，对应 Master 必须已经至少有一个 Replica
- `slots` 必须在 `0-16383` 范围内
- 不同 `AddSlots` 的 slots 不能重叠
- `namespace` 必须等于 Redis Cluster 集群名

## MigrateSlots

`MigrateSlots` 将一段 slots 从源 Master 迁移到目标 Master。

Params：

```json
{
  "namespace": "example",
  "sourcePod": "redis-0",
  "targetPod": "redis-2",
  "slots": "0-1023"
}
```

实现简述：

- Controller 解析 `slots` 范围，并查询每个 slot 当前归属；如果 live owner 已不是计划里的 `sourcePod`，但仍是 healthy managed Master，则从 live owner 继续迁移
- 如果某个目标 slot 已经归属于目标 Master，则该 slot 视为已完成
- 如果某个目标 slot 暂无 owner，则在目标 Master 上执行 `CLUSTER ADDSLOTS` 修复缺口
- 对每个需要迁移的 slot，将目标 Master 标记为 `IMPORTING`，源 Master 标记为 `MIGRATING`
- 从源 Master 扫描属于该 slot 的 keys，并使用受控 Redis 迁移流程迁移到目标 Master
- 每次 reconcile 中，单个 `MigrateSlots` 最多并发处理 8 个 slots；每个 slot 每批最多迁移 100 个 keys，未完成时返回 `Running`，下一轮 reconcile 从 live Redis 状态继续
- 迁移前必须等待所有 ready managed peer 都看到 target 是 healthy master，且 target 已有 healthy replica；`SETSLOT` 因 gossip 未收敛返回 unknown node 或 “use SETSLOT only with masters” 时返回 `Running`
- 连续的多个 `MigrateSlots` step 如果 source Pod 和 target Pod 都互不重复，可以在同一次 reconcile 中并发执行
- key 迁移完成后，将该 slot 的 owner 切换为目标 Master
- 每迁移一批 slots 后重新观察 cluster 状态，确保可以从中断处继续执行
- 所有 slots 迁移完成后，从最多 3 个独立 healthy Master 读取 `CLUSTER NODES`，全部确认 slot owner 已变为 `target` 后才完成；不足 3 个 Master 时要求所有可用 Master 全部确认

安全校验：

- source Pod 和 target Pod 必须已被 `EnsureNode` 声明，或存在于当前 Redis Cluster 拓扑中
- source Pod 和 target Pod 必须是 Master
- source Pod 必须是计划生成时的 Master；执行时 slots 可以已被其他 healthy managed Master 持有，Controller 会按 live owner 继续迁移；暂无 owner 的目标 slots 可直接补到 target
- target Pod 必须已经至少有一个 Replica
- source Pod 和 target Pod 不能是同一个 Pod
- 如果目标 slot 归属于无法映射到 managed healthy Master 的第三方节点，`MigrateSlots` 必须失败
- 如果目标 slot 已处于与当前 source/target 不一致的 migrating/importing 状态，`MigrateSlots` 必须失败
- `slots` 必须在 `0-16383` 范围内
- `namespace` 必须等于 Redis Cluster 集群名

## ForgetNode

`ForgetNode` 确保一个 Redis 节点从 Redis Cluster 中移除。

Params：

```json
{
  "namespace": "example",
  "pod": "redis-3",
  "lastKnownNodeId": "abc"
}
```

`lastKnownNodeId` 是可选字段。清理仍有 live Pod 的节点时，Controller 可以通过 `namespace` + `pod` 发现 nodeId；清理已经被删除的 Pod 时，必须显式提供 `lastKnownNodeId`，该字段是旧 Redis 节点身份锚点，`pod` 只表达最后已知 Pod 名。

实现简述：

- Controller 根据 `namespace` + `pod` 查询待移除节点的 Redis nodeId；如果 Pod 已删除，则使用 `lastKnownNodeId` 定位旧 Redis 节点
- 从其他仍保留在 Redis Cluster 中的节点上执行受控的 `CLUSTER FORGET nodeId`
- 如果目标节点已经不在某个节点的 `CLUSTER NODES` 结果中，则对该节点视为已完成
- 对整个保留节点集合重复确认，直到所有保留节点都不再认识待移除节点
- `ForgetNode` 只移除 Redis Cluster 拓扑关系，不删除 K8S 资源

安全校验：

- `namespace` + `pod` 必须已被 `EnsureNode` 声明、存在于当前 Redis Cluster 拓扑中，或配合 `lastKnownNodeId` 表示已删除节点的最后已知身份
- 如果目标 Pod 已被删除，`lastKnownNodeId` 必须存在且必须能在保留节点的 `CLUSTER NODES` 结果中匹配到旧节点
- 如果目标 Pod 是 Master，则必须不再持有 slots
- 如果目标 Pod 是 Master，则其 Replica 必须已被迁移或删除计划覆盖
- 如果目标 Pod 已被删除，只能清理 Redis Cluster 中与 `lastKnownNodeId` 匹配的旧 nodeId，不能清理任何 live Pod 当前提供服务的 nodeId
- `namespace` 必须等于 Redis Cluster 集群名

## DeleteNode

`DeleteNode` 删除一个 Redis 节点对应的 K8S 资源。

Params：

```json
{
  "namespace": "example",
  "pod": "redis-3"
}
```

实现简述：

- Controller 根据 `namespace` + `pod` 找到对应的 Pod
- 删除该节点对应的 Pod 和相关配置
- 等待 Pod 被删除，确认不再有同名 Redis endpoint 对外服务
- 删除前不直接操作 Redis Cluster 拓扑，拓扑移除必须由 `ForgetNode` 完成
- 如果 Pod 已经不存在，则直接完成

安全校验：

- `namespace` + `pod` 必须已被 `ForgetNode` 从 Redis Cluster 中移除，或从未加入 Redis Cluster
- 删除后不能导致任何持有 slots 的 Master 失去全部 Replica
- `namespace` 必须等于 Redis Cluster 集群名

## VerifyCluster

`VerifyCluster` 验证 Redis Cluster 满足目标拓扑和安全约束。

Params：

```json
{
  "expectedShards": 2,
  "expectedReplicasPerShard": 1,
  "requireClusterStateOk": true,
  "requireFullSlotCoverage": true,
  "requireAllSlotOwnersHaveReplicas": true
}
```

实现简述：

- Controller 从 Redis Cluster 读取 `CLUSTER INFO`、`CLUSTER NODES`、slot 分布等实际状态
- Controller 同时读取 K8S Pod 等资源状态
- 将 Redis 实际拓扑和 K8S 实际资源合并成状态快照
- Controller 需要连续 2 次观察到相同的健康状态快照后才执行最终校验；若 2 分钟内未稳定则步骤失败
- 校验持有 slots 的 Master 数量、Replica 数量、slot 覆盖、cluster health 和单 Master 安全不变量
- 如果额外健康 Master 暂时不持有 slots，视为 Redis gossip 收敛中，继续等待而不是判定失败
- 校验通过后刷新 `status.topology` 和相关 conditions
- `VerifyCluster` 不修改 Redis Cluster 拓扑，只负责观察、校验和更新状态
- 稳定等待状态保存在当前 step 的 controller 内部 params 中，不属于 Planner DSL 输入

安全校验：

- `expectedShards` 必须与当前 RedisCluster CR spec 中的 `shards` 一致
- `expectedReplicasPerShard` 必须与当前 RedisCluster CR spec 中的 `replicasPerShard` 一致
- `requireClusterStateOk` 必须为 `true`
- `requireFullSlotCoverage` 必须为 `true`
- `requireAllSlotOwnersHaveReplicas` 必须为 `true`
