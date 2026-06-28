# LLM Planner 设计

LLM 在这里不是执行器，只是规划器。

Controller 把当前 RedisCluster 的目标状态和已观察到的实际状态交给 LLM。LLM 返回一个 Action Plan。Controller 再做确定性校验，通过后才把 Plan 写入 `status.activePlan`，并且每次 reconcile 只执行一个 step。

所有路径都基于本仓库当前代码：`internal/planner/`（planner 实现、prompt 拼装、provider-agnostic 客户端、OpenAI 兼容适配）和 `internal/plan/`（Plan DSL + Validator）。

## 角色

Controller 只依赖一个领域接口，定义在 `internal/planner/types.go`：

```go
type Planner interface {
    Plan(ctx context.Context, req Request) (*plan.Plan, error)
}
```

返回的 `*plan.Plan` **不**被信任，必须经过 `plan.Validator`（`internal/plan/validator.go`）才能落库。

LLM 不能调用 K8S client、Redis client、controller-runtime、shell，只能产出一个白名单 Action 序列。

**设计哲学：LLM 是规划者，不是状态机。** Controller 只把 desired spec 和 observed state 投喂给 LLM，不替 LLM 决定"现在该走哪条路"。文档里可以用"首次建群 / 扩 shard / 漂移 / 删除"这些场景词讨论问题，但它们不是 DSL 字段，也不进入 `status.activePlan`。LLM 先用内部 JSON 分析本次可能涉及的子过程（`repairTopology` / `cleanupGhostNodes` / `cleanupDirtyNodes` / `changeClusterSpec`），再基于这个分析和白名单 Action 生成序列；Validator 决定这个序列是否安全、是否能落地。

当前 `Planner` 实现只有`LLMPlanner`一种 — 持有固定的 `planner.Client`，不可热更新。

## Context Engineering

Context 分两层。**全是字符串**，没有 tool calling、没有外部函数。

### 第一层：固定 System Prompt

由 `buildSystemPrompt()`（`internal/planner/prompt.go`）拼装。启动时不会变，每次 `Complete()` 都用同一份。结构固定为六段，且不携带 worked examples：

1. **角色声明** — 第一行 `You are a Redis Cluster operations planner for a Kubernetes controller.`
2. **DSL 版本** — 写死 `"dslVersion": "redis.ops/v1alpha1"`，直接引用 `plan.DSLVersion` 常量。
3. **Plan schema** — 用一个 JSON 块描述输出结构（`dslVersion` / `planId` / `targetGeneration` / `summary` / `steps[]`）。
4. **Whitelisted actions and their params** — 由 `actionReference()` 渲染。9 个 Action，分别是：
   - `EnsureNode` / `WaitNodeReady` / `MeetNode` / `ReplicateNode`
   - `AddSlots` / `MigrateSlots` / `ForgetNode` / `DeleteNode`
   - `VerifyCluster`
   每一行形如 `- EnsureNode: Ensure the Redis Pod exists with the desired image and memory; does not meet nodes, set replicas, or assign slots. Params: {"namespace":"<cluster>","pod":"<name>","image":"<image>","memorySize":"<size>"}`，`namespace` 永远等于 `RedisCluster` 名字。说明只保留会影响 LLM 选择 Action 和排序的关键语义，例如 `AddSlots` 只处理未归属 slots，`MigrateSlots` 执行器会分批继续，`VerifyCluster` 只统计持有 slots 的 master，额外无 slot master 先视为 gossip 收敛等待。
5. **Safety invariants** — 由 `safetyInvariants()` 渲染。覆盖：replica 不能持有 slot、`AddSlots` 必须在 master 已有 replica 之后、首次建群用 `AddSlots` 完整覆盖 `0-16383` 且无重叠、扩 shard 的 `MigrateSlots` 必须满足 controller 确定性均衡规则、Pod 命名 `redis-<N>` 且 N 不补历史缺口等。
6. **Output rules** — 输出格式的硬性约束。它可以要求模型读取 observed state，但不应把"create / scale / drift / delete"作为显式 `operation` 或 `mode` 模板交给模型。规则样本：
   - "Output ONLY the JSON plan. No markdown fences, no commentary."
   - "Do NOT invent Redis nodeIds; the controller discovers them at runtime."
   - "Every pod-targeting action must include a namespace param equal to the cluster name."
   - "All Redis pods must be named redis-<N> where N is a single non-negative integer."

> 注意：Action 名字、参数名、DSL schema 全部是字符串硬编码在 prompt 里。改一个字段（比如把 `pod` 改成 `podName`）必须**同步**改 `prompt.go` 的 `actionReference()`、`plan.Step.Params` 的字段读取、Validator、executor —— 不要只改一边。

### 第二层：每次 reconcile 的 User Prompt

由 `buildClusterContextPrompt(req Request)` 拼装基础上下文，结构按顺序：

```
RedisCluster name: <name>
metadata.generation: <gen>

## Desired spec
shards: <n>
replicasPerShard: <n>
image: <image>
memorySize: <size>

nextPodOrdinal: <n>

## Observed state
<ObservedState.Nodes：实时对账后的节点事实表；每行写 pod / podExists / redisSeen / nodeId / role / slots / masterId / masterPod / ready / deleting / flags / linkState>

```

`LLMPlanner.Plan()` 会调用 LLM 两次。第一次用 `buildAnalysisPrompt()` 要求只返回：

```json
{"subprocesses":["changeClusterSpec"],"summary":"one-line reason"}
```

第二次用 `buildUserPrompt()` 加上第一次的 analysis JSON，要求返回现有 Plan DSL。analysis JSON 只作为模型上下文，不持久化、不参与 controller 分支，也不替代 Validator。第二阶段 prompt 会按 analysis 文本里的标签追加对应 worked examples：`repairTopology`、`cleanupGhostNodes`、`cleanupDirtyNodes`、`changeClusterSpec` 各自只追加自己的示例；多个标签会按这个顺序拼接多组示例。

`ObservedState.Nodes` **不**从 `status.topology` 直接拷贝。Controller 在 reconcile 起点重新读取 K8S Pods 与 Redis `CLUSTER NODES`，再结合 `status.topology` 中的最后已知 pod ↔ nodeId 关系，构造一张面向 planner 的节点事实表。LLM 看到的 observed state 永远是这一次的实时事实，不是上一次的健康拓扑摘要。

`ActivePlan` 只用于决定是否需要重新规划。若当前 `activePlan` 仍处于 `Pending` 或 `Running`，Controller 不调用 LLM，不干扰当前计划，只继续执行下一步。只有当当前 plan 已经进入最终状态（`Completed` 或 `Failed`），或者没有 active plan 时，Controller 才基于最新 observed state 请求 LLM 重新规划。

`Request`（`internal/planner/types.go`）把这些状态一起送给 planner：

```go
type Request struct {
    Spec          plan.ClusterSpec
    ObservedState ObservedState
}

type ObservedState struct {
    // 当前正在执行的Plan（若有）
    ActivePlan     *v1alpha1.PlanStatus
    // 下一个Pod名使用的序号
    NextPodOrdinal int
    // Nodes 是 K8S Pods 与 Redis CLUSTER NODES join 后的当前事实表。
    Nodes          []ObservedNode
}

type ObservedNode struct {
    Pod          string
    PodExists    bool
    RedisSeen    bool
    NodeID       string
    Role         string // "master" / "replica" / "unknown"
    Slots        string
    MasterID     string
    MasterPod    string
    Ready        bool
    Deleting     bool
    Flags        []string
    LinkState    string // seed node's CLUSTER NODES link-state
}
```

`ObservedNode` 是事实表, 分类由字段组合自然表达：`podExists=false && redisSeen=true` 表示 Redis 残留节点，`podExists=true && redisSeen=false` 表示尚未加入 Redis Cluster 的 Pod，二者都为 true 表示 K8S 与 Redis 能对上。

字段含义：

- `Pod`：节点的Pod名称
- `PodExists`：表达 K8S 当前是否还有这个 Pod
- `RedisSeen`：表达该节点是否出现在 `CLUSTER NODES` 中
- `NodeID`：Redis 节点身份锚点
- `Role`：master / replica / unknown
- `Slots`：表达 slot ownership
- `MasterID`：Redis 原生 replica 关系使用 master nodeId 表达
- `MasterPod`：由 `MasterID` join 得出
- `Ready`：K8S Pod readiness
- `Deleting`：删除中的 Pod 即使还 Ready，也不应作为稳定 source/target。
- `Flags`：来自 seed 节点本次 `CLUSTER NODES` 视图的 raw flags，例如 `master`、`slave`、`myself`、`fail?`、`fail`、`handshake`、`noaddr`。
- `LinkState`：来自同一行 `CLUSTER NODES` 的 link-state，通常为 `connected` / `disconnected`；它只表示 seed 视角下到该 node 的 cluster-bus 连接状态，不是全局健康结论。

`Spec` 是 controller 从 `RedisCluster` 投影出来的目标状态；planner 不接收完整 K8S object。`name` 和 `metadata.generation` 留在 `plan.ClusterSpec` 里，是因为 planner 需要它们生成 `namespace` 参数和 `targetGeneration`。这样避免 `Cluster.Spec` 与 `Spec` 出现双来源事实，也让 planner 不依赖 Kubernetes API 类型。

## 输出约束

输出约束有三道。

### 第一道：Prompt 约束

System Prompt 末尾的 Output Rules 只写输出格式和通用硬约束，不替 LLM 写状态分支。Plan 顶层结构（`internal/plan/types.go` 的 `Plan`）：

```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "create-001",
  "targetGeneration": 1,
  "summary": "create redis cluster",
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
    }
  ]
}
```

`Step` 字段：
- `id` (string) — 唯一 step id。Validator 会查重。
- `action` (`ActionType`) — 必须落在白名单 9 个值内。
- `params` (`map[string]any`) — JSON 对象。`encoding/json` 解到 `map[string]any`；具体字段读取由 executor 在执行时按 action 类型从 `Params` 里取。

`params` schema 是 DSL 契约的一部分。所有 string 参数都必须是非空字符串；所有 pod 都必须是 `redis-<N>`，其中 N 是单个非负整数；`namespace` 必须等于 `RedisCluster.Name`。

| Action | Params |
| --- | --- |
| `EnsureNode` | `namespace` string, `pod` string, `image` string, `memorySize` string |
| `WaitNodeReady` | `namespace` string, `pod` string |
| `MeetNode` | `namespace` string, `sourcePod` string, `targetPod` string |
| `ReplicateNode` | `namespace` string, `masterPod` string, `replicaPod` string |
| `AddSlots` | `namespace` string, `pod` string, `slots` string |
| `MigrateSlots` | `namespace` string, `sourcePod` string, `targetPod` string, `slots` string |
| `ForgetNode` | `namespace` string, `pod` string, optional `lastKnownNodeId` string |
| `DeleteNode` | `namespace` string, `pod` string |
| `VerifyCluster` | `expectedShards` integer, `expectedReplicasPerShard` integer, `requireClusterStateOk` bool, `requireFullSlotCoverage` bool, `requireAllSlotOwnersHaveReplicas` bool |

`ForgetNode.lastKnownNodeId` 只有在目标 Pod 已不存在、无法从 live Pod join 到 Redis nodeId 时才是必填；只要目标 Pod 还存在，executor 可以从 `CLUSTER NODES` 解析出 nodeId。`slots` 必须能被 controller 的 slot parser 解析，且每个 slot 都在 `0-16383` 范围内。

### 第二道：API 输出格式

这一道只管一件事：让模型返回**可被 `json.Unmarshal` 解析的 JSON 对象**。

`OpenAIClient` 发请求时固定设置：

```go
ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
    OfJSONObject: &shared.ResponseFormatJSONObjectParam{},
}
```

含义是：要求 OpenAI-compatible endpoint 使用 JSON object 模式。模型不能返回 markdown、解释文字或多个对象；返回值应该是一个 JSON object，后面会被解析成 `plan.Plan`。

这里**没有**启用严格 JSON Schema：

- `json_object`：只约束“输出必须是 JSON 对象”。
- `json_schema`：还会约束字段结构、必填项、类型等。

当前 planner 选择 `json_object`，因为不同 action 的 `params` 形状不一样。例如 `EnsureNode` 需要 `pod/image/memorySize`，`ForgetNode` 需要 `pod/lastKnownNodeId`。如果把 schema 写得很严格，就必须把每种 action 的 params 都编码进 schema，复杂度高，收益不大。

所以当前分工是：

1. Prompt 告诉模型 Plan 应该长什么样。
2. API 的 `json_object` 模式保证返回的是 JSON 对象。
3. Controller 再用 `json.Unmarshal` 解析。
4. Validator 最后检查 action、参数、命名、slot、安全顺序等业务规则。

### 第三道：Controller 校验

`json.Unmarshal` 把 `resp.Text` 解到 `plan.Plan` 之后，`LLMPlanner` 强制覆盖两个字段（LLM 编造了也没用）：

```go
p2.DSLVersion       = plan.DSLVersion
p2.TargetGeneration = req.Spec.Generation
```

然后 controller 在写入 `status.activePlan` 之前调 `plan.Validator`。Validator 是一个安全不变量 VM：从 observed state 初始化抽象拓扑，按 Plan 顺序模拟每个 Action，观察是否出现危险中间态，以及最终是否到达当前 `spec` 对应的规范目标态。

第一层是基础 envelope 校验：

- Action 必须在白名单 9 个值内。
- `namespace` 参数必须等于 `RedisCluster.Name`。
- Step id 非空且全局唯一。
- 最后一步必须是 `VerifyCluster`。

第二层是 VM 模拟校验。VM 只模拟安全判断需要的抽象状态：node 是否存在、是否 ready、是否是 cluster member、master/replica 角色、`replicaOf`、slot ownership、是否已 `ForgetNode`、是否已 `DeleteNode`。每执行一个 Action transition 后，立即检查中间态安全不变量：

- Replica 不能持有 slot。
- 持有 slot 的 master 必须至少有一个 replica。
- 同一个 slot 不能有多个 owner。
- `AddSlots` 只能作用于已有 replica 的 master，不能作用于 replica。
- `MigrateSlots` 的 source/target 必须是 master，且 target master 必须已有 replica。
- `ForgetNode` 不能忘记仍持有 slot 的 master。
- `DeleteNode` 只能删除已从 Redis Cluster forget、或从未加入 Redis Cluster 的节点；删除后不能让任何持有 slot 的 master 失去最后一个 replica。

Validator 的 Action transition 必须完全契合实际 executor 的行为模式，不能为了让 plan 更容易通过而发明 executor 不会做的状态变化：

| Action | VM transition |
| --- | --- |
| `EnsureNode` | 确保 Pod 存在；校验 `image` / `memorySize` 等于 spec；不加入 Redis Cluster，不设置 master/replica，不分配 slots。 |
| `WaitNodeReady` | 要求目标 Pod 已由 `EnsureNode` 声明或已在 observed topology 中；把该 Pod 标记为 ready；不构造 shard。 |
| `MeetNode` | 要求 source/target ready，且二者不同；把二者标记为 cluster member；没有角色的 member 可作为 master 候选。 |
| `ReplicateNode` | 要求 master/replica 都是 ready cluster member；master 必须是 master；replica 不能持有 slots；把 `replicaPod` 标记为 `masterPod` 的 replica。 |
| `AddSlots` | 要求目标是 ready cluster member master，且已有至少一个 replica；只能给未归属 slots 建立 owner；如果 slot 已属其他 master，必须拒绝。 |
| `MigrateSlots` | 要求 source/target 都是 ready healthy master，且 target 已有至少一个 replica；source 和 target 不能相同；目标 slots 当前 owner 只能是 source 或 target；迁移后 owner 变为 target。 |
| `ForgetNode` | 要求目标已知为 cluster member；如果目标仍持有 slots，必须拒绝；通过后目标不再是 cluster member，但 K8S Pod 仍可存在。 |
| `DeleteNode` | 要求目标已被 forget 或从未加入 Redis Cluster；通过后 Pod 不存在；如果删除会让任何持有 slots 的 master 失去最后一个 replica，必须拒绝。 |
| `VerifyCluster` | 不修改 VM 拓扑；只验证最终态匹配 spec、slots 完整覆盖、每个 slot-owning master 的 replica 数等于目标值。 |

第三层是最终态和规范态校验。`VerifyCluster` 必须匹配当前 `spec.shards` 和 `spec.replicasPerShard`，slots 必须完整覆盖 `0-16383`，每个 slot-owning master 的 replica 数必须等于目标值。所有 Redis Pod 都必须命名为 `redis-<N>`（N 是单个非负整数）；无论这个 Pod 来自 observed state 还是当前 plan，只要命名不合规，Validator 必须拒绝。新 Pod 必须从 `nextPodOrdinal` 开始单调递增，**不**补历史缺口。

扩 shard 的 `MigrateSlots` 必须符合 controller 的确定性均衡规则。Master 顺序是：当前 topology 中 master 的顺序，后接新 master 的 `EnsureNode` 顺序。设 master 总数为 `total`，第 `i` 个 master 从 0 开始编号：

```text
base = 16384 / total
rem  = 16384 % total
size = base + 1, if i < rem
size = base,     if i >= rem
start = i * base + min(i, rem)
end   = start + size - 1
```

也就是说，`0-16383` 尽量均分，不能整除时前面的 master 各多持有 1 个 slot。LLM 生成的 `MigrateSlots` 必须覆盖从当前 owner 到目标 owner 的完整迁移矩阵；迁移矩阵可以包含 existing master 到 existing master 的迁移，不只包含迁入新增 master 的迁移。

Validator 不做完整 Redis 模拟器：不模拟 key migration 批处理、`IMPORTING` / `MIGRATING` 临时细节或 failover 时序。这些属于 executor 的 live-state 安全检查范围。即使 Plan 已通过 Validator，Action executor 每步执行前仍必须重新读取 live K8S / Redis 状态并重复校验安全前置条件。

Validator 不以 operation 分支作为核心模型；首次建群、扩 shard、漂移收敛、删除等词只用于文档讨论，不是 DSL 字段或 prompt 场景标签。这些差异应落到初始状态、Action transition 和最终态策略里，由 controller 的确定性安全网裁决，而不是预设给 LLM 的模式分支。

## 运行时关系

Controller 启动时从 ConfigMap 加载 LLM 配置，初始化 `planner.Client`，再把 client 交给 `LLMPlanner`。模型、`maxTokens`、`temperature`、`reasoningEffort`、`planValidationRetries` 都保存在 `OpenAIClient` 的 `Config` 里。`LLMPlanner` 不热更新；运行期要换模型、baseUrl、apiKey 或重试次数，就更新 ConfigMap 后重启 controller。

当前 `OpenAIClient` 是 `planner.Client` 的唯一实现。它用 `github.com/openai/openai-go/v3`，把请求发到 `cfg.BaseURL + "/chat/completions"`：

- `option.WithAPIKey(cfg.APIKey)` + `option.WithBaseURL(cfg.BaseURL)` 是必装。
- `LLMRequest.System` 和 `LLMRequest.Prompt` 默认翻译成 system/user 两条消息；validator 反馈重试时改用显式消息流，把被拒 plan 作为 assistant 消息、validator 意见作为下一条 user 消息。
- `cfg.MaxTokens` / `cfg.Temperature` / `cfg.ReasoningEffort` 非零或非空时写入请求。
- `ResponseFormat` 固定为 `json_object`。

`cfg.BaseURL` 可以指向任何 OpenAI 兼容的 `/v1/chat/completions` 端点：OpenAI、Azure OpenAI、vLLM、Ollama、DeepSeek、Anthropic OpenAI-compatible 层。`provider` ConfigMap key 只为兼容旧配置保留，不参与分支。

## 当前边界

LLM 不可信。

- 它不能直接执行 Redis 命令、K8S patch 或 shell。
- 它只能生成白名单 9 个 Action。
- Action executor 每步执行时会重新读取 live K8S / Redis 状态，按幂等规则执行；LLM 假设的状态和真实状态之间可能存在漂移，executor 不会盲目重做 LLM 写下的"假设"。

`ActionExecutor.ExecuteStep`（`internal/controller/actions.go`）按白名单 Action 分发。每个 Action handler 都必须幂等，并在执行前重新读取 live K8S / Redis 状态，重复校验和 Validator 一致的安全前置条件。

- 没有原生 Anthropic Messages API adapter。
- 单元测试用 `sigs.k8s.io/controller-runtime/pkg/client/fake` + `httptest` 模拟 LLM 端点，不依赖 envtest 或真实 LLM。
