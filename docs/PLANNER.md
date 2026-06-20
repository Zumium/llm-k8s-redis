# LLM Planner 设计

LLM 在这里不是执行器，只是规划器。

Controller 把当前 RedisCluster 的目标状态和已观察到的实际状态交给 LLM。LLM 返回一个 Action Plan。Controller 再做确定性校验，通过后才把 Plan 写入 `status.activePlan`，并且每次 reconcile 只执行一个 step。

所有路径都基于本仓库当前代码：`internal/planner/`（planner 实现 + prompt 拼装）和 `internal/llm/`（provider-agnostic 客户端 + OpenAI 兼容适配）。

## 角色

Controller 只依赖一个领域接口，定义在 `internal/planner/types.go`：

```go
type Planner interface {
    Plan(ctx context.Context, req Request) (*plan.Plan, error)
}
```

返回的 `*plan.Plan` **不**被信任，必须经过 `plan.Validator`（`internal/plan/validator.go`）才能落库。

LLM 不能调用 K8S client、Redis client、controller-runtime、shell，只能产出一个白名单 Action 序列。

当前三个 `Planner` 实现，编译期都满足接口（`var _ Planner = (*LLMPlanner)(nil)` 等）：

- `NoopPlanner` — stub，固定返回 `ErrNotConfigured`。
- `LLMPlanner` — 持有固定的 `llm.Client` 和 `Model`，不可热更新。
- `DynamicPlanner` — 每次 `Plan()` 都通过 `PlannerSource.Get()` 拿当前 client/model，承接 `ConfigMapSource` 的 15s 轮询，配置变更不需要重启 controller。

## Context Engineering

Context 分两层。**全是字符串**，没有 tool calling、没有外部函数。

### 第一层：固定 System Prompt

由 `buildSystemPrompt()`（`internal/planner/prompt.go`）拼装。启动时不会变，每次 `Complete()` 都用同一份。结构固定为六段：

1. **角色声明** — 第一行 `You are a Redis Cluster operations planner for a Kubernetes controller.`
2. **DSL 版本** — 写死 `"dslVersion": "redis.ops/v1alpha1"`，直接引用 `plan.DSLVersion` 常量。
3. **Plan schema** — 用一个 JSON 块描述输出结构（`dslVersion` / `planId` / `targetGeneration` / `summary` / `steps[]`）。
4. **Whitelisted actions and their params** — 由 `actionReference()` 渲染。9 个 Action，分别是：
   - `EnsureNode` / `WaitNodeReady` / `MeetNode` / `ReplicateNode`
   - `AddSlots` / `MigrateSlots` / `ForgetNode` / `DeleteNode`
   - `VerifyCluster`
   每一行形如 `- EnsureNode: {"namespace":"<cluster>","pod":"<name>","image":"<image>","memorySize":"<size>"}`，`namespace` 永远等于 `RedisCluster` 名字。
5. **Safety invariants** — 由 `safetyInvariants()` 渲染。覆盖：replica 不能持有 slot、`AddSlots` 必须在 master 已有 replica 之后、Create 用 `AddSlots` 完整覆盖 `0-16383` 且无重叠、ScaleOut 的 `MigrateSlots` 必须满足 controller 确定性均衡规则、Pod 命名 `redis-<N>` 且 N 不补历史缺口等。
6. **Output rules** — 9 条硬性规则。关键几条：
   - "Output ONLY the JSON plan. No markdown fences, no commentary."
   - "Do NOT invent Redis nodeIds; the controller discovers them at runtime."
   - "If observed drift is present, use the exact replacementPod from the observed state, do not use AddSlots or MigrateSlots for a single missing node, and include ForgetNode only when lastKnownNodeId is non-empty."

> Junior 注意：Action 名字、参数名、DSL schema 全部是字符串硬编码在 prompt 里。改一个字段（比如把 `pod` 改成 `podName`）必须**同步**改 `prompt.go` 的 `actionReference()`、`plan.Step.Params` 的字段读取、Validator、executor —— 不要只改一边。

### 第二层：每次 reconcile 的 User Prompt

由 `buildUserPrompt(req Request)` 拼装，结构按顺序：

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
<topology == nil 或无 shard：写 "No existing topology; the cluster has not been created yet.">
<否则：writeTopology 列出每个 shard 的 master + replica，包含 pod / nodeId / slots / ready>
<DriftContext != nil：列出 missingPod / lastKnownNodeId / role / replacementPod / targetMasterPod / observedShards / observedReplicasPerShard>
<LiveContext != nil：writeLiveContext 列出 managed pods、原始 CLUSTER INFO、原始 CLUSTER NODES 和采集错误>

## Task
<Drift 模式：仅可用 EnsureNode / WaitNodeReady / MeetNode / ReplicateNode / ForgetNode / VerifyCluster；用 replacementPod；lastKnownNodeId 非空才出 ForgetNode>
<Create 模式：均分 slot、确保 replica 先于 AddSlots>
<普通过渡模式：从 observed state 推断，不带 operation 字段>
```

末尾固定追加 `Return only the JSON plan.`

`LiveContext` 是 controller 在 reconcile 时按需采集的实时上下文（managed Pod 摘要、原始 `CLUSTER INFO`、原始 `CLUSTER NODES` 以及采集失败信息）。它**只**进本次 user prompt，**不会**写进 `status.topology`；持久化拓扑仍然是 controller 整理过的 `v1alpha1.ClusterTopology`。

`Request`（`internal/planner/types.go`）把这三样东西一起送给 planner：

```go
type Request struct {
    Cluster       *v1alpha1.RedisCluster
    Spec          plan.ClusterSpec
    ObservedState ObservedState
}
type ObservedState struct {
    Topology       *v1alpha1.ClusterTopology
    ActivePlan     *v1alpha1.PlanStatus
    NextPodOrdinal int
    Drift          *plan.DriftContext
    LiveContext    *LiveContext
}
```

`LiveContext` / `PodContext` 字段就是上面 user prompt 里 `writeLiveContext` 直接读出来的那些键。Junior 改 live 采集时，两边要一起改。

## 输出约束

输出约束有三道。

### 第一道：Prompt 约束

System Prompt 末尾的 9 条 Output Rules 把所有"什么不能做"写死。Plan 顶层结构（`internal/plan/types.go` 的 `Plan`）：

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

### 第二道：API 输出格式

`planWithClient()`（`internal/planner/dynamic.go`）构造 `llm.Request` 时显式设置：

```go
ResponseFormat: llm.ResponseFormat{Type: llm.ResponseFormatJSONObject}
```

即请求模型输出 "a single JSON object"，**不**传严格 JSON Schema。

`planJSONSchema()`（`internal/planner/prompt.go`）虽然定义了 schema，但 `planWithClient` 当前**不**调用它，仅作 prompt 内嵌的参考。

`llm.Client` 接口（`internal/llm/types.go`）支持三种 `ResponseFormatType`：
- `ResponseFormatText` — 自由文本
- `ResponseFormatJSONObject` — 单个 JSON 对象
- `ResponseFormatJSONSchema` — 严格 schema；`name` 字段由 `sanitizeSchemaName` 从 schema 的 `title` 派生（仅 `[a-zA-Z0-9_-]`，最长 64 字符，空则回退 `plan`）

`OpenAIClient`（`internal/llm/openai_client.go`）把这三种类型翻译为 OpenAI 的 `OfText` / `OfJSONObject` / `OfJSONSchema`。

为什么 planner 选 `json_object` 而不是 `json_schema`：模型要按 action 用不同 param 形状（`EnsureNode` 的 params 跟 `ForgetNode` 完全不一样），传严格 schema 反而绑死这一点；当前取舍是 prompt + Validator 兜底。

### 第三道：Controller 校验

`json.Unmarshal` 把 `resp.Text` 解到 `plan.Plan` 之后，`planWithClient` 强制覆盖两个字段（LLM 编造了也没用）：

```go
p2.DSLVersion       = plan.DSLVersion
p2.TargetGeneration = req.Spec.Generation
```

然后 controller 在写入 `status.activePlan` 之前调 `plan.Validator`。Validator 检查项（实现细节在 `internal/plan/validator.go`）：

- Action 必须在白名单 9 个值内。
- `namespace` 参数必须等于 `RedisCluster.Name`。
- Step id 非空且全局唯一。
- 最后一步必须是 `VerifyCluster`。
- Create 必须用 `AddSlots` 完整覆盖 `0-16383`，无重叠。
- 新 Pod 命名 `redis-<N>`（N 是单个非负整数），且 N 从 `nextPodOrdinal` 开始，**不**补历史缺口。
- Replica 不能持有 slot；slot owner master 必须有 replica。
- ScaleOut 的 `MigrateSlots` 必须符合 controller 的确定性均衡规则（observed topology 顺序 + EnsureNode 顺序，按 slot 数量均分 `0-16383`）。
- Drift 修复只能使用允许的 Action（`EnsureNode` / `WaitNodeReady` / `MeetNode` / `ReplicateNode` / `ForgetNode` / `VerifyCluster`），且必须用 `DriftContext.ReplacementPod`；`ForgetNode` 的 `lastKnownNodeId` 取 `DriftContext.LastKnownNodeID`，为空就不出 `ForgetNode`。

## 运行时关系

LLM 配置来自 ConfigMap。`llm.ConfigMapSource`（`internal/llm/source.go`）实现 `manager.Runnable`，启动时做一次 `Reload`，然后每 `PollInterval = 15 * time.Second` 轮询一次：

1. `client.Get(ctx, key, &cm)` 拉 ConfigMap。
2. `ParseConfig(cm.Data)` 解析键值对。识别：`provider` / `baseUrl` / `apiKey` / `model` / `maxTokens` / `temperature` / `reasoningEffort` / `tlsInsecureSkipVerify`。未知键忽略（ConfigMap 可以塞额外 metadata）。
3. `cfg.Validate()` 检查：`BaseURL` / `APIKey` / `Model` 非空，`Provider` ∈ {`openai`, `anthropic`}，`ReasoningEffort` ∈ {空, `low`, `medium`, `high`, `max`, `xhigh`}。
4. `NewOpenAIClient(cfg)` 重建 client。
5. `atomic.Pointer[loaded]` 替换快照（读侧零锁）。

`ConfigMapSource.Get()` 返回 `(Config, Client, error)`。首次成功 reload 之前返回 `ErrConfigNotLoaded`，controller 会把集群标 `Failed`，直到 reload 成功。

`DynamicPlanner.Plan()` 每次都通过 `Source.Get()` 拉最新 client + config。`Config` 上的 `MaxTokens` / `Temperature` / `ReasoningEffort` 作为默认值；`DynamicPlanner` 实例上同名字段非零时覆盖（`orInt` / `orFloat` / `orString`）。

当前 `OpenAIClient` 是 `llm.Client` 的唯一实现。它用 `github.com/openai/openai-go/v3`，把请求发到 `cfg.BaseURL + "/chat/completions"`：

- `option.WithAPIKey(cfg.APIKey)` + `option.WithBaseURL(cfg.BaseURL)` 是必装。
- `cfg.TLSInsecureSkipVerify` 为真时附加一个跳过证书校验的 `http.Client`（`crypto/tls` + `InsecureSkipVerify: true`）。
- `req.Messages` 翻译成 OpenAI 的 `ChatCompletionMessageParamUnion`（system / user / assistant）；多个 `ContentPart` 当前只拼 text（`joinContentParts`）。
- `req.MaxTokens` / `req.Temperature` / `req.ReasoningEffort` 优先于 `cfg` 同名字段（按 `> 0` / `!= ""` 判），再回退到 `cfg`。
- `ResponseFormat` 翻译为 OpenAI 的 `OfText` / `OfJSONObject` / `OfJSONSchema`；`json_schema` 模式下 `name` 来自 schema 的 `title` 字段经 `sanitizeSchemaName` 清洗。

`cfg.BaseURL` 可以指向任何 OpenAI 兼容的 `/v1/chat/completions` 端点：OpenAI、Azure OpenAI、vLLM、Ollama、DeepSeek、Anthropic OpenAI-compatible 层。当前 `provider: anthropic` **不**调用 Anthropic 原生 Messages API，仍然走 OpenAI SDK + 兼容端点（见 "当前边界"）。

CLI 暴露的 flag（`cmd/manager/main.go`）：
- `--llm-configmap-name`（默认 `llm-config`）
- `--llm-configmap-namespace`（默认 `redis-cluster-system`）
- `--disable-llm-planner`（为 true 时 controller 改用 `NoopPlanner`）

## 当前边界

LLM 不可信。

- 它不能直接执行 Redis 命令、K8S patch 或 shell。
- 它只能生成白名单 9 个 Action。
- Action executor 每步执行时会重新读取 live K8S / Redis 状态，按幂等规则执行；LLM 假设的状态和真实状态之间可能存在漂移，executor 不会盲目重做 LLM 写下的"假设"。

`ActionExecutor.ExecuteStep`（`internal/controller/actions.go`）当前实现的 Action：
- `EnsureNode` / `WaitNodeReady` / `MeetNode` / `ReplicateNode` / `AddSlots` / `MigrateSlots` / `ForgetNode` / `VerifyCluster`（8 个）

唯一尚未实现的：`DeleteNode`。Plan 里出现 `DeleteNode` 时会进 `default` 分支，返回 `Failed` 状态加 `action "DeleteNode" is not yet implemented` 的 message，plan 停在该步不再推进。`DeleteNode` 真正接通前，不要在生产 plan 里放它。

- `provider: anthropic` 当前仅走 OpenAI SDK 兼容端点，没有 `internal/llm/anthropic_client.go`。
- 单元测试用 `sigs.k8s.io/controller-runtime/pkg/client/fake` + `httptest` 模拟 LLM 端点，不依赖 envtest 或真实 LLM。
