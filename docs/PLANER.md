# LLM Planner 设计

LLM 在这里不是执行器，只是规划器。

Controller 把当前 RedisCluster 的目标状态和已观察到的实际状态交给 LLM。LLM 返回一个 Action Plan。Controller 再做确定性校验，通过后才把 Plan 写入 `status.activePlan`，并且每次 reconcile 只执行一个 step。

## Context Engineering

Context 分两层。

第一层是固定 System Prompt。它告诉模型：

- 只能输出 Redis Cluster 运维 Plan
- Plan 必须符合固定 DSL
- 只能使用白名单 Action
- 每个 Action 允许哪些参数
- 必须遵守哪些安全不变量
- 输出只能是 JSON，不能解释

第二层是每次 reconcile 的 User Prompt。它只放本次规划必要的信息：

- RedisCluster 名字和 `metadata.generation`
- 目标 `spec`：`shards`、`replicasPerShard`、`image`、`memorySize`
- 下一个可用 Pod 序号 `nextPodOrdinal`
- 当前 Redis topology
- 当前 activePlan
- drift 信息，比如缺失 Pod、replacementPod、lastKnownNodeId

这样做的核心是：不给模型整套集群自由发挥，只给它完成本次 Plan 所需的上下文。

模型要解决的是“从当前状态到目标状态，下一份完整 Plan 应该长什么样”。它不需要知道 controller-runtime、K8S client、Redis client、状态更新细节，也不能直接调用它们。

## 输出约束

输出约束有三道。

第一道是 Prompt 约束。System Prompt 明确要求只输出单个 JSON 对象，不能有 Markdown、解释、注释或额外文本。

Plan 结构固定：

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
      "params": {}
    }
  ]
}
```

第二道是 API 输出格式。当前实现通过 OpenAI-compatible Chat Completions 调用模型，并请求 JSON 输出。代码里也有 `planJSONSchema()`，但当前实际请求用的是 `json_object`，不是严格 `json_schema`。

第三道是 Controller 校验。模型输出会先 `json.Unmarshal` 成 `plan.Plan`。Planner 会强制修正两个字段：

- `dslVersion` 固定为 controller 支持的版本
- `targetGeneration` 固定为当前 CR generation

然后 Validator 再检查：

- Action 必须在白名单内
- namespace 必须等于 RedisCluster 名字
- step id 不能为空且不能重复
- 最后必须用 `VerifyCluster` 收尾
- Create 必须完整覆盖 slots `0-16383`
- 新 Pod 命名必须符合 `redis-<N>`
- 新 Pod 必须从 `nextPodOrdinal` 开始，不能复用历史 Pod 名
- Replica 不能持有 slot
- slot owner master 不能没有 replica
- ScaleOut 的 slot 迁移必须符合 controller 的确定性均衡规则
- Drift 修复只能使用允许的 Action，并且必须使用指定 replacementPod

所以 LLM 可以决定步骤顺序和参数，但不能扩大能力边界。能不能执行，最终由代码决定。

## 运行时关系

LLM 配置来自 ConfigMap。`ConfigMapSource` 定期加载 provider、baseUrl、apiKey、model、maxTokens、temperature、reasoningEffort。`DynamicPlanner` 每次 `Plan()` 都读取当前配置，所以配置变更不用重启 controller。

当前只有一个真实 LLM client：`OpenAIClient`。它使用 OpenAI Go SDK，但 baseUrl 可指向 OpenAI-compatible endpoint，例如 DeepSeek、vLLM、Ollama 或 Anthropic OpenAI-compatible endpoint。

## 当前边界

LLM 不可信。

它不能直接执行 Redis 命令、K8S patch 或 Shell。它只能生成白名单 Action。Action executor 每步执行时仍会重新读取 live K8S / Redis 状态，并按幂等规则执行。

目前 `MigrateSlots`、`DeleteNode` 在 Validator 中是白名单 Action，但 executor 还没实现完整覆盖。Plan 可能通过部分校验，但没有 executor 的 Action 不能真正完成执行。
