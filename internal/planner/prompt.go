package planner

import (
	"fmt"
	"strings"

	"github.com/example/llm-k8s-redis/internal/plan"
)

// buildSystemPrompt constructs the system prompt that tells the LLM its role,
// the DSL schema, the available actions, and the safety invariants it must
// respect. The prompt is provider-agnostic: it works with any chat model.
func buildSystemPrompt() string {
	var b strings.Builder
	b.WriteString("You are a Redis Cluster operations planner for a Kubernetes controller.\n")
	b.WriteString("Your job is to produce a JSON plan that the controller will validate and execute step by step.\n\n")

	b.WriteString("## DSL version\n")
	b.WriteString(fmt.Sprintf("Every plan must set \"dslVersion\" to %q.\n\n", plan.DSLVersion))

	b.WriteString("## Plan schema\n")
	b.WriteString("The output must be a single JSON object matching this structure:\n")
	b.WriteString("```json\n")
	b.WriteString(`{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "<unique short id, e.g. create-001>",
  "targetGeneration": <integer, the cluster generation you are planning for>,
  "summary": "<one-line human-readable description>",
  "steps": [
    {
      "id": "<unique step id>",
      "action": "<one of the whitelisted actions>",
      "params": { ... }
    }
  ]
}`)
	b.WriteString("\n```\n\n")

	b.WriteString("## Whitelisted actions and their params\n")
	b.WriteString(actionReference())
	b.WriteString("\n")

	b.WriteString("## Safety invariants (the controller will reject plans that violate these)\n")
	b.WriteString(safetyInvariants())
	b.WriteString("\n")

	b.WriteString("## Output rules\n")
	b.WriteString("1. Output ONLY the JSON plan. No markdown fences, no commentary.\n")
	b.WriteString("2. Every pod-targeting action must include a \"namespace\" param equal to the cluster name.\n")
	b.WriteString("3. Do NOT invent Redis nodeIds; the controller discovers them at runtime.\n")
	b.WriteString("4. All Redis pods must be named redis-<N> where N is a single non-negative integer.\n")
	b.WriteString("5. Reconcile desired spec, live Pods, and CLUSTER NODES facts before choosing actions.\n")
	return b.String()
}

// actionReference returns the per-action param reference table.
func actionReference() string {
	var b strings.Builder
	type actionDoc struct {
		name   plan.ActionType
		params string
	}
	docs := []actionDoc{
		{plan.ActionEnsureNode, `{"namespace":"<cluster>","pod":"<name>","image":"<image>","memorySize":"<size>"}`},
		{plan.ActionWaitNodeReady, `{"namespace":"<cluster>","pod":"<name>"}`},
		{plan.ActionMeetNode, `{"namespace":"<cluster>","sourcePod":"<name>","targetPod":"<name>"}`},
		{plan.ActionReplicateNode, `{"namespace":"<cluster>","masterPod":"<name>","replicaPod":"<name>"}`},
		{plan.ActionAddSlots, `{"namespace":"<cluster>","pod":"<name>","slots":"<start-end>"}`},
		{plan.ActionMigrateSlots, `{"namespace":"<cluster>","sourcePod":"<name>","targetPod":"<name>","slots":"<start-end>"}`},
		{plan.ActionForgetNode, `{"namespace":"<cluster>","pod":"<last-known-name>","lastKnownNodeId":"<node-id-for-deleted-pod>"}`},
		{plan.ActionDeleteNode, `{"namespace":"<cluster>","pod":"<name>"}`},
		{plan.ActionVerifyCluster, `{"expectedShards":<n>,"expectedReplicasPerShard":<n>,"requireClusterStateOk":true,"requireFullSlotCoverage":true,"requireAllSlotOwnersHaveReplicas":true}`},
	}
	for _, d := range docs {
		fmt.Fprintf(&b, "- %s: %s\n", d.name, d.params)
	}
	return b.String()
}

// safetyInvariants lists the hard constraints the Validator enforces.
func safetyInvariants() string {
	return strings.TrimSpace(`- The cluster must never have a shard with zero replicas for a slot-owning master, not even transiently.
- AddSlots must run only after the target master has at least one replica.
- A replica must not hold slots.
- Slots 0-16383 must be fully covered exactly once across all AddSlots steps.
- For shard ScaleOut, MigrateSlots must exactly rebalance slots to the controller rule: existing masters in observed topology order, then new masters in EnsureNode order, with slots 0-16383 split as evenly as possible.
- Every namespace param must equal the RedisCluster name.
- Every new pod referenced by WaitNodeReady/MeetNode/ReplicateNode/AddSlots must be declared by a preceding EnsureNode.
- All Redis pods must be named "redis-<N>" where <N> is a single non-negative integer. Do NOT embed the cluster name or any other prefix. Correct examples: redis-0, redis-1, redis-2. Wrong examples: redis-3s1r-0, redis-cluster-0, redis-example-0. Pod names are globally non-reusable. Create uses redis-0 upward; all later new pods must start at the provided nextPodOrdinal and must not fill historical gaps.
- Plans for single-node drift must not use AddSlots or MigrateSlots, must use replacementPod for EnsureNode, and must use lastKnownNodeId when forgetting a deleted pod. If lastKnownNodeId is empty, do not include ForgetNode.
- sourcePod and targetPod (or masterPod and replicaPod) must not be the same pod.`)
}

// buildUserPrompt constructs the per-cluster user prompt with the concrete spec
// and observed state.
func buildUserPrompt(req Request) string {
	var b strings.Builder
	fmt.Fprintf(&b, "RedisCluster name: %s\n", req.Spec.Name)
	fmt.Fprintf(&b, "metadata.generation: %d\n\n", req.Spec.Generation)

	b.WriteString("## Desired spec\n")
	fmt.Fprintf(&b, "shards: %d\n", req.Spec.Shards)
	fmt.Fprintf(&b, "replicasPerShard: %d\n", req.Spec.ReplicasPerShard)
	fmt.Fprintf(&b, "image: %s\n", req.Spec.Image)
	fmt.Fprintf(&b, "memorySize: %s\n\n", req.Spec.MemorySize)
	fmt.Fprintf(&b, "nextPodOrdinal: %d\n\n", req.ObservedState.NextPodOrdinal)

	b.WriteString("## Observed state\n")
	writeObservedNodes(&b, req.ObservedState.Nodes)
	b.WriteString("\n")

	b.WriteString("## Task\n")
	b.WriteString("Bring the cluster from the observed state to the desired spec. Pick whichever whitelisted action sequence you think is safest; the controller's Validator is the final safety net.\n")
	b.WriteString("Return only the JSON plan.\n")
	return b.String()
}

func writeObservedNodes(b *strings.Builder, nodes []ObservedNode) {
	if len(nodes) == 0 {
		b.WriteString("No observed nodes.\n")
		return
	}
	b.WriteString("pod | podExists | redisSeen | nodeId | role | slots | masterId | masterPod | ready | deleting | flags | linkState\n")
	for _, n := range nodes {
		fmt.Fprintf(b, "%s | %v | %v | %s | %s | %s | %s | %s | %v | %v | %s | %s\n",
			dash(n.Pod), n.PodExists, n.RedisSeen, dash(n.NodeID), dash(n.Role), dash(n.Slots),
			dash(n.MasterID), dash(n.MasterPod), n.Ready, n.Deleting, dash(strings.Join(n.Flags, ",")), dash(n.LinkState))
	}
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// planJSONSchema returns a minimal JSON Schema for the Plan object. Providers
// that support structured output (OpenAI json_schema, Anthropic tools) can use
// it to constrain the model's output. It is intentionally permissive on params
// (additionalProperties: true) because action params vary by action type.
func planJSONSchema() map[string]any {
	stepSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":     map[string]any{"type": "string"},
			"action": map[string]any{"type": "string"},
			"params": map[string]any{"type": "object"},
		},
		"required":             []string{"id", "action", "params"},
		"additionalProperties": false,
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dslVersion":       map[string]any{"type": "string"},
			"planId":           map[string]any{"type": "string"},
			"targetGeneration": map[string]any{"type": "integer"},
			"summary":          map[string]any{"type": "string"},
			"steps": map[string]any{
				"type":  "array",
				"items": stepSchema,
			},
		},
		"required":             []string{"dslVersion", "planId", "targetGeneration", "steps"},
		"additionalProperties": false,
	}
}
