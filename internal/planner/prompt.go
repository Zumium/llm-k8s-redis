package planner

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
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
	b.WriteString("4. If there is no existing topology, create the full cluster from scratch and end with VerifyCluster.\n")
	b.WriteString("5. If topology exists, transition from observed state to desired spec with the smallest safe action sequence.\n")
	b.WriteString("6. AddSlots across all Create steps must cover exactly slots 0-16383 with no overlaps.\n")
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
		{plan.ActionForgetNode, `{"namespace":"<cluster>","pod":"<name>"}`},
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
- Every namespace param must equal the RedisCluster name.
- Every pod referenced by an action must be declared by a preceding EnsureNode.
- All Redis pods must be named "redis-<N>" where <N> is a single non-negative integer starting from 0. Do NOT embed the cluster name or any other prefix. Correct examples: redis-0, redis-1, redis-2. Wrong examples: redis-3s1r-0, redis-cluster-0, redis-example-0. Pod names must be contiguous within each plan; for ScaleOut, new pods must continue from the highest existing ordinal plus one.
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

	b.WriteString("## Observed state\n")
	if req.ObservedState.Topology == nil || len(req.ObservedState.Topology.Shards) == 0 {
		b.WriteString("No existing topology; the cluster has not been created yet.\n\n")
	} else {
		b.WriteString("Current topology:\n")
		writeTopology(&b, req.ObservedState.Topology)
		b.WriteString("\n")
	}

	b.WriteString("## Task\n")
	if req.ObservedState.Topology == nil || len(req.ObservedState.Topology.Shards) == 0 {
		b.WriteString("Generate a Create plan that provisions the full Redis Cluster matching the desired spec.\n")
		b.WriteString("Distribute slots evenly across all masters. Ensure every master has replicas before assigning slots.\n")
	} else {
		b.WriteString("Generate a plan that safely transitions the observed state to the desired spec.\n")
		b.WriteString("Infer the required path from the current topology and desired spec; do not include an operation field.\n")
	}
	b.WriteString("Return only the JSON plan.\n")
	return b.String()
}

func writeTopology(b *strings.Builder, t *v1alpha1.ClusterTopology) {
	for _, sh := range t.Shards {
		fmt.Fprintf(b, "- shard %s: master pod=%s nodeId=%s slots=%q ready=%v\n",
			sh.ID, sh.Master.Pod, sh.Master.NodeID, sh.Master.Slots, sh.Master.Ready)
		for _, r := range sh.Replicas {
			fmt.Fprintf(b, "    replica pod=%s nodeId=%s ready=%v\n", r.Pod, r.NodeID, r.Ready)
		}
	}
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
