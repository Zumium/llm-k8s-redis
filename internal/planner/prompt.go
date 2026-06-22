package planner

import (
	"fmt"
	"strings"

	"github.com/example/llm-k8s-redis/internal/plan"
)

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

func actionReference() string {
	var b strings.Builder
	type actionPromptLine struct {
		name        plan.ActionType
		description string
		params      string
	}
	lines := []actionPromptLine{
		{plan.ActionEnsureNode, "Ensure the Redis Pod exists with the desired image and memory; does not meet nodes, set replicas, or assign slots.", `{"namespace":"<cluster>","pod":"<name>","image":"<image>","memorySize":"<size>"}`},
		{plan.ActionWaitNodeReady, "Wait until the Pod is Ready and Redis is reachable.", `{"namespace":"<cluster>","pod":"<name>"}`},
		{plan.ActionMeetNode, "Join targetPod to the Redis Cluster gossip network that sourcePod belongs to.", `{"namespace":"<cluster>","sourcePod":"<name>","targetPod":"<name>"}`},
		{plan.ActionReplicateNode, "Make replicaPod replicate from masterPod; replicaPod must not own slots.", `{"namespace":"<cluster>","masterPod":"<name>","replicaPod":"<name>"}`},
		{plan.ActionAddSlots, "Assign only unowned slots to a master; do not use it to move slots already owned by another master.", `{"namespace":"<cluster>","pod":"<name>","slots":"<start-end>"}`},
		{plan.ActionMigrateSlots, "Move slots from source master to target master; slots already on target are treated as done and execution continues in batches.", `{"namespace":"<cluster>","sourcePod":"<name>","targetPod":"<name>","slots":"<start-end>"}`},
		{plan.ActionForgetNode, "Remove a node from Redis Cluster membership without deleting its Pod; include lastKnownNodeId when the Pod is gone.", `{"namespace":"<cluster>","pod":"<name>","lastKnownNodeId":"<optional last-known node id only when pod is gone>"}`},
		{plan.ActionDeleteNode, "Delete the Kubernetes resources for a node; only safe after ForgetNode or if the node never joined Redis Cluster.", `{"namespace":"<cluster>","pod":"<name>"}`},
		{plan.ActionVerifyCluster, "Verify final state without changing topology; count only slot-owning masters, and wait for extra no-slot masters as gossip convergence instead of immediate shard mismatch.", `{"expectedShards":<n>,"expectedReplicasPerShard":<n>,"requireClusterStateOk":true,"requireFullSlotCoverage":true,"requireAllSlotOwnersHaveReplicas":true}`},
	}
	for _, line := range lines {
		fmt.Fprintf(&b, "- %s: %s Params: %s\n", line.name, line.description, line.params)
	}
	return b.String()
}

func safetyInvariants() string {
	return strings.TrimSpace(`- The cluster must never have a shard with zero replicas for a slot-owning master, not even transiently.
- AddSlots must run only after the target master has at least one replica.
- A replica must not hold slots.
- Slots 0-16383 must be fully covered exactly once across all AddSlots steps.
- For shard ScaleOut, MigrateSlots must exactly rebalance slots to the controller rule: existing masters in observed topology order, then new masters in EnsureNode order, with slots 0-16383 split as evenly as possible.
- For replica ScaleIn, only remove extra replicas with ForgetNode, then DeleteNode, then VerifyCluster. Never remove a master, migrate slots, or reduce replicasPerShard below 1.
- Every namespace param must equal the RedisCluster name.
- Every new pod referenced by WaitNodeReady/MeetNode/ReplicateNode/AddSlots must be declared by a preceding EnsureNode.
- All Redis pods must be named "redis-<N>" where <N> is a single non-negative integer. Do NOT embed the cluster name or any other prefix. Correct examples: redis-0, redis-1, redis-2. Wrong examples: redis-3s1r-0, redis-cluster-0, redis-example-0. Pod names are globally non-reusable. Create uses redis-0 upward; all later new pods must start at the provided nextPodOrdinal and must not fill historical gaps.
- sourcePod and targetPod (or masterPod and replicaPod) must not be the same pod.`)
}

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
	writeObservedNodesTable(&b, req.ObservedState.Nodes)
	b.WriteString("\n")

	b.WriteString("## Task\n")
	b.WriteString("Bring the cluster from the observed state to the desired spec. Pick whichever whitelisted action sequence you think is safest; the controller's Validator is the final safety net.\n")
	b.WriteString("Return only the JSON plan.\n")
	return b.String()
}

func writeObservedNodesTable(b *strings.Builder, nodes []ObservedNode) {
	if len(nodes) == 0 {
		b.WriteString("No observed nodes.\n")
		return
	}
	b.WriteString("pod | podExists | redisSeen | nodeId | role | slots | masterId | masterPod | ready | deleting | flags | linkState\n")
	for _, n := range nodes {
		fmt.Fprintf(b, "%s | %v | %v | %s | %s | %s | %s | %s | %v | %v | %s | %s\n",
			dashIfEmpty(n.Pod), n.PodExists, n.RedisSeen, dashIfEmpty(n.NodeID), dashIfEmpty(n.Role), dashIfEmpty(n.Slots),
			dashIfEmpty(n.MasterID), dashIfEmpty(n.MasterPod), n.Ready, n.Deleting, dashIfEmpty(strings.Join(n.Flags, ",")), dashIfEmpty(n.LinkState))
	}
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
