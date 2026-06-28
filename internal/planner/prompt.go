package planner

import (
	"bytes"
	"embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

//go:embed prompts/*.md
var promptFS embed.FS

var (
	systemTmpl = template.Must(template.New("system.md").ParseFS(promptFS, "prompts/system.md"))
	userTmpl   = template.Must(template.New("user.md").ParseFS(promptFS, "prompts/user.md"))
)

func buildSystemPrompt() string {
	var buf bytes.Buffer
	err := systemTmpl.Execute(&buf, map[string]any{
		"DSLVersion": plan.DSLVersion,
		"Actions":    actionReference(),
		"Invariants": safetyInvariants(),
	})
	if err != nil {
		panic(fmt.Sprintf("system prompt template: %v", err))
	}
	return buf.String()
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
	return promptFile("prompts/invariants.md")
}

func workedExamplesForAnalysis(analysisJSON string) string {
	exampleFiles := []struct {
		label string
		file  string
	}{
		{`"repairTopology"`, "prompts/examples_repair_topology.md"},
		{`"cleanupGhostNodes"`, "prompts/examples_cleanup_ghost_nodes.md"},
		{`"cleanupDirtyNodes"`, "prompts/examples_cleanup_dirty_nodes.md"},
		{`"changeClusterSpec"`, "prompts/examples_change_cluster_spec.md"},
	}
	var examples []string
	for _, exampleFile := range exampleFiles {
		if strings.Contains(analysisJSON, exampleFile.label) {
			examples = append(examples, promptFile(exampleFile.file))
		}
	}
	return strings.Join(examples, "\n\n")
}

func promptFile(name string) string {
	data, err := promptFS.ReadFile(name)
	if err != nil {
		panic(fmt.Sprintf("%s not found in embedded FS", name))
	}
	return strings.TrimSpace(string(data))
}

func buildUserPrompt(req Request) string {
	return buildClusterContextPrompt(req) + `
## Task
Bring the cluster from the observed state to the desired spec. Pick whichever whitelisted action sequence you think is safest; the controller's Validator is the final safety net.
Return only the JSON plan.
`
}

func buildAnalysisPrompt(req Request) string {
	return buildClusterContextPrompt(req) + `
## Task
Analyze which subprocesses are needed before writing the plan. Use only these subprocess labels: repairTopology, cleanupGhostNodes, cleanupDirtyNodes, changeClusterSpec.
Return only JSON:
{"subprocesses":["<label>"],"summary":"<one-line reason>"}
`
}

func buildClusterContextPrompt(req Request) string {
	var buf bytes.Buffer
	var tableBuf strings.Builder
	writeObservedNodesTable(&tableBuf, req.ObservedState.Nodes)
	err := userTmpl.Execute(&buf, map[string]any{
		"Name":               req.Spec.Name,
		"Generation":         req.Spec.Generation,
		"Shards":             req.Spec.Shards,
		"ReplicasPerShard":   req.Spec.ReplicasPerShard,
		"Image":              req.Spec.Image,
		"MemorySize":         req.Spec.MemorySize,
		"NextPodOrdinal":     req.ObservedState.NextPodOrdinal,
		"ObservedNodesTable": tableBuf.String(),
	})
	if err != nil {
		panic(fmt.Sprintf("user prompt template: %v", err))
	}
	return buf.String()
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
