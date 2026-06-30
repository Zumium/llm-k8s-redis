package planner

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const redisSlotCount = rediscluster.SlotRangeBound + 1

type GoPlanner struct {
	Fallback Planner
}

func NewGoPlanner(fallback Planner) *GoPlanner {
	return &GoPlanner{Fallback: fallback}
}

func (p *GoPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	if generated := buildGoPlan(req); generated != nil {
		if err := plan.NewValidator().Validate(generated, plan.ValidationContext{
			Spec:           req.Spec,
			NextPodOrdinal: req.ObservedState.NextPodOrdinal,
			ObservedNodes:  req.ObservedState.Nodes,
		}); err == nil {
			log.FromContext(ctx).Info("planner produced plan", "planner", "go", "planID", generated.PlanID, "steps", len(generated.Steps), "targetGeneration", generated.TargetGeneration)
			return generated, nil
		} else {
			log.FromContext(ctx).Info("deterministic go planner plan failed validation", "planID", generated.PlanID, "error", err)
		}
	}
	if p.Fallback == nil {
		return nil, ErrNotConfigured
	}
	log.FromContext(ctx).Info("planner delegating to fallback", "planner", "go", "fallback", true)
	generated, err := p.Fallback.Plan(ctx, req)
	if err != nil || generated == nil {
		return generated, err
	}
	log.FromContext(ctx).Info("planner produced plan", "planner", "fallback", "planID", generated.PlanID, "steps", len(generated.Steps), "targetGeneration", generated.TargetGeneration)
	return generated, nil
}

func buildGoPlan(req Request) *plan.Plan {
	if req.Spec.ReplicasPerShard < 1 {
		return nil
	}
	t := topologyFromObserved(req.ObservedState.Nodes)
	if len(t.masters) == 0 {
		return buildCreatePlan(req)
	}
	if p := buildRepairPlan(req, t); p != nil {
		return p
	}
	if p := buildReplicaScaleInPlan(req, t); p != nil {
		return p
	}
	if p := buildShardScaleOutPlan(req, t); p != nil {
		return p
	}
	if p := buildShardScaleInPlan(req, t); p != nil {
		return p
	}
	return buildReplicaScaleOutPlan(req, t)
}

type observedTopology struct {
	masters []observedMaster
}

type observedMaster struct {
	pod      string
	nodeID   string
	slots    string
	ready    bool
	replicas []observedReplica
}

type observedReplica struct {
	pod    string
	nodeID string
	ready  bool
}

func topologyFromObserved(nodes []ObservedNode) observedTopology {
	nodeIDToPod := map[string]string{}
	for _, n := range nodes {
		if n.NodeID != "" && n.Pod != "" {
			nodeIDToPod[n.NodeID] = n.Pod
		}
	}
	t := observedTopology{}
	masterIndex := map[string]int{}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "master" || n.Slots == "" {
			continue
		}
		if !plan.ObservedNodeHealthy(n) {
			continue
		}
		masterIndex[n.Pod] = len(t.masters)
		t.masters = append(t.masters, observedMaster{pod: n.Pod, nodeID: n.NodeID, slots: n.Slots, ready: true})
	}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "replica" {
			continue
		}
		masterPod := n.MasterPod
		if masterPod == "" {
			masterPod = nodeIDToPod[n.MasterID]
		}
		if i, ok := masterIndex[masterPod]; ok {
			if plan.ObservedNodeHealthy(n) {
				t.masters[i].replicas = append(t.masters[i].replicas, observedReplica{pod: n.Pod, nodeID: n.NodeID, ready: true})
			}
		}
	}
	return t
}

func buildCreatePlan(req Request) *plan.Plan {
	spec := req.Spec
	total := int(spec.Shards) * (1 + int(spec.ReplicasPerShard))
	if total == 0 {
		return nil
	}
	steps := []plan.Step{}
	pods := redisPods(0, total)
	steps = append(steps, ensureSteps(spec, pods)...)
	steps = append(steps, waitSteps(spec, pods)...)
	steps = append(steps, meetSteps(spec, "redis-0", pods[1:])...)
	for shard := 0; shard < int(spec.Shards); shard++ {
		master := redisPod(shard * (1 + int(spec.ReplicasPerShard)))
		for replica := 1; replica <= int(spec.ReplicasPerShard); replica++ {
			pod := redisPod(shard*(1+int(spec.ReplicasPerShard)) + replica)
			steps = append(steps, replicateStep(spec, master, pod))
		}
	}
	for shard := 0; shard < int(spec.Shards); shard++ {
		master := redisPod(shard * (1 + int(spec.ReplicasPerShard)))
		steps = append(steps, step("add-slots-"+master, plan.ActionAddSlots, map[string]any{"namespace": spec.Name, "pod": master, "slots": slotRange(shard, int(spec.Shards))}))
	}
	return newGoPlan(spec, "go-create", "Create Redis Cluster", append(steps, verifyStep(spec)))
}

func buildRepairPlan(req Request, t observedTopology) *plan.Plan {
	spec := req.Spec
	if len(t.masters) != int(spec.Shards) {
		return nil
	}
	ghosts := ghostNodes(req.ObservedState.Nodes)
	if hasAmbiguousNoSlotNode(req.ObservedState.Nodes) {
		return nil
	}
	if len(ghosts) == 0 && uniformReplicaCount(t) {
		return nil
	}
	next := req.ObservedState.NextPodOrdinal
	steps := []plan.Step{}
	missingReplicas := []replicaProvision{}
	for _, n := range ghosts {
		if n.Pod == "" && n.NodeID == "" {
			return nil
		}
		label := safeIDPart(n.Pod, n.NodeID)
		params := map[string]any{"namespace": spec.Name}
		if n.Pod != "" {
			params["pod"] = n.Pod
		}
		if n.NodeID != "" {
			params["lastKnownNodeId"] = n.NodeID
		}
		steps = append(steps, step("forget-"+label, plan.ActionForgetNode, params))
		if n.Pod != "" && !n.PodExists && !livePodExists(req.ObservedState.Nodes, n.Pod) {
			steps = append(steps, deleteStep(spec, n.Pod))
		}
	}
	for _, m := range t.masters {
		for len(m.replicas) < int(spec.ReplicasPerShard) {
			if !m.ready {
				return nil
			}
			pod := redisPod(next)
			next++
			missingReplicas = append(missingReplicas, replicaProvision{master: m.pod, replica: pod})
			m.replicas = append(m.replicas, observedReplica{pod: pod})
		}
	}
	steps = append(steps, replicaProvisionSteps(spec, missingReplicas)...)
	if len(steps) == 0 {
		return nil
	}
	return newGoPlan(spec, "go-repair", "Repair Redis Cluster topology", append(steps, verifyStep(spec)))
}

func buildReplicaScaleInPlan(req Request, t observedTopology) *plan.Plan {
	spec := req.Spec
	target := int(spec.ReplicasPerShard)
	if len(t.masters) != int(spec.Shards) {
		return nil
	}
	steps := []plan.Step{}
	for _, m := range t.masters {
		if len(m.replicas) < target {
			return nil
		}
		if len(m.replicas) == target {
			continue
		}
		replicas := append([]observedReplica{}, m.replicas...)
		sort.Slice(replicas, func(i, j int) bool {
			return redisPodOrdinalValue(replicas[i].pod) > redisPodOrdinalValue(replicas[j].pod)
		})
		for _, r := range replicas[:len(replicas)-target] {
			if r.pod == "" {
				return nil
			}
			steps = append(steps, forgetDeleteSteps(spec, r.pod, r.nodeID)...)
		}
	}
	if len(steps) == 0 {
		return nil
	}
	return newGoPlan(spec, "go-replica-scalein", "Scale Redis Cluster replicas down", append(steps, verifyStep(spec)))
}

func buildReplicaScaleOutPlan(req Request, t observedTopology) *plan.Plan {
	spec := req.Spec
	if len(t.masters) != int(spec.Shards) {
		return nil
	}
	current := -1
	for _, m := range t.masters {
		if !m.ready {
			return nil
		}
		if current == -1 {
			current = len(m.replicas)
		}
		if len(m.replicas) != current {
			return nil
		}
	}
	if current < 0 || int(spec.ReplicasPerShard) <= current {
		return nil
	}
	next := req.ObservedState.NextPodOrdinal
	missingReplicas := []replicaProvision{}
	for _, m := range t.masters {
		for i := current; i < int(spec.ReplicasPerShard); i++ {
			pod := redisPod(next)
			next++
			missingReplicas = append(missingReplicas, replicaProvision{master: m.pod, replica: pod})
		}
	}
	return newGoPlan(spec, "go-replica-scaleout", "Scale Redis Cluster replicas", append(replicaProvisionSteps(spec, missingReplicas), verifyStep(spec)))
}

func buildShardScaleOutPlan(req Request, t observedTopology) *plan.Plan {
	spec := req.Spec
	currentReplicas, ok := shardScaleReady(t)
	if !ok || int(spec.Shards) <= len(t.masters) || int(spec.ReplicasPerShard) != currentReplicas {
		return nil
	}
	owners, ok := slotOwners(t)
	if !ok {
		return nil
	}
	next := req.ObservedState.NextPodOrdinal
	newPods := []string{}
	newMasters := []string{}
	newReplicas := []replicaProvision{}
	steps := []plan.Step{}
	for i := len(t.masters); i < int(spec.Shards); i++ {
		master := redisPod(next)
		next++
		newPods = append(newPods, master)
		newMasters = append(newMasters, master)
		steps = append(steps, ensureStep(spec, master))
		for r := 0; r < currentReplicas; r++ {
			replica := redisPod(next)
			next++
			newPods = append(newPods, replica)
			newReplicas = append(newReplicas, replicaProvision{master: master, replica: replica})
			steps = append(steps, ensureStep(spec, replica))
		}
	}
	steps = append(steps, waitSteps(spec, newPods)...)
	steps = append(steps, meetSteps(spec, t.masters[0].pod, newMasters)...)
	for _, r := range newReplicas {
		steps = append(steps, meetStep(spec, t.masters[0].pod, r.replica))
	}
	for _, r := range newReplicas {
		steps = append(steps, replicateStep(spec, r.master, r.replica))
	}
	masters := masterPods(t)
	masters = append(masters, newMasters...)
	steps = append(steps, migrationSteps(spec, owners, masters, true)...)
	return newGoPlan(spec, "go-shard-scaleout", "Scale Redis Cluster shards out", append(steps, verifyStep(spec)))
}

func buildShardScaleInPlan(req Request, t observedTopology) *plan.Plan {
	spec := req.Spec
	currentReplicas, ok := shardScaleReady(t)
	if !ok || int(spec.Shards) >= len(t.masters) || int(spec.ReplicasPerShard) != currentReplicas {
		return nil
	}
	owners, ok := slotOwners(t)
	if !ok {
		return nil
	}
	next := req.ObservedState.NextPodOrdinal
	newMasters := []string{}
	newReplicas := []replicaProvision{}
	steps := []plan.Step{}
	total := int(spec.Shards) * (1 + currentReplicas)
	pods := redisPods(next, total)
	for i, pod := range pods {
		steps = append(steps, ensureStep(spec, pod))
		if i%(1+currentReplicas) == 0 {
			newMasters = append(newMasters, pod)
			continue
		}
		newReplicas = append(newReplicas, replicaProvision{master: newMasters[len(newMasters)-1], replica: pod})
	}
	steps = append(steps, waitSteps(spec, pods)...)
	steps = append(steps, meetSteps(spec, t.masters[0].pod, pods)...)
	for _, r := range newReplicas {
		steps = append(steps, replicateStep(spec, r.master, r.replica))
	}
	steps = append(steps, migrationSteps(spec, owners, newMasters, false)...)
	for _, old := range topologyNodes(t) {
		steps = append(steps, forgetDeleteSteps(spec, old.pod, old.nodeID)...)
	}
	return newGoPlan(spec, "go-shard-scalein", "Scale Redis Cluster shards in", append(steps, verifyStep(spec)))
}

type replicaProvision struct {
	master  string
	replica string
}

func replicaProvisionSteps(spec plan.ClusterSpec, replicas []replicaProvision) []plan.Step {
	steps := []plan.Step{}
	pods := replicaPods(replicas)
	steps = append(steps, ensureSteps(spec, pods)...)
	steps = append(steps, waitSteps(spec, pods)...)
	for _, r := range replicas {
		steps = append(steps, meetStep(spec, r.master, r.replica))
	}
	for _, r := range replicas {
		steps = append(steps, replicateStep(spec, r.master, r.replica))
	}
	return steps
}

func newGoPlan(spec plan.ClusterSpec, id, summary string, steps []plan.Step) *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           fmt.Sprintf("%s-%d", id, spec.Generation),
		TargetGeneration: spec.Generation,
		Summary:          summary,
		Steps:            steps,
	}
}

func ensureStep(spec plan.ClusterSpec, pod string) plan.Step {
	return step("ensure-"+pod, plan.ActionEnsureNode, map[string]any{"namespace": spec.Name, "pod": pod, "image": spec.Image, "memorySize": spec.MemorySize})
}

func waitStep(spec plan.ClusterSpec, pod string) plan.Step {
	return step("wait-"+pod, plan.ActionWaitNodeReady, map[string]any{"namespace": spec.Name, "pod": pod})
}

func deleteStep(spec plan.ClusterSpec, pod string) plan.Step {
	return step("delete-"+pod, plan.ActionDeleteNode, map[string]any{"namespace": spec.Name, "pod": pod})
}

func meetStep(spec plan.ClusterSpec, source, target string) plan.Step {
	return step("meet-"+target, plan.ActionMeetNode, map[string]any{"namespace": spec.Name, "sourcePod": source, "targetPod": target})
}

func replicateStep(spec plan.ClusterSpec, master, replica string) plan.Step {
	return step("replicate-"+replica, plan.ActionReplicateNode, map[string]any{"namespace": spec.Name, "masterPod": master, "replicaPod": replica})
}

func verifyStep(spec plan.ClusterSpec) plan.Step {
	return step("verify", plan.ActionVerifyCluster, map[string]any{
		"expectedShards":                   int(spec.Shards),
		"expectedReplicasPerShard":         int(spec.ReplicasPerShard),
		"requireClusterStateOk":            true,
		"requireFullSlotCoverage":          true,
		"requireAllSlotOwnersHaveReplicas": true,
	})
}

func step(id string, action plan.ActionType, params map[string]any) plan.Step {
	return plan.Step{ID: id, Action: action, Params: params}
}

func ensureSteps(spec plan.ClusterSpec, pods []string) []plan.Step {
	steps := make([]plan.Step, 0, len(pods))
	for _, pod := range pods {
		steps = append(steps, ensureStep(spec, pod))
	}
	return steps
}

func waitSteps(spec plan.ClusterSpec, pods []string) []plan.Step {
	steps := make([]plan.Step, 0, len(pods))
	for _, pod := range pods {
		steps = append(steps, waitStep(spec, pod))
	}
	return steps
}

func meetSteps(spec plan.ClusterSpec, source string, pods []string) []plan.Step {
	steps := make([]plan.Step, 0, len(pods))
	for _, pod := range pods {
		steps = append(steps, meetStep(spec, source, pod))
	}
	return steps
}

func forgetDeleteSteps(spec plan.ClusterSpec, pod, nodeID string) []plan.Step {
	params := map[string]any{"namespace": spec.Name, "pod": pod}
	if nodeID != "" {
		params["lastKnownNodeId"] = nodeID
	}
	return []plan.Step{
		step("forget-"+pod, plan.ActionForgetNode, params),
		deleteStep(spec, pod),
	}
}

func slotRange(shard, shards int) string {
	start := shard * redisSlotCount / shards
	end := (shard+1)*redisSlotCount/shards - 1
	return fmt.Sprintf("%d-%d", start, end)
}

func shardScaleReady(t observedTopology) (int, bool) {
	if len(t.masters) == 0 {
		return 0, false
	}
	replicas := len(t.masters[0].replicas)
	for _, m := range t.masters {
		if !m.ready || len(m.replicas) != replicas {
			return 0, false
		}
		for _, r := range m.replicas {
			if !r.ready {
				return 0, false
			}
		}
	}
	return replicas, true
}

func slotOwners(t observedTopology) (map[int]string, bool) {
	out := map[int]string{}
	for _, m := range t.masters {
		for _, part := range strings.Split(m.slots, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			start, end, err := rediscluster.SlotTokenRange(part)
			if err != nil {
				return nil, false
			}
			for i := start; i <= end; i++ {
				if out[i] != "" {
					return nil, false
				}
				out[i] = m.pod
			}
		}
	}
	return out, len(out) == redisSlotCount
}

func masterPods(t observedTopology) []string {
	out := make([]string, 0, len(t.masters))
	for _, m := range t.masters {
		out = append(out, m.pod)
	}
	return out
}

func replicaPods(replicas []replicaProvision) []string {
	out := make([]string, 0, len(replicas))
	for _, r := range replicas {
		out = append(out, r.replica)
	}
	return out
}

type topologyNode struct {
	pod    string
	nodeID string
}

func topologyNodes(t observedTopology) []topologyNode {
	out := []topologyNode{}
	for _, m := range t.masters {
		out = append(out, topologyNode{pod: m.pod, nodeID: m.nodeID})
		for _, r := range m.replicas {
			out = append(out, topologyNode{pod: r.pod, nodeID: r.nodeID})
		}
	}
	return out
}

func migrationSteps(spec plan.ClusterSpec, owners map[int]string, masters []string, skipSame bool) []plan.Step {
	type key struct{ source, target string }
	grouped := map[key][]int{}
	order := []key{}
	for i, target := range masters {
		start, end := balancedSlotRange(i, len(masters))
		for slot := start; slot <= end; slot++ {
			source := owners[slot]
			if skipSame && source == target {
				continue
			}
			k := key{source: source, target: target}
			if len(grouped[k]) == 0 {
				order = append(order, k)
			}
			grouped[k] = append(grouped[k], slot)
		}
	}
	steps := []plan.Step{}
	for _, k := range order {
		steps = append(steps, step("migrate-"+k.source+"-"+k.target, plan.ActionMigrateSlots, map[string]any{
			"namespace": spec.Name, "sourcePod": k.source, "targetPod": k.target, "slots": compactSlots(grouped[k]),
		}))
	}
	return steps
}

func balancedSlotRange(index, total int) (int, int) {
	base := redisSlotCount / total
	rem := redisSlotCount % total
	start := index * base
	if index < rem {
		start += index
	} else {
		start += rem
	}
	size := base
	if index < rem {
		size++
	}
	return start, start + size - 1
}

func compactSlots(slots []int) string {
	sort.Ints(slots)
	parts := []string{}
	for i := 0; i < len(slots); {
		start := slots[i]
		end := start
		for i++; i < len(slots) && slots[i] == end+1; i++ {
			end = slots[i]
		}
		if start == end {
			parts = append(parts, strconv.Itoa(start))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, end))
		}
	}
	return strings.Join(parts, ",")
}

func redisPod(i int) string {
	return fmt.Sprintf("redis-%d", i)
}

func redisPods(start, count int) []string {
	pods := make([]string, 0, count)
	for i := 0; i < count; i++ {
		pods = append(pods, redisPod(start+i))
	}
	return pods
}

func redisPodOrdinal(pod string) (int, bool) {
	raw, ok := strings.CutPrefix(pod, "redis-")
	if !ok || raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	return n, err == nil && n >= 0
}

func redisPodOrdinalValue(pod string) int {
	n, ok := redisPodOrdinal(pod)
	if !ok {
		return -1
	}
	return n
}

func livePodExists(nodes []ObservedNode, pod string) bool {
	for _, n := range nodes {
		if n.Pod == pod && n.PodExists {
			return true
		}
	}
	return false
}

func ghostNodes(nodes []ObservedNode) []ObservedNode {
	var out []ObservedNode
	for _, n := range nodes {
		if plan.ObservedNodeForgettableGhost(n) {
			out = append(out, n)
		}
	}
	return out
}

func hasAmbiguousNoSlotNode(nodes []ObservedNode) bool {
	for _, n := range nodes {
		if n.RedisSeen && n.Slots == "" && !plan.ObservedNodeHealthy(n) && !plan.ObservedNodeForgettableGhost(n) {
			return true
		}
	}
	return false
}

func uniformReplicaCount(t observedTopology) bool {
	if len(t.masters) == 0 {
		return true
	}
	count := len(t.masters[0].replicas)
	for _, m := range t.masters[1:] {
		if len(m.replicas) != count {
			return false
		}
	}
	return true
}

func safeIDPart(values ...string) string {
	for _, v := range values {
		if v != "" {
			return strings.NewReplacer("/", "-", ":", "-").Replace(v)
		}
	}
	return "unknown"
}

var _ Planner = (*GoPlanner)(nil)
