package plan

import (
	"fmt"
	"strconv"
	"strings"
)

var allowedActions = map[ActionType]bool{
	ActionEnsureNode:    true,
	ActionWaitNodeReady: true,
	ActionMeetNode:      true,
	ActionReplicateNode: true,
	ActionAddSlots:      true,
	ActionMigrateSlots:  true,
	ActionForgetNode:    true,
	ActionDeleteNode:    true,
	ActionVerifyCluster: true,
}

// Validator enforces the deterministic safety invariants described in
// docs/OPERATIONS.md and docs/ACTIONS.md. It never calls out to the LLM, K8S,
// or Redis: validation must be fully reproducible from the plan and the spec.
type Validator struct{}

// NewValidator returns a Validator.
func NewValidator() *Validator { return &Validator{} }

// Validate runs generic checks that apply to every plan, then validates the
// action shape against the observed state. It accepts ClusterSpec for callers
// without topology and ValidationContext for callers that have topology.
func (v *Validator) Validate(p *Plan, input any) error {
	ctx, err := validationContext(input)
	if err != nil {
		return err
	}
	spec := ctx.Spec
	if p == nil {
		return fmt.Errorf("plan is nil")
	}
	if p.DSLVersion != DSLVersion {
		return fmt.Errorf("dslVersion %q is not supported (expected %q)", p.DSLVersion, DSLVersion)
	}
	if p.TargetGeneration != spec.Generation {
		return fmt.Errorf("targetGeneration %d does not match current generation %d", p.TargetGeneration, spec.Generation)
	}
	if len(p.Steps) == 0 {
		return fmt.Errorf("plan has no steps")
	}
	seen := map[string]bool{}
	ensurePods := map[string]bool{}
	for i, s := range p.Steps {
		if !allowedActions[s.Action] {
			return fmt.Errorf("step %d: action %q is not in the whitelist", i, s.Action)
		}
		if s.ID == "" {
			return fmt.Errorf("step %d: id is empty", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("step %d: duplicate step id %q", i, s.ID)
		}
		seen[s.ID] = true
		if err := validateStepSchema(s); err != nil {
			return fmt.Errorf("step %q: %w", s.ID, err)
		}
		if err := validateNamespace(s, spec); err != nil {
			return fmt.Errorf("step %q: %w", s.ID, err)
		}
		if s.Action == ActionEnsureNode {
			pod, ok := paramString(s.Params, "pod")
			if !ok || pod == "" {
				return fmt.Errorf("step %q: EnsureNode requires a non-empty pod param", s.ID)
			}
			ensurePods[pod] = true
		}
	}

	last := p.Steps[len(p.Steps)-1]
	if last.Action != ActionVerifyCluster {
		return fmt.Errorf("last step must be VerifyCluster, got %q", last.Action)
	}
	topology := validationTopology(ctx)
	ctx.Topology = topology
	if topology == nil || len(topology.Shards) == 0 {
		if err := validateCreate(p, spec, ensurePods); err != nil {
			return err
		}
		return simulatePlan(p, ctx)
	}
	if err := validateExistingTopologyPlan(p, spec, &ctx, ensurePods); err != nil {
		return err
	}
	return simulatePlan(p, ctx)
}

func validationContext(input any) (ValidationContext, error) {
	switch v := input.(type) {
	case ClusterSpec:
		return ValidationContext{Spec: v}, nil
	case ValidationContext:
		return v, nil
	default:
		return ValidationContext{}, fmt.Errorf("unsupported validation context %T", input)
	}
}

func validationTopology(ctx ValidationContext) *ClusterTopology {
	if len(ctx.ObservedNodes) == 0 {
		return ctx.Topology
	}
	return topologyFromObservedNodes(ctx.ObservedNodes)
}

func topologyFromObservedNodes(nodes []ObservedNode) *ClusterTopology {
	nodeIDToPod := map[string]string{}
	for _, n := range nodes {
		if n.NodeID != "" && n.Pod != "" {
			nodeIDToPod[n.NodeID] = n.Pod
		}
	}
	masters := []ShardTopology{}
	masterIndex := map[string]int{}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "master" || n.Slots == "" {
			continue
		}
		masters = append(masters, ShardTopology{
			ID:     fmt.Sprintf("shard-%d", len(masters)),
			Master: NodeTopology{Pod: n.Pod, NodeID: n.NodeID, Slots: n.Slots, Ready: n.Ready && !n.Deleting},
		})
		masterIndex[n.Pod] = len(masters) - 1
	}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "replica" {
			continue
		}
		masterPod := n.MasterPod
		if masterPod == "" && n.MasterID != "" {
			masterPod = nodeIDToPod[n.MasterID]
		}
		i, ok := masterIndex[masterPod]
		if !ok {
			continue
		}
		masters[i].Replicas = append(masters[i].Replicas, NodeTopology{Pod: n.Pod, NodeID: n.NodeID, Ready: n.Ready && !n.Deleting})
	}
	return &ClusterTopology{Shards: masters}
}

// validateNamespace checks that every pod-targeting action carries a namespace
// param equal to the cluster name. VerifyCluster carries no namespace and is
// skipped.
func validateNamespace(s Step, spec ClusterSpec) error {
	if s.Action == ActionVerifyCluster {
		return nil
	}
	ns, ok := paramString(s.Params, "namespace")
	if !ok {
		return fmt.Errorf("missing namespace param")
	}
	if ns != spec.Name {
		return fmt.Errorf("namespace %q must equal cluster name %q", ns, spec.Name)
	}
	return nil
}

func validateCreate(p *Plan, spec ClusterSpec, ensurePods map[string]bool) error {
	// EnsureNode count must equal shards * (1 + replicasPerShard).
	want := int(spec.Shards) * (1 + int(spec.ReplicasPerShard))
	if len(ensurePods) != want {
		return fmt.Errorf("EnsureNode declared %d distinct pods, expected shards*(1+replicasPerShard)=%d", len(ensurePods), want)
	}
	if err := validateCreatePodNames(ensurePods); err != nil {
		return err
	}

	// AddSlots must cover 0-16383 exactly once, and target a declared node.
	covered := map[int]struct{}{}
	for _, s := range p.Steps {
		if s.Action != ActionAddSlots {
			continue
		}
		pod, _ := paramString(s.Params, "pod")
		if !ensurePods[pod] {
			return fmt.Errorf("step %q: AddSlots target pod %q was not declared by any EnsureNode", s.ID, pod)
		}
		slotsStr, ok := paramString(s.Params, "slots")
		if !ok {
			return fmt.Errorf("step %q: AddSlots missing slots param", s.ID)
		}
		set, err := parseSlots(slotsStr)
		if err != nil {
			return fmt.Errorf("step %q: %w", s.ID, err)
		}
		for slot := range set {
			if _, exists := covered[slot]; exists {
				return fmt.Errorf("step %q: slot %d overlaps a previous AddSlots", s.ID, slot)
			}
			covered[slot] = struct{}{}
		}
	}
	if len(covered) != 16384 {
		return fmt.Errorf("AddSlots must cover all 16384 slots, covered %d", len(covered))
	}

	return nil
}

func validateStepSchema(s Step) error {
	switch s.Action {
	case ActionEnsureNode:
		if err := requireStringParams(s.Params, "namespace", "image", "memorySize"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case ActionWaitNodeReady:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case ActionMeetNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "sourcePod", "targetPod")
	case ActionReplicateNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "masterPod", "replicaPod")
	case ActionAddSlots:
		if err := requireStringParams(s.Params, "namespace", "slots"); err != nil {
			return err
		}
		if err := requireSlotsParam(s.Params, "slots"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case ActionMigrateSlots:
		if err := requireStringParams(s.Params, "namespace", "slots"); err != nil {
			return err
		}
		if err := requireSlotsParam(s.Params, "slots"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "sourcePod", "targetPod")
	case ActionForgetNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		if err := requirePodParams(s.Params, "pod"); err != nil {
			return err
		}
		if _, ok := s.Params["lastKnownNodeId"]; ok {
			return requireStringParams(s.Params, "lastKnownNodeId")
		}
		return nil
	case ActionDeleteNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case ActionVerifyCluster:
		if err := requireIntParams(s.Params, "expectedShards", "expectedReplicasPerShard"); err != nil {
			return err
		}
		return requireTrueBoolParams(s.Params, "requireClusterStateOk", "requireFullSlotCoverage", "requireAllSlotOwnersHaveReplicas")
	default:
		return nil
	}
}

func requireStringParams(params map[string]any, keys ...string) error {
	for _, key := range keys {
		value, ok := paramString(params, key)
		if !ok {
			return fmt.Errorf("%s must be a string", key)
		}
		if value == "" {
			return fmt.Errorf("%s must be non-empty", key)
		}
	}
	return nil
}

func requirePodParams(params map[string]any, keys ...string) error {
	if err := requireStringParams(params, keys...); err != nil {
		return err
	}
	for _, key := range keys {
		pod, _ := paramString(params, key)
		if _, ok := redisPodOrdinal(pod); !ok {
			return fmt.Errorf("%s %q does not match required naming redis-<N>", key, pod)
		}
	}
	return nil
}

func requireIntParams(params map[string]any, keys ...string) error {
	for _, key := range keys {
		if _, ok := paramInt(params, key); !ok {
			return fmt.Errorf("%s must be an integer", key)
		}
	}
	return nil
}

func requireSlotsParam(params map[string]any, key string) error {
	slots, _ := paramString(params, key)
	if _, err := parseSlots(slots); err != nil {
		return fmt.Errorf("%s is invalid: %w", key, err)
	}
	return nil
}

func requireTrueBoolParams(params map[string]any, keys ...string) error {
	for _, key := range keys {
		if params == nil {
			return fmt.Errorf("%s must be true", key)
		}
		value, ok := params[key]
		if !ok {
			return fmt.Errorf("%s must be true", key)
		}
		b, ok := value.(bool)
		if !ok {
			return fmt.Errorf("%s must be a bool", key)
		}
		if !b {
			return fmt.Errorf("%s must be true", key)
		}
	}
	return nil
}

func validateCreatePodNames(ensurePods map[string]bool) error {
	want := len(ensurePods)
	if want == 0 {
		return nil
	}
	ordinals := make(map[int]bool, want)
	max := -1
	for pod := range ensurePods {
		n, ok := redisPodOrdinal(pod)
		if !ok {
			return fmt.Errorf("pod %q does not match required naming redis-<N>", pod)
		}
		ordinals[n] = true
		if n > max {
			max = n
		}
	}
	if max != want-1 {
		return fmt.Errorf("EnsureNode pods must be contiguous from redis-0 to redis-%d", want-1)
	}
	if len(ordinals) != want {
		return fmt.Errorf("EnsureNode pods have duplicate ordinals")
	}
	return nil
}

func validateExistingTopologyPlan(p *Plan, spec ClusterSpec, ctx *ValidationContext, ensurePods map[string]bool) error {
	topology := ctx.Topology
	currentShards := len(topology.Shards)
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		// Non-uniform topology is only allowed on the heal path: shards count
		// matches spec, every shard has a ready slot-owning master, slot
		// coverage is complete, and the cluster is in a recoverable state.
		if h := healableTopology(topology, spec, ctx.ObservedNodes); h != nil {
			return validateHealRepair(p, spec, ctx, ensurePods, h)
		}
		return err
	}
	if int(spec.Shards) == currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return validateReplicaScaleOut(p, spec, *ctx, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateShardScaleOut(p, spec, *ctx, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return fmt.Errorf("shards and replicasPerShard cannot both change in one scaleout")
	}
	if int(spec.Shards) == currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateSameSpecRepair(*ctx, ensurePods)
	}
	return fmt.Errorf("unsupported existing-topology change: spec.shards=%d current=%d spec.replicasPerShard=%d current=%d", spec.Shards, currentShards, spec.ReplicasPerShard, currentReplicas)
}

// healableTopology inspects a non-uniform topology and decides whether the
// cluster is in a state the heal path can repair. Returns nil when the state
// is not healable (caller should surface the original uniformReplicaCount
// error).
//
// Healable criteria (failover recoverable only):
//   - shard count equals spec
//   - every shard has a ready, slot-owning master
//   - slots 0-16383 are covered exactly once across masters
//   - at least one shard is under-replicated (otherwise it is not a heal case)
//   - observed nodes reveal at least one ghost (a Redis-seen, slotless node
//     whose pod is gone or flagged fail). A heal plan must clean these up.
func healableTopology(topology *ClusterTopology, spec ClusterSpec, observed []ObservedNode) *healState {
	if topology == nil || int(spec.Shards) != len(topology.Shards) {
		return nil
	}
	covered := map[int]string{}
	for _, sh := range topology.Shards {
		if sh.Master.Pod == "" || !sh.Master.Ready || sh.Master.Slots == "" {
			return nil
		}
		slots, err := parseSlots(sh.Master.Slots)
		if err != nil {
			return nil
		}
		for slot := range slots {
			if _, ok := covered[slot]; ok {
				return nil
			}
			covered[slot] = sh.Master.Pod
		}
	}
	if len(covered) != 16384 {
		return nil
	}

	underReplicated := []ShardTopology{}
	for _, sh := range topology.Shards {
		if len(sh.Replicas) < int(spec.ReplicasPerShard) {
			underReplicated = append(underReplicated, sh)
		}
	}
	if len(underReplicated) == 0 {
		return nil
	}

	ghosts := ghostNodes(observed)
	if len(ghosts) == 0 {
		return nil
	}

	return &healState{
		underReplicated: underReplicated,
		ghosts:          ghosts,
	}
}

// ghostNodes returns observed nodes that Redis still remembers but that hold no
// slots and whose pod is gone or Redis has flagged failed. These are leftovers
// from a master that died and whose replica was auto-promoted by failover.
func ghostNodes(observed []ObservedNode) []ObservedNode {
	var out []ObservedNode
	for _, n := range observed {
		if !n.RedisSeen {
			continue
		}
		if n.Slots != "" {
			continue
		}
		failed := !n.PodExists || hasFailFlag(n.Flags)
		if !failed {
			continue
		}
		out = append(out, n)
	}
	return out
}

func hasFailFlag(flags []string) bool {
	for _, f := range flags {
		if f == "fail" || f == "fail?" {
			return true
		}
	}
	return false
}

// healState summarises what a heal plan must fix.
type healState struct {
	underReplicated []ShardTopology
	ghosts          []ObservedNode
}

// validateHealRepair checks a plan that repairs a failover-damaged cluster:
// it replenishes under-replicated shards with new replicas and forgets/deletes
// ghost nodes left by the dead master.
//
// Allowed actions: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode,
// ForgetNode, DeleteNode, VerifyCluster. AddSlots/MigrateSlots are forbidden
// because slots are already fully covered and owned by healthy masters.
func validateHealRepair(p *Plan, spec ClusterSpec, ctx *ValidationContext, ensurePods map[string]bool, h *healState) error {
	// Heal mode relaxes the "slot-owning master has replica" intermediate
	// invariant: the cluster starts violating it and the plan restores it.
	ctx.HealMode = true
	// Action whitelist.
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionForgetNode, ActionDeleteNode, ActionVerifyCluster:
		default:
			return fmt.Errorf("step %q: action %q is not allowed for heal repair", s.ID, s.Action)
		}
	}

	// New pods must be contiguous from nextPodOrdinal.
	existingPods := topologyPods(ctx.Topology)
	newPods := map[string]bool{}
	for pod := range ensurePods {
		if !existingPods[pod] {
			newPods[pod] = true
		}
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(*ctx)); err != nil {
		return err
	}

	// Verify every under-replicated shard gets one new replica per missing slot.
	replicasByMaster := map[string]int{}
	replicaTargets := map[string]bool{}
	masterSet := map[string]bool{}
	for _, sh := range ctx.Topology.Shards {
		masterSet[sh.Master.Pod] = true
	}
	for i, s := range p.Steps {
		if s.Action != ActionReplicateNode {
			continue
		}
		masterPod, ok := paramString(s.Params, "masterPod")
		if !ok || !masterSet[masterPod] {
			return fmt.Errorf("step %q: masterPod %q is not an existing master", s.ID, masterPod)
		}
		replicaPod, ok := paramString(s.Params, "replicaPod")
		if !ok || !newPods[replicaPod] {
			return fmt.Errorf("step %q: replicaPod %q is not a new pod", s.ID, replicaPod)
		}
		if replicaTargets[replicaPod] {
			return fmt.Errorf("step %q: replica pod %q is assigned more than once", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionEnsureNode, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding EnsureNode", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionWaitNodeReady, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding WaitNodeReady", s.ID, replicaPod)
		}
		replicaTargets[replicaPod] = true
		replicasByMaster[masterPod]++
	}
	needByMaster := map[string]int{}
	wantNewReplicas := 0
	for _, sh := range h.underReplicated {
		need := int(spec.ReplicasPerShard) - len(sh.Replicas)
		needByMaster[sh.Master.Pod] = need
		wantNewReplicas += need
	}
	if len(replicaTargets) != wantNewReplicas {
		return fmt.Errorf("heal plan assigns %d new replicas, expected %d", len(replicaTargets), wantNewReplicas)
	}
	for master, need := range needByMaster {
		if replicasByMaster[master] != need {
			return fmt.Errorf("master %q gets %d new replicas, expected %d", master, replicasByMaster[master], need)
		}
	}

	// Verify every ghost node is forgotten. ForgetNode must carry
	// lastKnownNodeId when the ghost pod no longer exists.
	ghostSeen := map[string]bool{}
	for _, g := range h.ghosts {
		ghostSeen[g.NodeID] = false
	}
	for _, s := range p.Steps {
		if s.Action != ActionForgetNode {
			continue
		}
		var id string
		if v, ok := s.Params["lastKnownNodeId"]; ok {
			if str, ok := v.(string); ok {
				id = str
			}
		}
		if id == "" {
			// Executor can resolve id from pod when the pod still exists; for
			// ghosts whose pod is gone we must require lastKnownNodeId.
			pod, _ := paramString(s.Params, "pod")
			podStillExists := false
			for _, g := range h.ghosts {
				if g.Pod == pod && g.PodExists {
					podStillExists = true
					break
				}
			}
			if !podStillExists {
				return fmt.Errorf("step %q: ForgetNode requires lastKnownNodeId when pod %q no longer exists", s.ID, pod)
			}
			continue
		}
		if _, ok := ghostSeen[id]; !ok {
			return fmt.Errorf("step %q: lastKnownNodeId %q is not a known ghost node", s.ID, id)
		}
		ghostSeen[id] = true
	}
	for id, seen := range ghostSeen {
		if !seen {
			return fmt.Errorf("heal plan must forget ghost node %s", id)
		}
	}

	// Ensure the plan only forgets ghosts it declares (no random ForgetNode).
	// A ForgetNode matches a ghost either by pod name or by lastKnownNodeId,
	// because a topology refresh may drop the dead pod's name->nodeId mapping.
	ghostPods := map[string]bool{}
	ghostIDs := map[string]bool{}
	for _, g := range h.ghosts {
		if g.Pod != "" {
			ghostPods[g.Pod] = true
		}
		if g.NodeID != "" {
			ghostIDs[g.NodeID] = true
		}
	}
	for _, s := range p.Steps {
		if s.Action != ActionForgetNode {
			continue
		}
		pod, _ := paramString(s.Params, "pod")
		id, _ := paramString(s.Params, "lastKnownNodeId")
		if pod != "" && ghostPods[pod] {
			continue
		}
		if id != "" && ghostIDs[id] {
			continue
		}
		return fmt.Errorf("step %q: ForgetNode target pod %q / nodeId %q is not a known ghost", s.ID, pod, id)
	}

	return simulatePlan(p, *ctx)
}

func validateSameSpecRepair(ctx ValidationContext, ensurePods map[string]bool) error {
	existingPods := topologyPods(ctx.Topology)
	newPods := map[string]bool{}
	for pod := range ensurePods {
		if !existingPods[pod] {
			newPods[pod] = true
		}
	}
	return validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx))
}

func validateReplicaScaleOut(p *Plan, spec ClusterSpec, ctx ValidationContext, ensurePods map[string]bool) error {
	topology := ctx.Topology
	currentShards := len(topology.Shards)
	if int(spec.Shards) != currentShards {
		return fmt.Errorf("only replica scaleout is supported for existing topology: spec.shards=%d current masters=%d", spec.Shards, currentShards)
	}
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.ReplicasPerShard) <= currentReplicas {
		return fmt.Errorf("replicasPerShard must increase for replica scaleout: spec=%d current=%d", spec.ReplicasPerShard, currentReplicas)
	}

	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionVerifyCluster:
		default:
			return fmt.Errorf("step %q: action %q is not allowed for replica scaleout", s.ID, s.Action)
		}
	}
	existingPods := topologyPods(topology)
	newPods := map[string]bool{}
	for pod := range ensurePods {
		if !existingPods[pod] {
			newPods[pod] = true
		}
	}
	wantNewPods := currentShards * (int(spec.ReplicasPerShard) - currentReplicas)
	if len(newPods) != wantNewPods {
		return fmt.Errorf("EnsureNode declared %d new pods, expected %d new replicas", len(newPods), wantNewPods)
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx)); err != nil {
		return err
	}

	replicasByMaster := map[string]int{}
	replicaTargets := map[string]bool{}
	for i, s := range p.Steps {
		if s.Action != ActionReplicateNode {
			continue
		}
		masterPod, ok := paramString(s.Params, "masterPod")
		if !ok || !masterPodInTopology(topology, masterPod) {
			return fmt.Errorf("step %q: masterPod %q is not an existing master", s.ID, masterPod)
		}
		replicaPod, ok := paramString(s.Params, "replicaPod")
		if !ok || !newPods[replicaPod] {
			return fmt.Errorf("step %q: replicaPod %q is not a new replica pod", s.ID, replicaPod)
		}
		if replicaTargets[replicaPod] {
			return fmt.Errorf("step %q: replica pod %q is assigned more than once", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionEnsureNode, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding EnsureNode", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionWaitNodeReady, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding WaitNodeReady", s.ID, replicaPod)
		}
		replicaTargets[replicaPod] = true
		replicasByMaster[masterPod]++
	}
	for _, sh := range topology.Shards {
		need := int(spec.ReplicasPerShard) - currentReplicas
		if replicasByMaster[sh.Master.Pod] != need {
			return fmt.Errorf("master %q gets %d new replicas, expected %d", sh.Master.Pod, replicasByMaster[sh.Master.Pod], need)
		}
	}
	if len(replicaTargets) != wantNewPods {
		return fmt.Errorf("ReplicateNode assigned %d new replicas, expected %d", len(replicaTargets), wantNewPods)
	}
	return nil
}

func validateShardScaleOut(p *Plan, spec ClusterSpec, ctx ValidationContext, ensurePods map[string]bool) error {
	topology := ctx.Topology
	currentShards := len(topology.Shards)
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.Shards) <= currentShards {
		return fmt.Errorf("shards must increase for shard scaleout: spec=%d current=%d", spec.Shards, currentShards)
	}
	if int(spec.ReplicasPerShard) != currentReplicas {
		return fmt.Errorf("replicasPerShard must remain %d for shard scaleout, got %d", currentReplicas, spec.ReplicasPerShard)
	}
	if err := validateTopologyReadyAndCovered(topology); err != nil {
		return err
	}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionMigrateSlots, ActionVerifyCluster:
		default:
			return fmt.Errorf("step %q: action %q is not allowed for shard scaleout", s.ID, s.Action)
		}
	}
	existingPods := topologyPods(topology)
	newPods := map[string]bool{}
	ensureOrder := []string{}
	for _, s := range p.Steps {
		if s.Action != ActionEnsureNode {
			continue
		}
		pod, _ := paramString(s.Params, "pod")
		if !existingPods[pod] {
			if !newPods[pod] {
				ensureOrder = append(ensureOrder, pod)
			}
			newPods[pod] = true
		}
	}
	wantNewPods := (int(spec.Shards) - currentShards) * (1 + int(spec.ReplicasPerShard))
	if len(newPods) != wantNewPods {
		return fmt.Errorf("EnsureNode declared %d new pods, expected %d new shard nodes", len(newPods), wantNewPods)
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx)); err != nil {
		return err
	}
	newMasters, err := shardScaleOutMasters(p, newPods, ensureOrder, int(spec.Shards)-currentShards)
	if err != nil {
		return err
	}
	if err := validateShardScaleOutReplicas(p, spec, newPods, newMasters); err != nil {
		return err
	}
	expected, err := expectedShardScaleOutMigrations(topology, newMasters)
	if err != nil {
		return err
	}
	actual, err := actualMigrations(p, topology, newMasters)
	if err != nil {
		return err
	}
	if err := compareMigrationSets(expected, actual); err != nil {
		return err
	}
	return nil
}

type migrationKey struct {
	source string
	target string
}

func validateTopologyReadyAndCovered(topology *ClusterTopology) error {
	covered := map[int]string{}
	for _, sh := range topology.Shards {
		if sh.Master.Pod == "" || !sh.Master.Ready {
			return fmt.Errorf("topology master %q is not ready", sh.Master.Pod)
		}
		if len(sh.Replicas) == 0 {
			return fmt.Errorf("topology master %q has no replicas", sh.Master.Pod)
		}
		for _, r := range sh.Replicas {
			if r.Pod == "" || !r.Ready {
				return fmt.Errorf("topology replica %q is not ready", r.Pod)
			}
		}
		slots, err := parseSlots(sh.Master.Slots)
		if err != nil {
			return fmt.Errorf("topology master %q has invalid slots: %w", sh.Master.Pod, err)
		}
		for slot := range slots {
			if prev, ok := covered[slot]; ok {
				return fmt.Errorf("slot %d appears on both %q and %q", slot, prev, sh.Master.Pod)
			}
			covered[slot] = sh.Master.Pod
		}
	}
	if len(covered) != 16384 {
		return fmt.Errorf("topology slot coverage is %d, expected 16384", len(covered))
	}
	return nil
}

func shardScaleOutMasters(p *Plan, newPods map[string]bool, ensureOrder []string, want int) ([]string, error) {
	masters := map[string]bool{}
	for i, s := range p.Steps {
		if s.Action != ActionReplicateNode {
			continue
		}
		master, ok := paramString(s.Params, "masterPod")
		if !ok || !newPods[master] {
			continue
		}
		if !precededAction(p, i, ActionEnsureNode, "pod", master) {
			return nil, fmt.Errorf("step %q: new master pod %q missing preceding EnsureNode", s.ID, master)
		}
		if !precededAction(p, i, ActionWaitNodeReady, "pod", master) {
			return nil, fmt.Errorf("step %q: new master pod %q missing preceding WaitNodeReady", s.ID, master)
		}
		masters[master] = true
	}
	if len(masters) != want {
		return nil, fmt.Errorf("ReplicateNode identifies %d new masters, expected %d", len(masters), want)
	}
	out := make([]string, 0, len(masters))
	for _, pod := range ensureOrder {
		if masters[pod] {
			out = append(out, pod)
		}
	}
	return out, nil
}

func validateShardScaleOutReplicas(p *Plan, spec ClusterSpec, newPods map[string]bool, newMasters []string) error {
	masterSet := map[string]bool{}
	for _, pod := range newMasters {
		masterSet[pod] = true
	}
	replicaTargets := map[string]bool{}
	replicasByMaster := map[string]int{}
	for i, s := range p.Steps {
		if s.Action != ActionReplicateNode {
			continue
		}
		masterPod, ok := paramString(s.Params, "masterPod")
		if !ok || !masterSet[masterPod] {
			return fmt.Errorf("step %q: masterPod %q is not a new shard master", s.ID, masterPod)
		}
		replicaPod, ok := paramString(s.Params, "replicaPod")
		if !ok || !newPods[replicaPod] || masterSet[replicaPod] {
			return fmt.Errorf("step %q: replicaPod %q is not a new replica pod", s.ID, replicaPod)
		}
		if replicaTargets[replicaPod] {
			return fmt.Errorf("step %q: replica pod %q is assigned more than once", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionEnsureNode, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding EnsureNode", s.ID, replicaPod)
		}
		if !precededAction(p, i, ActionWaitNodeReady, "pod", replicaPod) {
			return fmt.Errorf("step %q: replica pod %q missing preceding WaitNodeReady", s.ID, replicaPod)
		}
		replicaTargets[replicaPod] = true
		replicasByMaster[masterPod]++
	}
	for _, master := range newMasters {
		if replicasByMaster[master] != int(spec.ReplicasPerShard) {
			return fmt.Errorf("new master %q gets %d replicas, expected %d", master, replicasByMaster[master], spec.ReplicasPerShard)
		}
	}
	wantReplicas := len(newMasters) * int(spec.ReplicasPerShard)
	if len(replicaTargets) != wantReplicas {
		return fmt.Errorf("ReplicateNode assigned %d new replicas, expected %d", len(replicaTargets), wantReplicas)
	}
	return nil
}

func expectedShardScaleOutMigrations(topology *ClusterTopology, newMasters []string) (map[migrationKey]map[int]struct{}, error) {
	masters := make([]string, 0, len(topology.Shards)+len(newMasters))
	currentOwner := map[int]string{}
	for _, sh := range topology.Shards {
		masters = append(masters, sh.Master.Pod)
		slots, err := parseSlots(sh.Master.Slots)
		if err != nil {
			return nil, err
		}
		for slot := range slots {
			currentOwner[slot] = sh.Master.Pod
		}
	}
	masters = append(masters, newMasters...)
	out := map[migrationKey]map[int]struct{}{}
	for i, master := range masters {
		start, end := balancedSlotRange(i, len(masters))
		for slot := start; slot <= end; slot++ {
			source := currentOwner[slot]
			if source == "" {
				return nil, fmt.Errorf("slot %d has no current owner", slot)
			}
			if source == master {
				continue
			}
			key := migrationKey{source: source, target: master}
			if out[key] == nil {
				out[key] = map[int]struct{}{}
			}
			out[key][slot] = struct{}{}
		}
	}
	return out, nil
}

func actualMigrations(p *Plan, topology *ClusterTopology, newMasters []string) (map[migrationKey]map[int]struct{}, error) {
	existingMasters := map[string]bool{}
	for _, sh := range topology.Shards {
		existingMasters[sh.Master.Pod] = true
	}
	targetMasters := map[string]bool{}
	for pod := range existingMasters {
		targetMasters[pod] = true
	}
	for _, pod := range newMasters {
		targetMasters[pod] = true
	}
	out := map[migrationKey]map[int]struct{}{}
	for _, s := range p.Steps {
		if s.Action != ActionMigrateSlots {
			continue
		}
		source, ok := paramString(s.Params, "sourcePod")
		if !ok || !existingMasters[source] {
			return nil, fmt.Errorf("step %q: sourcePod %q is not an existing master", s.ID, source)
		}
		target, ok := paramString(s.Params, "targetPod")
		if !ok || !targetMasters[target] {
			return nil, fmt.Errorf("step %q: targetPod %q is not a final master", s.ID, target)
		}
		if source == target {
			return nil, fmt.Errorf("step %q: sourcePod and targetPod must differ", s.ID)
		}
		slotsStr, ok := paramString(s.Params, "slots")
		if !ok || slotsStr == "" {
			return nil, fmt.Errorf("step %q: MigrateSlots missing slots param", s.ID)
		}
		slots, err := parseSlots(slotsStr)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", s.ID, err)
		}
		key := migrationKey{source: source, target: target}
		if out[key] == nil {
			out[key] = map[int]struct{}{}
		}
		for slot := range slots {
			if _, exists := out[key][slot]; exists {
				return nil, fmt.Errorf("step %q: slot %d is migrated more than once for %s->%s", s.ID, slot, source, target)
			}
			out[key][slot] = struct{}{}
		}
	}
	return out, nil
}

func compareMigrationSets(expected, actual map[migrationKey]map[int]struct{}) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("MigrateSlots has %d source/target pairs, expected %d", len(actual), len(expected))
	}
	for key, want := range expected {
		got, ok := actual[key]
		if !ok {
			return fmt.Errorf("missing MigrateSlots for %s->%s", key.source, key.target)
		}
		if len(got) != len(want) {
			return fmt.Errorf("MigrateSlots %s->%s covers %d slots, expected %d", key.source, key.target, len(got), len(want))
		}
		for slot := range want {
			if _, ok := got[slot]; !ok {
				return fmt.Errorf("MigrateSlots %s->%s missing slot %d", key.source, key.target, slot)
			}
		}
	}
	return nil
}

func balancedSlotRange(index, total int) (int, int) {
	base := 16384 / total
	rem := 16384 % total
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

func uniformReplicaCount(topology *ClusterTopology) (int, error) {
	want := len(topology.Shards[0].Replicas)
	for _, sh := range topology.Shards {
		if len(sh.Replicas) != want {
			return 0, fmt.Errorf("topology has non-uniform replica counts")
		}
	}
	return want, nil
}

func topologyPods(topology *ClusterTopology) map[string]bool {
	out := map[string]bool{}
	for _, sh := range topology.Shards {
		if sh.Master.Pod != "" {
			out[sh.Master.Pod] = true
		}
		for _, r := range sh.Replicas {
			if r.Pod != "" {
				out[r.Pod] = true
			}
		}
	}
	return out
}

func masterPodInTopology(topology *ClusterTopology, pod string) bool {
	if pod == "" {
		return false
	}
	for _, sh := range topology.Shards {
		if sh.Master.Pod == pod {
			return true
		}
	}
	return false
}

func validateSequentialNewPods(existing, newPods map[string]bool, next int) error {
	for pod := range existing {
		if _, ok := redisPodOrdinal(pod); !ok {
			return fmt.Errorf("existing pod %q does not match supported sequential pod naming", pod)
		}
	}
	for i := 0; i < len(newPods); i++ {
		pod := fmt.Sprintf("redis-%d", next+i)
		if !newPods[pod] {
			return fmt.Errorf("new pods must be contiguous from ordinal %d; missing %q", next, pod)
		}
	}
	return nil
}

func effectiveNextPodOrdinal(ctx ValidationContext) int {
	next := ctx.NextPodOrdinal
	if ctx.Topology != nil {
		for pod := range topologyPods(ctx.Topology) {
			n, ok := redisPodOrdinal(pod)
			if ok && n >= next {
				next = n + 1
			}
		}
	}
	return next
}

func redisPodOrdinal(pod string) (int, bool) {
	i := strings.LastIndex(pod, "-")
	if i < 0 || i == len(pod)-1 {
		return 0, false
	}
	if pod[:i+1] != "redis-" {
		return 0, false
	}
	n, err := strconv.Atoi(pod[i+1:])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func precededAction(p *Plan, stepIndex int, action ActionType, paramKey, paramValue string) bool {
	for i := 0; i < stepIndex; i++ {
		s := p.Steps[i]
		if s.Action != action {
			continue
		}
		value, ok := paramString(s.Params, paramKey)
		if ok && value == paramValue {
			return true
		}
	}
	return false
}

// paramString reads a string-typed param. Non-string values are treated as
// absent: the DSL requires these fields to be strings, and the schema check is
// the responsibility of per-action executors for non-string params.
func paramString(params map[string]any, key string) (string, bool) {
	if params == nil {
		return "", false
	}
	v, ok := params[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// parseSlots parses a slot spec like "0-8191" or "0-100,200-300" into a set.
func parseSlots(s string) (map[int]struct{}, error) {
	out := map[int]struct{}{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			rg := strings.SplitN(part, "-", 2)
			start, err := strconv.Atoi(strings.TrimSpace(rg[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid slot range %q: %w", part, err)
			}
			end, err := strconv.Atoi(strings.TrimSpace(rg[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid slot range %q: %w", part, err)
			}
			if start < 0 || end > 16383 || start > end {
				return nil, fmt.Errorf("slot range %q out of bounds [0,16383]", part)
			}
			for i := start; i <= end; i++ {
				out[i] = struct{}{}
			}
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid slot %q: %w", part, err)
		}
		if n < 0 || n > 16383 {
			return nil, fmt.Errorf("slot %d out of bounds [0,16383]", n)
		}
		out[n] = struct{}{}
	}
	return out, nil
}
