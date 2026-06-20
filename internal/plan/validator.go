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

	if ctx.Drift != nil {
		return validateDrift(p, spec, ctx, ensurePods)
	}
	if ctx.Topology == nil || len(ctx.Topology.Shards) == 0 {
		if err := validateCreate(p, spec, ensurePods); err != nil {
			return err
		}
		return simulatePlan(p, ctx)
	}
	if err := validateExistingTopologyPlan(p, spec, ctx, ensurePods); err != nil {
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

	// EnsureNode image/memorySize must match spec.
	for _, s := range p.Steps {
		if s.Action != ActionEnsureNode {
			continue
		}
		if img, _ := paramString(s.Params, "image"); img != spec.Image {
			return fmt.Errorf("step %q: image %q must equal spec.image %q", s.ID, img, spec.Image)
		}
		if mem, _ := paramString(s.Params, "memorySize"); mem != spec.MemorySize {
			return fmt.Errorf("step %q: memorySize %q must equal spec.memorySize %q", s.ID, mem, spec.MemorySize)
		}
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

	// The plan must terminate with VerifyCluster.
	last := p.Steps[len(p.Steps)-1]
	if last.Action != ActionVerifyCluster {
		return fmt.Errorf("last step must be VerifyCluster, got %q", last.Action)
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

func validateDrift(p *Plan, spec ClusterSpec, ctx ValidationContext, ensurePods map[string]bool) error {
	d := ctx.Drift
	if d == nil {
		return fmt.Errorf("drift plan requires observed drift context")
	}
	if d.ReplacementPod == "" || d.TargetMasterPod == "" {
		return fmt.Errorf("observed drift context is incomplete")
	}
	if d.BaselineShards < 1 || d.BaselineReplicas < 0 {
		return fmt.Errorf("observed drift counts must have positive shards and non-negative replicas")
	}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionForgetNode, ActionVerifyCluster:
		default:
			return fmt.Errorf("step %q: action %q is not allowed for observed drift convergence", s.ID, s.Action)
		}
	}
	last := p.Steps[len(p.Steps)-1]
	if last.Action != ActionVerifyCluster {
		return fmt.Errorf("last step must be VerifyCluster, got %q", last.Action)
	}
	if len(ensurePods) != 1 || !ensurePods[d.ReplacementPod] {
		return fmt.Errorf("drift convergence EnsureNode must declare only replacement pod %q", d.ReplacementPod)
	}
	if err := validateDriftReplacementOrdinal(ctx, d.ReplacementPod); err != nil {
		return err
	}
	for i, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode:
			pod, _ := paramString(s.Params, "pod")
			if pod != d.ReplacementPod {
				return fmt.Errorf("step %q: EnsureNode pod %q must equal replacement pod %q", s.ID, pod, d.ReplacementPod)
			}
			if img, _ := paramString(s.Params, "image"); img != spec.Image {
				return fmt.Errorf("step %q: image %q must equal spec.image %q", s.ID, img, spec.Image)
			}
			if mem, _ := paramString(s.Params, "memorySize"); mem != spec.MemorySize {
				return fmt.Errorf("step %q: memorySize %q must equal spec.memorySize %q", s.ID, mem, spec.MemorySize)
			}
		case ActionWaitNodeReady:
			pod, _ := paramString(s.Params, "pod")
			if pod != d.ReplacementPod {
				return fmt.Errorf("step %q: WaitNodeReady pod %q must equal replacement pod %q", s.ID, pod, d.ReplacementPod)
			}
			if !precededAction(p, i, ActionEnsureNode, "pod", d.ReplacementPod) {
				return fmt.Errorf("step %q: replacement pod missing preceding EnsureNode", s.ID)
			}
		case ActionMeetNode:
			target, _ := paramString(s.Params, "targetPod")
			if target != d.ReplacementPod {
				return fmt.Errorf("step %q: MeetNode targetPod %q must equal replacement pod %q", s.ID, target, d.ReplacementPod)
			}
			source, _ := paramString(s.Params, "sourcePod")
			if source == d.ReplacementPod {
				return fmt.Errorf("step %q: MeetNode sourcePod must not be replacement pod", s.ID)
			}
			if !precededAction(p, i, ActionWaitNodeReady, "pod", d.ReplacementPod) {
				return fmt.Errorf("step %q: replacement pod missing preceding WaitNodeReady", s.ID)
			}
		case ActionReplicateNode:
			master, _ := paramString(s.Params, "masterPod")
			replica, _ := paramString(s.Params, "replicaPod")
			if master != d.TargetMasterPod || replica != d.ReplacementPod {
				return fmt.Errorf("step %q: ReplicateNode must replicate replacement pod %q from target master %q", s.ID, d.ReplacementPod, d.TargetMasterPod)
			}
			if !precededAction(p, i, ActionMeetNode, "targetPod", d.ReplacementPod) {
				return fmt.Errorf("step %q: replacement pod missing preceding MeetNode", s.ID)
			}
		case ActionForgetNode:
			if d.MissingPod == "" || d.LastKnownNodeID == "" {
				return fmt.Errorf("step %q: ForgetNode is not allowed without missing pod and lastKnownNodeId in observed drift context", s.ID)
			}
			pod, _ := paramString(s.Params, "pod")
			nodeID, _ := paramString(s.Params, "lastKnownNodeId")
			if pod != d.MissingPod || nodeID != d.LastKnownNodeID {
				return fmt.Errorf("step %q: ForgetNode must target missing pod %q with lastKnownNodeId %q", s.ID, d.MissingPod, d.LastKnownNodeID)
			}
		case ActionVerifyCluster:
			expectedShards, ok := paramInt(s.Params, "expectedShards")
			if !ok || expectedShards != int(spec.Shards) {
				return fmt.Errorf("step %q: expectedShards must equal spec.shards %d", s.ID, spec.Shards)
			}
			expectedReplicas, ok := paramInt(s.Params, "expectedReplicasPerShard")
			if !ok || expectedReplicas != int(spec.ReplicasPerShard) {
				return fmt.Errorf("step %q: expectedReplicasPerShard must equal spec.replicasPerShard %d", s.ID, spec.ReplicasPerShard)
			}
		}
	}
	return nil
}

func validateExistingTopologyPlan(p *Plan, spec ClusterSpec, ctx ValidationContext, ensurePods map[string]bool) error {
	topology := ctx.Topology
	currentShards := len(topology.Shards)
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.Shards) == currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return validateReplicaScaleOut(p, spec, ctx, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateShardScaleOut(p, spec, ctx, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return fmt.Errorf("shards and replicasPerShard cannot both change in one scaleout")
	}
	return fmt.Errorf("unsupported existing-topology change: spec.shards=%d current=%d spec.replicasPerShard=%d current=%d", spec.Shards, currentShards, spec.ReplicasPerShard, currentReplicas)
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
	last := p.Steps[len(p.Steps)-1]
	if last.Action != ActionVerifyCluster {
		return fmt.Errorf("last step must be VerifyCluster, got %q", last.Action)
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

	for _, s := range p.Steps {
		if s.Action != ActionEnsureNode {
			continue
		}
		if img, _ := paramString(s.Params, "image"); img != spec.Image {
			return fmt.Errorf("step %q: image %q must equal spec.image %q", s.ID, img, spec.Image)
		}
		if mem, _ := paramString(s.Params, "memorySize"); mem != spec.MemorySize {
			return fmt.Errorf("step %q: memorySize %q must equal spec.memorySize %q", s.ID, mem, spec.MemorySize)
		}
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
	last := p.Steps[len(p.Steps)-1]
	if last.Action != ActionVerifyCluster {
		return fmt.Errorf("last step must be VerifyCluster, got %q", last.Action)
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
	for _, s := range p.Steps {
		if s.Action != ActionEnsureNode {
			continue
		}
		if img, _ := paramString(s.Params, "image"); img != spec.Image {
			return fmt.Errorf("step %q: image %q must equal spec.image %q", s.ID, img, spec.Image)
		}
		if mem, _ := paramString(s.Params, "memorySize"); mem != spec.MemorySize {
			return fmt.Errorf("step %q: memorySize %q must equal spec.memorySize %q", s.ID, mem, spec.MemorySize)
		}
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

func validateDriftReplacementOrdinal(ctx ValidationContext, pod string) error {
	n, ok := redisPodOrdinal(pod)
	if !ok {
		return fmt.Errorf("replacement pod %q does not match required naming redis-<N>", pod)
	}
	next := effectiveNextPodOrdinal(ctx)
	if n != next {
		return fmt.Errorf("replacement pod must be redis-%d, got %q", next, pod)
	}
	return nil
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
