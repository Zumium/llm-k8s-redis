package plan

import (
	"fmt"
	"strconv"
	"strings"
)

type Validator struct{}

func NewValidator() *Validator { return &Validator{} }

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
		if s.Action != ActionVerifyCluster {
			ns, _ := paramString(s.Params, "namespace")
			if ns != spec.Name {
				return verr(fmt.Sprintf("Set namespace to %q", spec.Name), "step %q: namespace %q must equal cluster name %q", s.ID, ns, spec.Name)
			}
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
		return verr("Add a VerifyCluster step as the final step of the plan", "last step must be VerifyCluster, got %q", last.Action)
	}
	topology := validationTopology(ctx)
	ctx.Topology = topology
	if topology == nil || len(topology.Shards) == 0 {
		if err := validateCreate(spec, ensurePods); err != nil {
			return err
		}
		return simulatePlan(p, ctx)
	}
	if err := validateExistingTopologyPlan(p, spec, &ctx, topology, ensurePods); err != nil {
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

func validateCreate(spec ClusterSpec, ensurePods map[string]bool) error {
	want := int(spec.Shards) * (1 + int(spec.ReplicasPerShard))
	if len(ensurePods) != want {
		return verr(fmt.Sprintf("Create exactly %d EnsureNode steps (shards=%d * (1+replicasPerShard=%d) = %d)", want, spec.Shards, spec.ReplicasPerShard, want), "EnsureNode declared %d distinct pods, expected shards*(1+replicasPerShard)=%d", len(ensurePods), want)
	}
	return validateCreatePodNames(ensurePods)
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
		return verr("Use only whitelisted actions: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode, AddSlots, MigrateSlots, ForgetNode, DeleteNode, VerifyCluster", "action %q is not in the whitelist", s.Action)
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
			return verr(fmt.Sprintf("Rename %s to 'redis-<N>' format where N is a non-negative integer", key), "%s %q does not match required naming redis-<N>", key, pod)
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
			return verr(fmt.Sprintf("Rename %q to 'redis-<N>' format where N is a non-negative integer", pod), "pod %q does not match required naming redis-<N>", pod)
		}
		ordinals[n] = true
		if n > max {
			max = n
		}
	}
	if max != want-1 {
		return verr(fmt.Sprintf("Use pod names redis-0 through redis-%d without gaps", want-1), "EnsureNode pods must be contiguous from redis-0 to redis-%d", want-1)
	}
	if len(ordinals) != want {
		return fmt.Errorf("EnsureNode pods have duplicate ordinals")
	}
	return nil
}

func validateExistingTopologyPlan(p *Plan, spec ClusterSpec, ctx *ValidationContext, topology *ClusterTopology, ensurePods map[string]bool) error {
	currentShards := len(topology.Shards)
	if int(spec.Shards) == currentShards {
		if err := validateReplicaScaleIn(p, spec, topology, ensurePods); err == nil {
			return nil
		}
	}
	if h := healableTopology(topology, spec, ctx.ObservedNodes); h != nil {
		return validateHealRepair(p, spec, ctx, topology, ensurePods, h)
	}
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.Shards) == currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return validateReplicaScaleOut(p, spec, *ctx, topology, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateShardScaleOut(p, spec, *ctx, topology, ensurePods)
	}
	if int(spec.Shards) < currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateShardScaleIn(p, spec, *ctx, topology, ensurePods)
	}
	if int(spec.Shards) > currentShards && int(spec.ReplicasPerShard) > currentReplicas {
		return verr("Change only shards or only replicasPerShard in one plan, not both", "shards and replicasPerShard cannot both change in one scaleout")
	}
	if int(spec.Shards) == currentShards && int(spec.ReplicasPerShard) == currentReplicas {
		return validateSameSpecRepair(*ctx, topology, ensurePods)
	}
	return verr("Check that spec.shards and spec.replicasPerShard match the current topology or change only one of them",
		"unsupported existing-topology change: spec.shards=%d current=%d spec.replicasPerShard=%d current=%d", spec.Shards, currentShards, spec.ReplicasPerShard, currentReplicas)
}

func validateReplicaScaleIn(p *Plan, spec ClusterSpec, topology *ClusterTopology, ensurePods map[string]bool) error {
	target := int(spec.ReplicasPerShard)
	if target < 1 {
		return verr("replicasPerShard cannot be reduced below 1", "replicasPerShard must be >= 1 for replica scalein")
	}
	if len(ensurePods) != 0 {
		return verr("Remove all EnsureNode steps; replica scalein only uses ForgetNode, DeleteNode, VerifyCluster", "EnsureNode is not allowed for replica scalein")
	}

	needByMaster := map[string]int{}
	replicaMaster := map[string]string{}
	total := 0
	for _, sh := range topology.Shards {
		if len(sh.Replicas) < target {
			return fmt.Errorf("master %q has %d replicas, fewer than target %d", sh.Master.Pod, len(sh.Replicas), target)
		}
		need := len(sh.Replicas) - target
		needByMaster[sh.Master.Pod] = need
		total += need
		for _, r := range sh.Replicas {
			replicaMaster[r.Pod] = sh.Master.Pod
		}
	}
	if total == 0 {
		return verr("Reduce spec.replicasPerShard to a value lower than the current replica count",
			"replicasPerShard is not lower than current topology")
	}

	forgot := map[string]bool{}
	deleted := map[string]bool{}
	deletedByMaster := map[string]int{}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionForgetNode:
			pod, _ := paramString(s.Params, "pod")
			if _, ok := replicaMaster[pod]; !ok {
				return fmt.Errorf("step %q: ForgetNode target %q is not a replica", s.ID, pod)
			}
			if forgot[pod] {
				return fmt.Errorf("step %q: replica %q is forgotten more than once", s.ID, pod)
			}
			forgot[pod] = true
		case ActionDeleteNode:
			pod, _ := paramString(s.Params, "pod")
			master, ok := replicaMaster[pod]
			if !ok {
				return fmt.Errorf("step %q: DeleteNode target %q is not a replica", s.ID, pod)
			}
			if !forgot[pod] {
				return fmt.Errorf("step %q: DeleteNode target %q missing preceding ForgetNode", s.ID, pod)
			}
			if deleted[pod] {
				return fmt.Errorf("step %q: replica %q is deleted more than once", s.ID, pod)
			}
			deleted[pod] = true
			deletedByMaster[master]++
		case ActionVerifyCluster:
		default:
			return verr("For replica scalein, only use: ForgetNode, DeleteNode, VerifyCluster",
				"step %q: action %q is not allowed for replica scalein", s.ID, s.Action)
		}
	}
	for pod := range forgot {
		if !deleted[pod] {
			return fmt.Errorf("replica %q is forgotten but not deleted", pod)
		}
	}
	for master, need := range needByMaster {
		if deletedByMaster[master] != need {
			return fmt.Errorf("master %q deletes %d replicas, expected %d", master, deletedByMaster[master], need)
		}
	}
	return nil
}

func healableTopology(topology *ClusterTopology, spec ClusterSpec, observed []ObservedNode) *healState {
	if topology == nil || int(spec.Shards) != len(topology.Shards) {
		return nil
	}
	covered := map[int]string{}
	for _, sh := range topology.Shards {
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

	return &healState{
		underReplicated: underReplicated,
		ghosts:          ghosts,
	}
}

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

type healState struct {
	underReplicated []ShardTopology
	ghosts          []ObservedNode
}

func validateHealRepair(p *Plan, spec ClusterSpec, ctx *ValidationContext, topology *ClusterTopology, ensurePods map[string]bool, h *healState) error {
	ctx.healMode = true
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionForgetNode, ActionDeleteNode, ActionVerifyCluster:
		default:
			return verr("For heal repair, only use: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode, ForgetNode, DeleteNode, VerifyCluster",
				"step %q: action %q is not allowed for heal repair", s.ID, s.Action)
		}
	}

	existingPods := topologyPods(topology)
	newPods := map[string]bool{}
	for pod := range ensurePods {
		if !existingPods[pod] {
			newPods[pod] = true
		}
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(*ctx, topology)); err != nil {
		return err
	}

	needByMaster := map[string]int{}
	for _, sh := range h.underReplicated {
		need := int(spec.ReplicasPerShard) - len(sh.Replicas)
		needByMaster[sh.Master.Pod] = need
	}
	if err := validateNewReplicaAssignments(p, needByMaster, newPods, "master", "new replica pod"); err != nil {
		return err
	}

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
			pod, _ := paramString(s.Params, "pod")
			var matchedID string
			for _, g := range h.ghosts {
				if g.Pod == pod && g.PodExists {
					matchedID = g.NodeID
					break
				}
			}
			if matchedID == "" {
				return verr(fmt.Sprintf("Add 'lastKnownNodeId' param with the NodeID of pod %q from the observed state", pod),
					"step %q: ForgetNode requires lastKnownNodeId when pod %q no longer exists", s.ID, pod)
			}
			ghostSeen[matchedID] = true
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

	return nil
}

func validateSameSpecRepair(ctx ValidationContext, topology *ClusterTopology, ensurePods map[string]bool) error {
	existingPods := topologyPods(topology)
	newPods := map[string]bool{}
	for pod := range ensurePods {
		if !existingPods[pod] {
			newPods[pod] = true
		}
	}
	return validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx, topology))
}

func validateReplicaScaleOut(p *Plan, spec ClusterSpec, ctx ValidationContext, topology *ClusterTopology, ensurePods map[string]bool) error {
	currentShards := len(topology.Shards)
	if int(spec.Shards) != currentShards {
		return fmt.Errorf("only replica scaleout is supported for existing topology: spec.shards=%d current masters=%d", spec.Shards, currentShards)
	}
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.ReplicasPerShard) <= currentReplicas {
		return verr("Set spec.replicasPerShard to a number greater than current replica count",
			"replicasPerShard must increase for replica scaleout: spec=%d current=%d", spec.ReplicasPerShard, currentReplicas)
	}

	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionVerifyCluster:
		default:
			return verr("For replica scaleout, only use: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode, VerifyCluster",
				"step %q: action %q is not allowed for replica scaleout", s.ID, s.Action)
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
		return verr(fmt.Sprintf("Create exactly %d new EnsureNode steps (%d shards * %d new replicas per shard)", wantNewPods, currentShards, wantNewPods/currentShards),
			"EnsureNode declared %d new pods, expected %d new replicas", len(newPods), wantNewPods)
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx, topology)); err != nil {
		return err
	}

	needByMaster := map[string]int{}
	for _, sh := range topology.Shards {
		needByMaster[sh.Master.Pod] = int(spec.ReplicasPerShard) - currentReplicas
	}
	if err := validateNewReplicaAssignments(p, needByMaster, newPods, "existing master", "new replica pod"); err != nil {
		return err
	}
	return nil
}

func validateShardScaleOut(p *Plan, spec ClusterSpec, ctx ValidationContext, topology *ClusterTopology, ensurePods map[string]bool) error {
	currentShards := len(topology.Shards)
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.Shards) <= currentShards {
		return verr("Set spec.shards to a number greater than current master count",
			"shards must increase for shard scaleout: spec=%d current=%d", spec.Shards, currentShards)
	}
	if int(spec.ReplicasPerShard) != currentReplicas {
		return verr(fmt.Sprintf("Keep replicasPerShard at %d; shard scaleout only changes shards", currentReplicas),
			"replicasPerShard must remain %d for shard scaleout, got %d", currentReplicas, spec.ReplicasPerShard)
	}
	if err := validateTopologyReadyAndCovered(topology); err != nil {
		return err
	}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionMigrateSlots, ActionVerifyCluster:
		default:
			return verr("For shard scaleout, only use: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode, MigrateSlots, VerifyCluster",
				"step %q: action %q is not allowed for shard scaleout", s.ID, s.Action)
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
		return verr(fmt.Sprintf("Create exactly %d new EnsureNode steps: (%d new shards) * (1+replicasPerShard=%d) = %d", wantNewPods, int(spec.Shards)-currentShards, currentReplicas+1, wantNewPods),
			"EnsureNode declared %d new pods, expected %d new shard nodes", len(newPods), wantNewPods)
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx, topology)); err != nil {
		return err
	}
	newMasters, err := newShardMasters(p, newPods, ensureOrder, int(spec.Shards)-currentShards)
	if err != nil {
		return err
	}
	if err := validateNewShardReplicas(p, spec, newPods, newMasters); err != nil {
		return err
	}
	expected, err := expectedShardScaleOutMigrations(topology, newMasters)
	if err != nil {
		return err
	}
	actual, err := actualMigrations(p, topology, finalMasterSet(topology, newMasters), "final master")
	if err != nil {
		return err
	}
	if err := compareMigrationSets(expected, actual); err != nil {
		return err
	}
	return nil
}

func validateShardScaleIn(p *Plan, spec ClusterSpec, ctx ValidationContext, topology *ClusterTopology, ensurePods map[string]bool) error {
	currentShards := len(topology.Shards)
	currentReplicas, err := uniformReplicaCount(topology)
	if err != nil {
		return err
	}
	if int(spec.Shards) >= currentShards {
		return verr("Set spec.shards to a number less than current master count",
			"shards must decrease for shard scalein: spec=%d current=%d", spec.Shards, currentShards)
	}
	if int(spec.ReplicasPerShard) != currentReplicas {
		return verr(fmt.Sprintf("Keep replicasPerShard at %d; shard scalein only changes shards", currentReplicas),
			"replicasPerShard must remain %d for shard scalein, got %d", currentReplicas, spec.ReplicasPerShard)
	}
	if err := validateTopologyReadyAndCovered(topology); err != nil {
		return err
	}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionEnsureNode, ActionWaitNodeReady, ActionMeetNode, ActionReplicateNode, ActionMigrateSlots, ActionForgetNode, ActionDeleteNode, ActionVerifyCluster:
		default:
			return verr("For shard scalein, only use: EnsureNode, WaitNodeReady, MeetNode, ReplicateNode, MigrateSlots, ForgetNode, DeleteNode, VerifyCluster",
				"step %q: action %q is not allowed for shard scalein", s.ID, s.Action)
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
	wantNewPods := int(spec.Shards) * (1 + int(spec.ReplicasPerShard))
	if len(newPods) != wantNewPods {
		return verr(fmt.Sprintf("Create exactly %d new EnsureNode steps: spec.shards=%d * (1+replicasPerShard=%d) = %d", wantNewPods, spec.Shards, spec.ReplicasPerShard, wantNewPods),
			"EnsureNode declared %d new pods, expected %d replacement shard nodes", len(newPods), wantNewPods)
	}
	if err := validateSequentialNewPods(existingPods, newPods, effectiveNextPodOrdinal(ctx, topology)); err != nil {
		return err
	}

	newMasters, err := newShardMasters(p, newPods, ensureOrder, int(spec.Shards))
	if err != nil {
		return err
	}
	if err := validateNewShardReplicas(p, spec, newPods, newMasters); err != nil {
		return err
	}
	expected, err := expectedShardScaleInMigrations(topology, newMasters)
	if err != nil {
		return err
	}
	actual, err := actualMigrations(p, topology, stringSet(newMasters), "replacement master")
	if err != nil {
		return err
	}
	if err := compareMigrationSets(expected, actual); err != nil {
		return err
	}
	return validateOldPodsForgottenAndDeleted(p, existingPods)
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

func newShardMasters(p *Plan, newPods map[string]bool, ensureOrder []string, want int) ([]string, error) {
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

func validateNewShardReplicas(p *Plan, spec ClusterSpec, newPods map[string]bool, newMasters []string) error {
	needByMaster := map[string]int{}
	for _, pod := range newMasters {
		needByMaster[pod] = int(spec.ReplicasPerShard)
	}
	return validateNewReplicaAssignments(p, needByMaster, newPods, "new shard master", "new replica pod")
}

func validateNewReplicaAssignments(p *Plan, needByMaster map[string]int, replicaPods map[string]bool, masterLabel, replicaLabel string) error {
	replicaTargets := map[string]bool{}
	replicasByMaster := map[string]int{}
	for i, s := range p.Steps {
		if s.Action != ActionReplicateNode {
			continue
		}
		masterPod, ok := paramString(s.Params, "masterPod")
		if !ok || needByMaster[masterPod] == 0 {
			return fmt.Errorf("step %q: masterPod %q is not a %s", s.ID, masterPod, masterLabel)
		}
		replicaPod, ok := paramString(s.Params, "replicaPod")
		_, isMaster := needByMaster[replicaPod]
		if !ok || !replicaPods[replicaPod] || isMaster {
			return fmt.Errorf("step %q: replicaPod %q is not a %s", s.ID, replicaPod, replicaLabel)
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
	wantReplicas := 0
	for master, need := range needByMaster {
		wantReplicas += need
		if replicasByMaster[master] != need {
			return fmt.Errorf("%s %q gets %d replicas, expected %d", masterLabel, master, replicasByMaster[master], need)
		}
	}
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

func expectedShardScaleInMigrations(topology *ClusterTopology, newMasters []string) (map[migrationKey]map[int]struct{}, error) {
	currentOwner := map[int]string{}
	for _, sh := range topology.Shards {
		slots, err := parseSlots(sh.Master.Slots)
		if err != nil {
			return nil, err
		}
		for slot := range slots {
			currentOwner[slot] = sh.Master.Pod
		}
	}
	out := map[migrationKey]map[int]struct{}{}
	for i, master := range newMasters {
		start, end := balancedSlotRange(i, len(newMasters))
		for slot := start; slot <= end; slot++ {
			source := currentOwner[slot]
			if source == "" {
				return nil, fmt.Errorf("slot %d has no current owner", slot)
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

func finalMasterSet(topology *ClusterTopology, newMasters []string) map[string]bool {
	out := map[string]bool{}
	for _, sh := range topology.Shards {
		out[sh.Master.Pod] = true
	}
	for _, pod := range newMasters {
		out[pod] = true
	}
	return out
}

func actualMigrations(p *Plan, topology *ClusterTopology, targetMasters map[string]bool, targetLabel string) (map[migrationKey]map[int]struct{}, error) {
	existingMasters := map[string]bool{}
	for _, sh := range topology.Shards {
		existingMasters[sh.Master.Pod] = true
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
			return nil, fmt.Errorf("step %q: targetPod %q is not a %s", s.ID, target, targetLabel)
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

func validateOldPodsForgottenAndDeleted(p *Plan, oldPods map[string]bool) error {
	forgot := map[string]bool{}
	deleted := map[string]bool{}
	for _, s := range p.Steps {
		switch s.Action {
		case ActionForgetNode:
			pod, _ := paramString(s.Params, "pod")
			if !oldPods[pod] {
				return fmt.Errorf("step %q: ForgetNode target %q is not an old topology pod", s.ID, pod)
			}
			if forgot[pod] {
				return fmt.Errorf("step %q: old pod %q is forgotten more than once", s.ID, pod)
			}
			forgot[pod] = true
		case ActionDeleteNode:
			pod, _ := paramString(s.Params, "pod")
			if !oldPods[pod] {
				return fmt.Errorf("step %q: DeleteNode target %q is not an old topology pod", s.ID, pod)
			}
			if !forgot[pod] {
				return fmt.Errorf("step %q: DeleteNode target %q missing preceding ForgetNode", s.ID, pod)
			}
			if deleted[pod] {
				return fmt.Errorf("step %q: old pod %q is deleted more than once", s.ID, pod)
			}
			deleted[pod] = true
		}
	}
	for pod := range oldPods {
		if !forgot[pod] {
			return fmt.Errorf("old pod %q is not forgotten", pod)
		}
		if !deleted[pod] {
			return fmt.Errorf("old pod %q is not deleted", pod)
		}
	}
	return nil
}

func stringSet(items []string) map[string]bool {
	out := map[string]bool{}
	for _, item := range items {
		out[item] = true
	}
	return out
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

func effectiveNextPodOrdinal(ctx ValidationContext, topology *ClusterTopology) int {
	next := ctx.NextPodOrdinal
	if topology != nil {
		for pod := range topologyPods(topology) {
			n, ok := redisPodOrdinal(pod)
			if ok && n >= next {
				next = n + 1
			}
		}
	}
	return next
}

func redisPodOrdinal(pod string) (int, bool) {
	s, ok := strings.CutPrefix(pod, "redis-")
	if !ok || s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
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
