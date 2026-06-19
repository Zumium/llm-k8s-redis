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

	if ctx.Topology == nil || len(ctx.Topology.Shards) == 0 {
		if err := validateCreate(p, spec, ensurePods); err != nil {
			return err
		}
		return simulatePlan(p, ctx)
	}
	if err := validateReplicaScaleOut(p, spec, ctx.Topology, ensurePods); err != nil {
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

func validateReplicaScaleOut(p *Plan, spec ClusterSpec, topology *ClusterTopology, ensurePods map[string]bool) error {
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
	if err := validateSequentialNewPods(existingPods, newPods); err != nil {
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

func validateSequentialNewPods(existing, newPods map[string]bool) error {
	max := -1
	for pod := range existing {
		n, ok := redisPodOrdinal(pod)
		if !ok {
			return fmt.Errorf("existing pod %q does not match supported sequential pod naming", pod)
		}
		if n > max {
			max = n
		}
	}
	for i := 1; i <= len(newPods); i++ {
		pod := fmt.Sprintf("redis-%d", max+i)
		if !newPods[pod] {
			return fmt.Errorf("new pods must be contiguous from ordinal %d; missing %q", max+1, pod)
		}
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
