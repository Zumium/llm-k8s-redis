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

func knownOperation(op Operation) bool {
	switch op {
	case OpCreate, OpDelete, OpScaleOut, OpScaleIn, OpUpdateMemorySize:
		return true
	}
	return false
}

// Validator enforces the deterministic safety invariants described in
// docs/OPERATIONS.md and docs/ACTIONS.md. It never calls out to the LLM, K8S,
// or Redis: validation must be fully reproducible from the plan and the spec.
type Validator struct{}

// NewValidator returns a Validator.
func NewValidator() *Validator { return &Validator{} }

// Validate runs generic checks that apply to every plan, then dispatches to
// operation-specific checks. Operation-specific checks for Delete/ScaleOut/
// ScaleIn/UpdateMemorySize are added alongside their executors; until then
// those operations still must pass the generic checks below.
func (v *Validator) Validate(p *Plan, spec ClusterSpec) error {
	if p == nil {
		return fmt.Errorf("plan is nil")
	}
	if p.DSLVersion != DSLVersion {
		return fmt.Errorf("dslVersion %q is not supported (expected %q)", p.DSLVersion, DSLVersion)
	}
	if !knownOperation(p.Operation) {
		return fmt.Errorf("unknown operation %q", p.Operation)
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

	switch p.Operation {
	case OpCreate:
		return validateCreate(p, spec, ensurePods)
	default:
		return nil
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
