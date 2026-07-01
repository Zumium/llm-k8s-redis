package validator

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

type validationError struct {
	Message string
	Hint    string
}

func (e *validationError) Error() string { return e.Message }

func verr(hint, format string, args ...any) error {
	return &validationError{Message: fmt.Sprintf(format, args...), Hint: hint}
}

func ValidationHint(err error) string {
	var ve *validationError
	if errors.As(err, &ve) && ve.Hint != "" {
		return ve.Hint
	}
	return ""
}

// ----------

func Validate(spec plan.ClusterSpec, nodes []plan.ObservedNode, p *plan.Plan) error {
	if err := isPlanValid(p); err != nil {
		return err
	}

	if err := simulatePlan(spec, nodes, p); err != nil {
		return err
	}
	return nil
}

func isPlanValid(p *plan.Plan) error {
	if p == nil {
		return fmt.Errorf("plan is nil")
	}
	if p.DSLVersion != plan.DSLVersion {
		return fmt.Errorf("dslVersion %q is not supported (expected %q)", p.DSLVersion, plan.DSLVersion)
	}
	if len(p.Steps) == 0 {
		return fmt.Errorf("plan has no steps")
	}

	seen := map[string]struct{}{}
	for i, s := range p.Steps {
		if s.ID == "" {
			return fmt.Errorf("step %d: id is empty", i)
		}
		if _, ok := seen[s.ID]; ok {
			return fmt.Errorf("step %d: duplicate step id %q", i, s.ID)
		}
		seen[s.ID] = struct{}{}

		if err := validateStepSchema(s); err != nil {
			return fmt.Errorf("step %q: %w", s.ID, err)
		}
	}

	last := p.Steps[len(p.Steps)-1]
	if last.Action != plan.ActionVerifyCluster {
		return verr("Add a VerifyCluster step as the final step of the plan", "last step must be VerifyCluster, got %q", last.Action)
	}

	return nil
}

func validateStepSchema(s plan.Step) error {
	switch s.Action {
	case plan.ActionEnsureNode:
		if err := requireStringParams(s.Params, "namespace", "image", "memorySize"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case plan.ActionWaitNodeReady:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case plan.ActionMeetNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "sourcePod", "targetPod")
	case plan.ActionReplicateNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "masterPod", "replicaPod")
	case plan.ActionAddSlots:
		if err := requireStringParams(s.Params, "namespace", "slots"); err != nil {
			return err
		}
		if err := requireSlotsParam(s.Params, "slots"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case plan.ActionMigrateSlots:
		if err := requireStringParams(s.Params, "namespace", "slots"); err != nil {
			return err
		}
		if err := requireSlotsParam(s.Params, "slots"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "sourcePod", "targetPod")
	case plan.ActionForgetNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		if _, ok := s.Params["pod"]; ok {
			if err := requirePodParams(s.Params, "pod"); err != nil {
				return err
			}
		} else if _, ok := s.Params["lastKnownNodeId"]; !ok {
			return fmt.Errorf("pod or lastKnownNodeId must be set")
		}
		if _, ok := s.Params["lastKnownNodeId"]; ok {
			return requireStringParams(s.Params, "lastKnownNodeId")
		}
		return nil
	case plan.ActionDeleteNode:
		if err := requireStringParams(s.Params, "namespace"); err != nil {
			return err
		}
		return requirePodParams(s.Params, "pod")
	case plan.ActionVerifyCluster:
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

func precededAction(p *plan.Plan, stepIndex int, action plan.ActionType, paramKey, paramValue string) bool {
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
