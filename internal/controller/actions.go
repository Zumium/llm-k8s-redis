package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
)

// ActionExecutor is the production Executor: it dispatches each plan step to a
// per-action handler. Handlers are idempotent and re-derive state from K8S and
// Redis on every call; they never rely on in-memory state from a previous
// reconcile.
//
// Only a subset of actions is implemented today. Unimplemented actions return
// a Failed outcome with a descriptive error so the plan halts cleanly instead
// of silently no-op'ing.
type ActionExecutor struct {
	client.Client
	Scheme       *runtime.Scheme
	RedisFactory redis.Factory
}

// compile-time check that ActionExecutor satisfies Executor.
var _ Executor = &ActionExecutor{}

// ExecuteStep dispatches to the per-action handler.
func (e *ActionExecutor) ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	if p == nil || stepIndex < 0 || stepIndex >= len(p.Steps) {
		return StepOutcome{Status: plan.StepStateFailed, Message: "invalid step index"}, fmt.Errorf("invalid step index %d", stepIndex)
	}
	step := p.Steps[stepIndex]
	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
	}
	switch step.Action {
	case plan.ActionEnsureNode:
		return e.ensureNode(ctx, cluster, step)
	case plan.ActionWaitNodeReady:
		return e.waitNodeReady(ctx, cluster, p, stepIndex)
	case plan.ActionMeetNode:
		return e.meetNode(ctx, cluster, p, stepIndex)
	case plan.ActionReplicateNode:
		return e.replicateNode(ctx, cluster, p, stepIndex)
	case plan.ActionAddSlots:
		return e.addSlots(ctx, cluster, p, stepIndex)
	case plan.ActionVerifyCluster:
		return e.verifyCluster(ctx, cluster, step)
	default:
		msg := fmt.Sprintf("action %q is not yet implemented", step.Action)
		return StepOutcome{Status: plan.StepStateFailed, Message: msg}, fmt.Errorf("%s", msg)
	}
}

// paramErr builds a Failed outcome for a parameter/safety violation. The
// returned error is non-nil so the reconciler records the failure path, but
// the outcome message is what lands in status.
func paramErr(format string, args ...any) (StepOutcome, error) {
	msg := fmt.Sprintf(format, args...)
	return StepOutcome{Status: plan.StepStateFailed, Message: msg}, fmt.Errorf("%s", msg)
}

// running returns a Running outcome with a human-readable reason. The
// reconciler requeues after a short delay so the step is retried.
func running(format string, args ...any) StepOutcome {
	return StepOutcome{Status: plan.StepStateRunning, Message: fmt.Sprintf(format, args...)}
}

// completed returns a Completed outcome.
func completed(format string, args ...any) StepOutcome {
	return StepOutcome{Status: plan.StepStateCompleted, Message: fmt.Sprintf(format, args...)}
}

// paramString reads a required string param. Returns ok=false if the key is
// missing, nil, or not a string.
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

// requireString reads a string param and fails the step if it is missing or
// empty. On success it returns the value and ok=true; on failure it returns
// ready-made outcome and err values and ok=false.
func requireString(params map[string]any, key string) (value string, outcome StepOutcome, err error, ok bool) {
	v, found := paramString(params, key)
	if !found || v == "" {
		o, e := paramErr("missing required string param %q", key)
		return "", o, e, false
	}
	return v, StepOutcome{}, nil, true
}

// paramInt reads an int param, accepting int, int32, int64 or an integer
// float64 (the form json.Unmarshal produces). It returns ok=false (with a
// Failed outcome) when the key is missing, nil, the wrong type, or a
// non-integer float.
func paramInt(params map[string]any, key string) (value int, outcome StepOutcome, err error, ok bool) {
	if params == nil {
		o, e := paramErr("missing required int param %q", key)
		return 0, o, e, false
	}
	v, found := params[key]
	if !found || v == nil {
		o, e := paramErr("missing required int param %q", key)
		return 0, o, e, false
	}
	switch n := v.(type) {
	case int:
		return n, StepOutcome{}, nil, true
	case int32:
		return int(n), StepOutcome{}, nil, true
	case int64:
		return int(n), StepOutcome{}, nil, true
	case float64:
		if n != float64(int(n)) {
			o, e := paramErr("param %q is not an integer: %v", key, n)
			return 0, o, e, false
		}
		return int(n), StepOutcome{}, nil, true
	default:
		o, e := paramErr("param %q is not an int: %T", key, v)
		return 0, o, e, false
	}
}

// requireBool reads a bool param. json.Unmarshal decodes JSON booleans to
// Go bool, so the only accepted type is bool. Returns ok=false (with a
// Failed outcome) when the key is missing, nil, or the wrong type.
func requireBool(params map[string]any, key string) (value bool, outcome StepOutcome, err error, ok bool) {
	if params == nil {
		o, e := paramErr("missing required bool param %q", key)
		return false, o, e, false
	}
	v, found := params[key]
	if !found || v == nil {
		o, e := paramErr("missing required bool param %q", key)
		return false, o, e, false
	}
	b, ok2 := v.(bool)
	if !ok2 {
		o, e := paramErr("param %q is not a bool: %T", key, v)
		return false, o, e, false
	}
	return b, StepOutcome{}, nil, true
}
