package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
)

type ActionExecutor struct {
	client.Client
	Scheme       *runtime.Scheme
	RedisFactory redis.Factory
}

var _ Driver = &ActionExecutor{}

func (e *ActionExecutor) ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (outcome StepOutcome, err error) {
	start := time.Now()
	if p == nil || stepIndex < 0 || stepIndex >= len(p.Steps) {
		return StepOutcome{Status: plan.StepStateFailed, Message: "invalid step index"}, fmt.Errorf("invalid step index %d", stepIndex)
	}
	step := p.Steps[stepIndex]
	logger := log.FromContext(ctx).WithValues("planID", p.PlanID, "stepIndex", stepIndex, "stepID", step.ID, "action", step.Action)
	logger.Info("action started")
	defer func() {
		logger.Info("action finished", "duration", time.Since(start), "status", outcome.Status, "message", outcome.Message, "error", err)
	}()
	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
		logger.Info("default redis factory initialized")
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
	case plan.ActionMigrateSlots:
		return e.migrateSlots(ctx, cluster, p, stepIndex)
	case plan.ActionForgetNode:
		return e.forgetNode(ctx, cluster, step)
	case plan.ActionDeleteNode:
		return e.deleteNode(ctx, cluster, step)
	case plan.ActionVerifyCluster:
		return e.verifyCluster(ctx, cluster, p, stepIndex)
	default:
		msg := fmt.Sprintf("action %q is not yet implemented", step.Action)
		return StepOutcome{Status: plan.StepStateFailed, Message: msg}, fmt.Errorf("%s", msg)
	}
}

func paramErr(format string, args ...any) (StepOutcome, error) {
	msg := fmt.Sprintf(format, args...)
	return StepOutcome{Status: plan.StepStateFailed, Message: msg}, fmt.Errorf("%s", msg)
}

func running(format string, args ...any) StepOutcome {
	return StepOutcome{Status: plan.StepStateRunning, Message: fmt.Sprintf(format, args...)}
}

func completed(format string, args ...any) StepOutcome {
	return StepOutcome{Status: plan.StepStateCompleted, Message: fmt.Sprintf(format, args...)}
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

func requireString(params map[string]any, key string) (value string, outcome StepOutcome, err error, ok bool) {
	v, found := paramString(params, key)
	if !found || v == "" {
		o, e := paramErr("missing required string param %q", key)
		return "", o, e, false
	}
	return v, StepOutcome{}, nil, true
}

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
