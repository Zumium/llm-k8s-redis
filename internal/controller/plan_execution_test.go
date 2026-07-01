package controller

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func TestExecuteNextStep_PersistsUpdatedParams(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	active := runningPlan()
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{outcome: StepOutcome{
		Status:  plan.StepStateRunning,
		Message: "waiting",
	}, params: map[string]any{"foo": "bar", "stable": 1}}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	if _, err := r.executeNextStep(ctx, cluster, active); err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(active.Steps[0].Params.Raw, &got); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if got["foo"] != "bar" || got["stable"].(float64) != 1 {
		t.Fatalf("unexpected persisted params: %#v", got)
	}
}

func TestExecuteNextStep_NeedsRepairSupersedesPlan(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	active := runningPlan()
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{outcome: StepOutcome{
		Status:        plan.StepStateFailed,
		Message:       "repair needed",
		SupersedePlan: true,
	}}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	res, err := r.executeNextStep(ctx, cluster, active)
	if err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue for replanning")
	}
	if active.Status != string(plan.PlanStateSuperseded) {
		t.Fatalf("expected superseded plan, got %#v", active)
	}
	if active.Steps[0].Status != string(plan.StepStateRunning) {
		t.Fatalf("expected running step while replanning, got %#v", active.Steps[0])
	}
}

func TestHandleActivePlan_ClearsSupersededPlanPreservingMessages(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	active := runningPlan()
	active.Status = string(plan.PlanStateSuperseded)
	cluster.Status.ActivePlan = active
	setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "repair needed")
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "repair needed")
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme}

	res, err, handled := r.handleActivePlan(ctx, cluster)
	if err != nil {
		t.Fatalf("handleActivePlan: %v", err)
	}
	if !handled || !res.Requeue {
		t.Fatalf("expected handled requeue, handled=%v result=%+v", handled, res)
	}
	if cluster.Status.ActivePlan != nil {
		t.Fatalf("expected active plan cleared, got %#v", cluster.Status.ActivePlan)
	}
	for _, typ := range []string{ConditionReady, ConditionPlanned} {
		found := false
		for _, c := range cluster.Status.Conditions {
			if c.Type == typ && c.Message == "repair needed" {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing preserved %s message: %#v", typ, cluster.Status.Conditions)
		}
	}
}

func TestExecuteNextStep_RunsOnlyCurrentMigrateStep(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	p := &plan.Plan{PlanID: "migrate", TargetGeneration: 1, Steps: []plan.Step{
		{ID: "m0", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-6", "slots": "0-1"}},
		{ID: "m1", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-2", "targetPod": "redis-8", "slots": "2-3"}},
		{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{}},
	}}
	active, err := planToStatus(p)
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	res, err := r.executeNextStep(ctx, cluster, active)
	if err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after completed migrate batch")
	}
	exec.mu.Lock()
	got := append([]int{}, exec.indices...)
	exec.mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected only step 0 to run, got %v", got)
	}
	if active.Steps[0].Status != string(plan.StepStateCompleted) || active.Steps[1].Status != string(plan.StepStatePending) || active.Steps[2].Status != string(plan.StepStatePending) {
		t.Fatalf("unexpected statuses: %#v", active.Steps)
	}
}

func TestExecuteNextStep_DoesNotBatchConflictingMigrateSteps(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	p := &plan.Plan{PlanID: "migrate", TargetGeneration: 1, Steps: []plan.Step{
		{ID: "m0", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-6", "slots": "0-1"}},
		{ID: "m1", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-8", "slots": "2-3"}},
	}}
	active, err := planToStatus(p)
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	if _, err := r.executeNextStep(ctx, cluster, active); err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	exec.mu.Lock()
	got := append([]int{}, exec.indices...)
	exec.mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected only step 0 to run, got %v", got)
	}
	if active.Steps[0].Status != string(plan.StepStateCompleted) || active.Steps[1].Status != string(plan.StepStatePending) {
		t.Fatalf("unexpected statuses: %#v", active.Steps)
	}
}
