package planner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// fakeLLMClient is a test double for llm.Client. It records the last request
// and returns a pre-configured response (or error).
type fakeLLMClient struct {
	resp     *llm.Response
	err      error
	lastReq  llm.Request
	lastText string
}

func (f *fakeLLMClient) Complete(_ context.Context, req llm.Request) (*llm.Response, error) {
	f.lastReq = req
	if len(req.Messages) > 0 && len(req.Messages[0].Content) > 0 {
		f.lastText = req.Messages[0].Content[0].Text
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func sampleCluster() *v1alpha1.RedisCluster {
	return &v1alpha1.RedisCluster{
		Spec: v1alpha1.RedisClusterSpec{
			Shards:           2,
			ReplicasPerShard: 1,
			Image:            "redis:7.2",
			MemorySize:       "2Gi",
		},
	}
}

func sampleSpec() plan.ClusterSpec {
	return plan.ClusterSpec{
		Name:             "example",
		Generation:       3,
		Shards:           2,
		ReplicasPerShard: 1,
		Image:            "redis:7.2",
		MemorySize:       "2Gi",
	}
}

// validCreatePlanJSON returns a JSON string matching the Create plan from
// docs/OPERATIONS.md (2 shards, 1 replica per shard).
func validCreatePlanJSON() string {
	p := plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "create-001",
		Operation:        plan.OpCreate,
		TargetGeneration: 3,
		Summary:          "Create Redis Cluster with 2 shards and 1 replica per shard",
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-2", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-3", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-3", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "wait-redis-2", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "wait-redis-3", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-3"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "meet-redis-2", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2"}},
			{ID: "meet-redis-3", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-3"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "replicate-redis-3", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-3"}},
			{ID: "add-slots-redis-0", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-8191"}},
			{ID: "add-slots-redis-2", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-2", "slots": "8192-16383"}},
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	b, _ := json.Marshal(p)
	return string(b)
}

func TestLLMPlanner_ValidPlan(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	p := NewLLMPlanner(fake, "test-model")

	got, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil plan")
	}
	if got.Operation != plan.OpCreate {
		t.Errorf("operation = %q, want Create", got.Operation)
	}
	if got.TargetGeneration != 3 {
		t.Errorf("targetGeneration = %d, want 3", got.TargetGeneration)
	}
	if len(got.Steps) != 16 {
		t.Errorf("steps = %d, want 16", len(got.Steps))
	}
	if got.Steps[len(got.Steps)-1].Action != plan.ActionVerifyCluster {
		t.Error("last step should be VerifyCluster")
	}
}

func TestLLMPlanner_InvalidJSON(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: "not json at all"}}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLLMPlanner_EmptyResponse(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: ""}}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error for empty response")
	}
}

func TestLLMPlanner_ClientError(t *testing.T) {
	fake := &fakeLLMClient{err: errors.New("network down")}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error from client")
	}
}

func TestLLMPlanner_FixesDSLVersionAndGeneration(t *testing.T) {
	// LLM emits wrong dslVersion and targetGeneration; planner should fix them.
	raw := validCreatePlanJSON()
	var m map[string]any
	_ = json.Unmarshal([]byte(raw), &m)
	m["dslVersion"] = "wrong"
	m["targetGeneration"] = 999
	fixed, _ := json.Marshal(m)

	fake := &fakeLLMClient{resp: &llm.Response{Text: string(fixed)}}
	p := NewLLMPlanner(fake, "test-model")

	got, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DSLVersion != plan.DSLVersion {
		t.Errorf("dslVersion = %q, want %q", got.DSLVersion, plan.DSLVersion)
	}
	if got.TargetGeneration != 3 {
		t.Errorf("targetGeneration = %d, want 3", got.TargetGeneration)
	}
}

func TestLLMPlanner_NilClient(t *testing.T) {
	p := &LLMPlanner{}
	_, err := p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestLLMPlanner_PromptContainsSpec(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	p := NewLLMPlanner(fake, "test-model")

	_, _ = p.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})

	if fake.lastReq.System == "" {
		t.Error("system prompt should not be empty")
	}
	if fake.lastText == "" {
		t.Error("user prompt should not be empty")
	}
	// The user prompt should contain the spec fields.
	for _, want := range []string{"example", "redis:7.2", "2Gi", "Create"} {
		if !contains(fake.lastText, want) {
			t.Errorf("user prompt missing %q", want)
		}
	}
}

func TestNoopPlanner(t *testing.T) {
	_, err := NoopPlanner{}.Plan(context.Background(), Request{})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
