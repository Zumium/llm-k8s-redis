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
		Spec: sampleSpec(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil plan")
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

func TestLLMPlanner_PassesRuntimeConfig(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	p := NewLLMPlanner(fake, "deepseek-v4-flash")
	p.MaxTokens = 2048
	p.Temperature = 0.1
	p.ReasoningEffort = "max"

	_, err := p.Plan(context.Background(), Request{
		Spec: sampleSpec(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.lastReq.Model != "deepseek-v4-flash" {
		t.Errorf("model = %q", fake.lastReq.Model)
	}
	if fake.lastReq.MaxTokens != 2048 {
		t.Errorf("maxTokens = %d", fake.lastReq.MaxTokens)
	}
	if fake.lastReq.Temperature != 0.1 {
		t.Errorf("temperature = %v", fake.lastReq.Temperature)
	}
	if fake.lastReq.ReasoningEffort != "max" {
		t.Errorf("reasoningEffort = %q", fake.lastReq.ReasoningEffort)
	}
}

func TestLLMPlanner_InvalidJSON(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: "not json at all"}}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Spec: sampleSpec(),
	})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLLMPlanner_EmptyResponse(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: ""}}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Spec: sampleSpec(),
	})
	if err == nil {
		t.Fatal("expected error for empty response")
	}
}

func TestLLMPlanner_ClientError(t *testing.T) {
	fake := &fakeLLMClient{err: errors.New("network down")}
	p := NewLLMPlanner(fake, "test-model")

	_, err := p.Plan(context.Background(), Request{
		Spec: sampleSpec(),
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
		Spec: sampleSpec(),
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
		Spec: sampleSpec(),
	})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestLLMPlanner_PromptContainsSpec(t *testing.T) {
	fake := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	p := NewLLMPlanner(fake, "test-model")

	_, _ = p.Plan(context.Background(), Request{
		Spec: sampleSpec(),
		ObservedState: ObservedState{
			NextPodOrdinal: 4,
			Nodes: []ObservedNode{{
				Pod:       "redis-0",
				PodExists: true,
				RedisSeen: true,
				NodeID:    "node-a",
				Role:      "master",
				Slots:     "0-16383",
				Ready:     true,
				Flags:     []string{"master", "myself"},
				LinkState: "connected",
			}},
		},
	})

	if fake.lastReq.System == "" {
		t.Error("system prompt should not be empty")
	}
	for _, want := range []string{"Redis Cluster operations planner", "redis.ops/v1alpha1", "CLUSTER NODES", "optional last-known node id only when pod is gone"} {
		if !contains(fake.lastReq.System, want) {
			t.Errorf("system prompt missing %q", want)
		}
	}
	if fake.lastText == "" {
		t.Error("user prompt should not be empty")
	}
	for _, want := range []string{
		"RedisCluster name: example",
		"metadata.generation: 3",
		"shards: 2",
		"replicasPerShard: 1",
		"image: redis:7.2",
		"memorySize: 2Gi",
		"nextPodOrdinal: 4",
		"pod | podExists | redisSeen | nodeId | role | slots | masterId | masterPod | ready | deleting | flags | linkState",
		"redis-0",
		"node-a",
		"master",
		"0-16383",
		"connected",
		"Bring the cluster from the observed state to the desired spec.",
		"Return only the JSON plan.",
	} {
		if !contains(fake.lastText, want) {
			t.Errorf("user prompt missing %q", want)
		}
	}
	for _, bad := range []string{"Generate a Create plan", "Observed drift", "Live context", "Current topology", "replacementPod", "single-node drift", "node-id-for-deleted-pod"} {
		if contains(fake.lastText, bad) {
			t.Errorf("user prompt should not contain %q", bad)
		}
		if contains(fake.lastReq.System, bad) {
			t.Errorf("system prompt should not contain %q", bad)
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
