package planner

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/example/llm-k8s-redis/internal/plan"
)

type recordingLLMClient struct {
	resp    *LLMResponse
	err     error
	lastReq LLMRequest
}

func (c *recordingLLMClient) Complete(_ context.Context, req LLMRequest) (*LLMResponse, error) {
	c.lastReq = req
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
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

func validMinimalPlanJSON() string {
	b, _ := json.Marshal(plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "create-001",
		TargetGeneration: 3,
		Steps: []plan.Step{
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{}},
		},
	})
	return string(b)
}

func TestLLMPlanner_ValidPlan(t *testing.T) {
	p := NewLLMPlanner(&recordingLLMClient{resp: &LLMResponse{Text: validMinimalPlanJSON()}})

	got, err := p.Plan(context.Background(), Request{Spec: sampleSpec()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TargetGeneration != 3 || len(got.Steps) != 1 {
		t.Fatalf("plan = %#v", got)
	}
}

func TestLLMPlanner_InvalidResponses(t *testing.T) {
	cases := []struct {
		name      string
		llmClient *recordingLLMClient
	}{
		{name: "invalid json", llmClient: &recordingLLMClient{resp: &LLMResponse{Text: "not json"}}},
		{name: "empty text", llmClient: &recordingLLMClient{resp: &LLMResponse{Text: ""}}},
		{name: "client error", llmClient: &recordingLLMClient{err: errors.New("network down")}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			p := NewLLMPlanner(testCase.llmClient)
			if _, err := p.Plan(context.Background(), Request{Spec: sampleSpec()}); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLLMPlanner_FixesDSLVersionAndGeneration(t *testing.T) {
	var m map[string]any
	_ = json.Unmarshal([]byte(validMinimalPlanJSON()), &m)
	m["dslVersion"] = "wrong"
	m["targetGeneration"] = 999
	fixed, _ := json.Marshal(m)

	got, err := NewLLMPlanner(&recordingLLMClient{resp: &LLMResponse{Text: string(fixed)}}).Plan(context.Background(), Request{Spec: sampleSpec()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DSLVersion != plan.DSLVersion || got.TargetGeneration != 3 {
		t.Fatalf("plan = %#v", got)
	}
}

func TestLLMPlanner_NilClient(t *testing.T) {
	if _, err := (&LLMPlanner{}).Plan(context.Background(), Request{Spec: sampleSpec()}); err == nil {
		t.Fatal("expected error")
	}
}

func TestLLMPlanner_PromptContainsSpec(t *testing.T) {
	llmClient := &recordingLLMClient{resp: &LLMResponse{Text: validMinimalPlanJSON()}}
	_, _ = NewLLMPlanner(llmClient).Plan(context.Background(), Request{
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

	for _, want := range []string{"Redis Cluster operations planner", "redis.ops/v1alpha1", "CLUSTER NODES"} {
		if !strings.Contains(llmClient.lastReq.System, want) {
			t.Errorf("system prompt missing %q", want)
		}
	}
	for _, want := range []string{"RedisCluster name: example", "metadata.generation: 3", "nextPodOrdinal: 4", "redis-0", "node-a", "connected"} {
		if !strings.Contains(llmClient.lastReq.Prompt, want) {
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
