package planner

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

type recordingLLMClient struct {
	resp    *LLMResponse
	resps   []*LLMResponse
	err     error
	lastReq LLMRequest
	reqs    []LLMRequest
}

func (c *recordingLLMClient) Complete(_ context.Context, req LLMRequest) (*LLMResponse, error) {
	c.lastReq = req
	c.reqs = append(c.reqs, req)
	if c.err != nil {
		return nil, c.err
	}
	if len(c.resps) > 0 {
		resp := c.resps[0]
		c.resps = c.resps[1:]
		return resp, nil
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

func validAnalysisJSON() string {
	return `{"subprocesses":["changeClusterSpec"],"summary":"scale toward desired spec"}`
}

func recordingClientWithPlan() *recordingLLMClient {
	return &recordingLLMClient{resps: []*LLMResponse{
		{Text: validAnalysisJSON()},
		{Text: validMinimalPlanJSON()},
	}}
}

func TestLLMPlanner_ValidPlan(t *testing.T) {
	llmClient := recordingClientWithPlan()
	p := NewLLMPlanner(llmClient)

	got, err := p.Plan(context.Background(), Request{Spec: sampleSpec()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TargetGeneration != 3 || len(got.Steps) != 1 {
		t.Fatalf("plan = %#v", got)
	}
	if len(llmClient.reqs) != 2 {
		t.Fatalf("llm calls = %d", len(llmClient.reqs))
	}
}

func TestLLMPlanner_InvalidResponses(t *testing.T) {
	cases := []struct {
		name      string
		llmClient *recordingLLMClient
	}{
		{name: "invalid plan json", llmClient: &recordingLLMClient{resps: []*LLMResponse{{Text: validAnalysisJSON()}, {Text: "not json"}}}},
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

	got, err := NewLLMPlanner(&recordingLLMClient{resps: []*LLMResponse{{Text: validAnalysisJSON()}, {Text: string(fixed)}}}).Plan(context.Background(), Request{Spec: sampleSpec()})
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
	llmClient := recordingClientWithPlan()
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

	for _, want := range []string{"Redis Cluster operations planner", "redis.ops/v1alpha1", "CLUSTER NODES", "Repair observed topology drift", "ghost node"} {
		if llmClient.lastReq.Messages[0].Role != "system" || !strings.Contains(llmClient.lastReq.Messages[0].Content, want) {
			t.Errorf("system prompt missing %q", want)
		}
	}
	for _, want := range []string{"RedisCluster name: example", "metadata.generation: 3", "nextPodOrdinal: 4", "redis-0", "node-a", "connected"} {
		if llmClient.lastReq.Messages[1].Role != "user" || !strings.Contains(llmClient.lastReq.Messages[1].Content, want) {
			t.Errorf("user prompt missing %q", want)
		}
	}
	if !strings.Contains(llmClient.reqs[0].Messages[1].Content, "repairTopology, cleanupGhostNodes, cleanupDirtyNodes, changeClusterSpec") {
		t.Fatalf("analysis prompt missing subprocess labels")
	}
	if !strings.Contains(llmClient.lastReq.Messages[2].Content, validAnalysisJSON()) {
		t.Fatalf("plan prompt missing subprocess analysis")
	}
	if !strings.Contains(llmClient.lastReq.Messages[3].Content, "analysis above") {
		t.Fatalf("plan prompt does not reference analysis")
	}
}

func TestLLMPlanner_PromptContainsActionSemantics(t *testing.T) {
	llmClient := recordingClientWithPlan()
	_, _ = NewLLMPlanner(llmClient).Plan(context.Background(), Request{Spec: sampleSpec()})

	systemPrompt := llmClient.lastReq.Messages[0].Content
	for _, want := range []string{
		"EnsureNode: Ensure the Redis Pod exists",
		"does not meet nodes, set replicas, or assign slots",
		"WaitNodeReady: Wait until the Pod is Ready and Redis is reachable",
		"MeetNode: Join targetPod to the Redis Cluster gossip network",
		"ReplicateNode: Make replicaPod replicate from masterPod",
		"replicaPod must not own slots",
		"AddSlots: Assign only unowned slots to a master",
		"do not use it to move slots already owned by another master",
		"MigrateSlots: Move slots from source master to target master",
		"execution continues in batches",
		"ForgetNode: Remove a node from Redis Cluster membership without deleting its Pod",
		"include lastKnownNodeId when the Pod is gone",
		"DeleteNode: Delete the Kubernetes resources for a node",
		"only safe after ForgetNode or if the node never joined Redis Cluster",
		"VerifyCluster: Verify final state without changing topology",
		"count only slot-owning masters",
		"wait for extra no-slot masters as gossip convergence",
	} {
		if !strings.Contains(systemPrompt, want) {
			t.Errorf("system prompt missing %q", want)
		}
	}
}

func TestLLMPlanner_FeedbackUsesMessageHistory(t *testing.T) {
	llmClient := recordingClientWithPlan()
	rejectedPlan := &plan.Plan{PlanID: "bad-plan", Steps: []plan.Step{{ID: "bad", Action: plan.ActionAddSlots}}}

	_, err := NewLLMPlanner(llmClient).Plan(context.Background(), Request{
		Spec: sampleSpec(),
		ValidationFeedback: []ValidationFeedback{{
			RejectedPlan: rejectedPlan,
			Error:        "slot coverage is incomplete",
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	messages := llmClient.lastReq.Messages
	if len(messages) != 6 {
		t.Fatalf("messages = %#v", messages)
	}
	if messages[0].Role != "system" || messages[1].Role != "user" || messages[2].Role != "assistant" || messages[3].Role != "user" || messages[4].Role != "assistant" || messages[5].Role != "user" {
		t.Fatalf("messages = %#v", messages)
	}
	for _, want := range []string{"RedisCluster name: example", validAnalysisJSON(), "bad-plan", "slot coverage is incomplete"} {
		found := false
		for _, message := range messages {
			if strings.Contains(message.Content, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("messages missing %q: %#v", want, messages)
		}
	}
}

func TestNoopPlanner(t *testing.T) {
	_, err := NoopPlanner{}.Plan(context.Background(), Request{})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got %v", err)
	}
}

func observedThreeShardOneReplica() []ObservedNode {
	return []ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "n0", Role: "master", Slots: "0-5461", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "n1", Role: "replica", MasterPod: "redis-0", Ready: true, Flags: []string{"slave"}, LinkState: "connected"},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "n2", Role: "master", Slots: "5462-10922", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "n3", Role: "replica", MasterPod: "redis-2", Ready: true, Flags: []string{"slave"}, LinkState: "connected"},
		{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "n4", Role: "master", Slots: "10923-16383", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-5", PodExists: true, RedisSeen: true, NodeID: "n5", Role: "replica", MasterPod: "redis-4", Ready: true, Flags: []string{"slave"}, LinkState: "connected"},
	}
}

func systemPromptPlans(t *testing.T) []plan.Plan {
	t.Helper()
	var plans []plan.Plan
	for _, part := range strings.Split(buildSystemPrompt(), "```json")[1:] {
		block, _, ok := strings.Cut(part, "```")
		if !ok {
			t.Fatal("unterminated json fence")
		}
		var p plan.Plan
		if err := json.Unmarshal([]byte(strings.TrimSpace(block)), &p); err == nil && p.PlanID != "" {
			plans = append(plans, p)
		}
	}
	return plans
}

func TestSystemPromptExamplesAreValid(t *testing.T) {
	observed := observedThreeShardOneReplica()
	inputs := map[string]any{
		"create-001":           plan.ClusterSpec{Name: "example", Generation: 1, Shards: 3, ReplicasPerShard: 1, Image: "redis:7", MemorySize: "1Gi"},
		"replica-scaleout-001": plan.ValidationContext{Spec: plan.ClusterSpec{Name: "example", Generation: 2, Shards: 3, ReplicasPerShard: 2, Image: "redis:7", MemorySize: "1Gi"}, NextPodOrdinal: 6, ObservedNodes: observed},
		"shard-scaleout-001":   plan.ValidationContext{Spec: plan.ClusterSpec{Name: "example", Generation: 3, Shards: 4, ReplicasPerShard: 1, Image: "redis:7", MemorySize: "1Gi"}, NextPodOrdinal: 6, ObservedNodes: observed},
	}
	seen := map[string]bool{}
	for _, p := range systemPromptPlans(t) {
		input, ok := inputs[p.PlanID]
		if !ok {
			continue
		}
		seen[p.PlanID] = true
		t.Run(p.PlanID, func(t *testing.T) {
			if err := plan.NewValidator().Validate(&p, input); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
	for id := range inputs {
		if !seen[id] {
			t.Fatalf("system prompt missing example %s", id)
		}
	}
}
