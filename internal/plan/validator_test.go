package plan

import "testing"

func validCreatePlan() *Plan {
	return &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "create-001",
		Operation:        OpCreate,
		TargetGeneration: 1,
		Summary:          "Create Redis Cluster with 2 shards and 1 replica per shard",
		Steps: []Step{
			{ID: "ensure-redis-0", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-2", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-3", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-3", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "wait-redis-2", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "wait-redis-3", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-3"}},
			{ID: "meet-redis-1", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "meet-redis-2", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2"}},
			{ID: "meet-redis-3", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-3"}},
			{ID: "replicate-redis-1", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "replicate-redis-3", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-3"}},
			{ID: "add-slots-redis-0", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-8191"}},
			{ID: "add-slots-redis-2", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-2", "slots": "8192-16383"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func spec() ClusterSpec {
	return ClusterSpec{Name: "example", Generation: 1, Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"}
}

func TestValidate_ValidCreate(t *testing.T) {
	if err := NewValidator().Validate(validCreatePlan(), spec()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_BadDSLVersion(t *testing.T) {
	p := validCreatePlan()
	p.DSLVersion = "wrong"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for bad dslVersion")
	}
}

func TestValidate_TargetGenerationMismatch(t *testing.T) {
	p := validCreatePlan()
	p.TargetGeneration = 2
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for targetGeneration mismatch")
	}
}

func TestValidate_UnknownAction(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Action = "RunCommand"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for non-whitelisted action")
	}
}

func TestValidate_DuplicateStepID(t *testing.T) {
	p := validCreatePlan()
	p.Steps[1].ID = "ensure-redis-0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for duplicate step id")
	}
}

func TestValidate_NamespaceMismatch(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["namespace"] = "other"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for namespace mismatch")
	}
}

func TestValidate_EnsureNodeCount(t *testing.T) {
	s := spec()
	s.ReplicasPerShard = 2 // want 6 nodes, plan has 4
	if err := NewValidator().Validate(validCreatePlan(), s); err == nil {
		t.Fatal("expected error for EnsureNode count mismatch")
	}
}

func TestValidate_ImageMismatch(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["image"] = "redis:7.0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for image mismatch")
	}
}

func TestValidate_AddSlotsIncompleteCoverage(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["slots"] = "0-4095"
	p.Steps[15].Params["slots"] = "8192-16383"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for incomplete slot coverage")
	}
}

func TestValidate_AddSlotsOverlap(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["slots"] = "0-8192"
	p.Steps[15].Params["slots"] = "8192-16383"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for overlapping slots")
	}
}

func TestValidate_AddSlotsUndeclaredPod(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["pod"] = "redis-9"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for AddSlots targeting undeclared pod")
	}
}

func TestValidate_LastStepNotVerify(t *testing.T) {
	p := validCreatePlan()
	p.Steps = append(p.Steps, Step{ID: "wait-again", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}})
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error when last step is not VerifyCluster")
	}
}

func TestParseSlots(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"0-2", 3},
		{"0,1,2", 3},
		{"0-1,3-4", 4},
		{"5", 1},
	}
	for _, c := range cases {
		got, err := parseSlots(c.in)
		if err != nil {
			t.Fatalf("parseSlots(%q) error: %v", c.in, err)
		}
		if len(got) != c.want {
			t.Fatalf("parseSlots(%q) = %d slots, want %d", c.in, len(got), c.want)
		}
	}
	if _, err := parseSlots("0-99999"); err == nil {
		t.Fatal("expected error for out-of-bounds slot")
	}
}
