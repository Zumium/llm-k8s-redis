package validator

import (
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/observor"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func validatePlan(p *plan.Plan, t *plan.ClusterTopology) error {
	return Validate(testSpec(), observedFromTopology(t), p)
}

func validatePlanWithNodes(p *plan.Plan, nodes []plan.ObservedNode) error {
	return Validate(testSpec(), nodes, p)
}

func testSpec() plan.ClusterSpec {
	return plan.ClusterSpec{Name: "example", Generation: 1, Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"}
}

func planWith(steps ...plan.Step) *plan.Plan {
	return &plan.Plan{DSLVersion: plan.DSLVersion, PlanID: "test", TargetGeneration: 1, Steps: steps}
}

func verify(shards, replicas int) plan.Step {
	return plan.Step{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{
		"expectedShards":                   shards,
		"expectedReplicasPerShard":         replicas,
		"requireClusterStateOk":            true,
		"requireFullSlotCoverage":          true,
		"requireAllSlotOwnersHaveReplicas": true,
	}}
}

func ensure(id, pod string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": pod, "image": "redis:7.2", "memorySize": "2Gi"}}
}

func wait(id, pod string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": pod}}
}

func meet(id, source, target string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": source, "targetPod": target}}
}

func replicate(id, master, replica string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": master, "replicaPod": replica}}
}

func addSlots(id, pod, slots string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": pod, "slots": slots}}
}

func migrate(id, source, target, slots string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": source, "targetPod": target, "slots": slots}}
}

func forget(id, pod string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": pod}}
}

func del(id, pod string) plan.Step {
	return plan.Step{ID: id, Action: plan.ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": pod}}
}

func validCreatePlan() *plan.Plan {
	return planWith(
		ensure("ensure-0", "redis-0"),
		ensure("ensure-1", "redis-1"),
		ensure("ensure-2", "redis-2"),
		ensure("ensure-3", "redis-3"),
		wait("wait-0", "redis-0"),
		wait("wait-1", "redis-1"),
		wait("wait-2", "redis-2"),
		wait("wait-3", "redis-3"),
		meet("meet-1", "redis-0", "redis-1"),
		meet("meet-2", "redis-0", "redis-2"),
		meet("meet-3", "redis-0", "redis-3"),
		replicate("replicate-1", "redis-0", "redis-1"),
		replicate("replicate-3", "redis-2", "redis-3"),
		addSlots("slots-0", "redis-0", "0-8191"),
		addSlots("slots-2", "redis-2", "8192-16383"),
		verify(2, 1),
	)
}

func topology() *plan.ClusterTopology {
	return &plan.ClusterTopology{Shards: []plan.ShardTopology{
		{ID: "shard-0", Master: plan.NodeTopology{Pod: "redis-0", Slots: "0-8191", Ready: true}, Replicas: []plan.NodeTopology{{Pod: "redis-1", Ready: true}}},
		{ID: "shard-1", Master: plan.NodeTopology{Pod: "redis-2", Slots: "8192-16383", Ready: true}, Replicas: []plan.NodeTopology{{Pod: "redis-3", Ready: true}}},
	}}
}

func observedFromTopology(t *plan.ClusterTopology) []plan.ObservedNode {
	if t == nil {
		return nil
	}
	var out []plan.ObservedNode
	for _, sh := range t.Shards {
		out = append(out, plan.ObservedNode{Pod: sh.Master.Pod, PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: sh.Master.NodeID, Role: "master", Slots: sh.Master.Slots, Ready: sh.Master.Ready})
		for _, r := range sh.Replicas {
			out = append(out, plan.ObservedNode{Pod: r.Pod, PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: r.NodeID, Role: "replica", MasterPod: sh.Master.Pod, Ready: r.Ready})
		}
	}
	return out
}

func TestValidate_AcceptsSchemaAndSafeSimulation(t *testing.T) {
	if err := validatePlan(validCreatePlan(), nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := validatePlan(planWith(verify(2, 1)), topology()); err != nil {
		t.Fatalf("expected existing topology verify to pass, got %v", err)
	}
}

func TestValidate_RejectsPlanShapeErrors(t *testing.T) {
	tests := []struct {
		name string
		plan *plan.Plan
	}{
		{name: "nil plan"},
		{name: "bad dsl", plan: func() *plan.Plan { p := validCreatePlan(); p.DSLVersion = "bad"; return p }()},
		{name: "duplicate ids", plan: func() *plan.Plan { p := validCreatePlan(); p.Steps[1].ID = p.Steps[0].ID; return p }()},
		{name: "last step not verify", plan: planWith(verify(2, 1), wait("wait-again", "redis-0"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validatePlan(tt.plan, nil); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateStepSchema_Actions(t *testing.T) {
	valid := []plan.Step{
		ensure("ensure", "redis-0"),
		wait("wait", "redis-0"),
		meet("meet", "redis-0", "redis-1"),
		replicate("replicate", "redis-0", "redis-1"),
		addSlots("add", "redis-0", "0-1"),
		migrate("migrate", "redis-0", "redis-2", "0-1"),
		forget("forget", "redis-1"),
		{ID: "forget-id", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "lastKnownNodeId": "node-1"}},
		del("delete", "redis-1"),
		verify(2, 1),
	}
	for _, step := range valid {
		if err := validateStepSchema(step); err != nil {
			t.Fatalf("%s should pass schema validation: %v", step.Action, err)
		}
	}

	invalid := []plan.Step{
		{Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "bad", "image": "redis:7.2", "memorySize": "2Gi"}},
		{Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example"}},
		{Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0"}},
		{Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0"}},
		{Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-99999"}},
		{Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "bad"}},
		{Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example"}},
		{Action: plan.ActionDeleteNode, Params: map[string]any{"namespace": "example", "lastKnownNodeId": "node-1"}},
		{Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": false, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		{Action: "RunCommand", Params: map[string]any{}},
	}
	for _, step := range invalid {
		if err := validateStepSchema(step); err == nil {
			t.Fatalf("%s should fail schema validation", step.Action)
		}
	}
}

func TestValidate_SimulatorSafety(t *testing.T) {
	tests := []struct {
		name string
		plan *plan.Plan
		topo *plan.ClusterTopology
	}{
		{
			name: "ordering",
			plan: planWith(ensure("ensure", "redis-4"), meet("meet", "redis-0", "redis-4"), verify(2, 1)),
			topo: topology(),
		},
		{
			name: "unknown pod",
			plan: planWith(del("delete", "redis-9"), verify(2, 1)),
			topo: topology(),
		},
		{
			name: "replica owning slots",
			plan: func() *plan.Plan {
				p := validCreatePlan()
				p.Steps = append(p.Steps[:len(p.Steps)-1], replicate("bad-replicate", "redis-2", "redis-0"), verify(2, 1))
				return p
			}(),
		},
		{
			name: "slot overlap",
			plan: func() *plan.Plan {
				p := validCreatePlan()
				p.Steps[13].Params["slots"] = "0-8192"
				return p
			}(),
		},
		{
			name: "incomplete coverage",
			plan: func() *plan.Plan {
				p := validCreatePlan()
				p.Steps[13].Params["slots"] = "0-4095"
				return p
			}(),
		},
		{
			name: "forget slot owner",
			plan: planWith(forget("forget", "redis-0"), verify(2, 1)),
			topo: topology(),
		},
		{
			name: "delete active member",
			plan: planWith(del("delete", "redis-1"), verify(2, 1)),
			topo: topology(),
		},
		{
			name: "verify mismatch",
			plan: planWith(verify(2, 2)),
			topo: topology(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validatePlan(tt.plan, tt.topo); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidate_DeleteForgottenNode(t *testing.T) {
	p := planWith(forget("forget", "redis-1"), del("delete", "redis-1"), verify(2, 0))
	topo := &plan.ClusterTopology{Shards: []plan.ShardTopology{
		{ID: "shard-0", Master: plan.NodeTopology{Pod: "redis-0", Slots: "0-16383", Ready: true}, Replicas: []plan.NodeTopology{{Pod: "redis-1", Ready: true}}},
	}}
	p.Steps[len(p.Steps)-1] = verify(1, 0)
	if err := validatePlan(p, topo); err != nil {
		t.Fatalf("expected forgotten replica delete to pass, got %v", err)
	}
}

func TestValidate_RejectsOldImageNoop(t *testing.T) {
	nodes := observedFromTopology(topology())
	for i := range nodes {
		nodes[i].Image = "redis:7.0"
	}
	if err := validatePlanWithNodes(planWith(verify(2, 1)), nodes); err == nil {
		t.Fatal("expected old image verify to fail")
	}
}

func TestValidate_AcceptsImageReplacementPlan(t *testing.T) {
	nodes := observedFromTopology(topology())
	for i := range nodes {
		nodes[i].Image = "redis:7.0"
	}
	p := planWith(
		ensure("ensure-4", "redis-4"),
		ensure("ensure-5", "redis-5"),
		ensure("ensure-6", "redis-6"),
		ensure("ensure-7", "redis-7"),
		wait("wait-4", "redis-4"),
		wait("wait-5", "redis-5"),
		wait("wait-6", "redis-6"),
		wait("wait-7", "redis-7"),
		meet("meet-4", "redis-0", "redis-4"),
		meet("meet-5", "redis-0", "redis-5"),
		meet("meet-6", "redis-0", "redis-6"),
		meet("meet-7", "redis-0", "redis-7"),
		replicate("replicate-5", "redis-4", "redis-5"),
		replicate("replicate-7", "redis-6", "redis-7"),
		migrate("migrate-0", "redis-0", "redis-4", "0-8191"),
		migrate("migrate-2", "redis-2", "redis-6", "8192-16383"),
		forget("forget-1", "redis-1"),
		del("delete-1", "redis-1"),
		forget("forget-3", "redis-3"),
		del("delete-3", "redis-3"),
		forget("forget-0", "redis-0"),
		del("delete-0", "redis-0"),
		forget("forget-2", "redis-2"),
		del("delete-2", "redis-2"),
		verify(2, 1),
	)
	if err := validatePlanWithNodes(p, nodes); err != nil {
		t.Fatalf("expected replacement plan to pass, got %v", err)
	}
}

func TestValidate_RejectsEnsureImageMismatch(t *testing.T) {
	p := planWith(plan.Step{ID: "ensure", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.0", "memorySize": "2Gi"}}, verify(2, 1))
	if err := validatePlanWithNodes(p, nil); err == nil {
		t.Fatal("expected image mismatch to fail")
	}
}

func TestValidate_RejectsEmptyLivePodImage(t *testing.T) {
	nodes := observedFromTopology(topology())
	nodes[0].Image = ""
	if err := validatePlanWithNodes(planWith(verify(2, 1)), nodes); err == nil {
		t.Fatal("expected empty image to fail")
	}
}

func TestValidate_GhostWithoutPodImageStillRequiresForget(t *testing.T) {
	nodes := append(observedFromTopology(topology()), plan.ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}})
	if err := validatePlanWithNodes(planWith(verify(2, 1)), nodes); err == nil {
		t.Fatal("expected unforgiven ghost to fail")
	}
	if err := validatePlanWithNodes(planWith(plan.Step{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "lastKnownNodeId": "node-4"}}, verify(2, 1)), nodes); err != nil {
		t.Fatalf("expected forgotten ghost to pass, got %v", err)
	}
}

func TestObservedNodeHealthy(t *testing.T) {
	base := plan.ObservedNode{Ready: true, PodExists: true, RedisSeen: true, LinkState: "connected"}
	cases := []struct {
		name string
		edit func(*plan.ObservedNode)
		want bool
	}{
		{name: "ready connected", want: true},
		{name: "fail", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }},
		{name: "fail?", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail?"} }},
		{name: "handshake", edit: func(n *plan.ObservedNode) { n.Flags = []string{"handshake"} }},
		{name: "noaddr", edit: func(n *plan.ObservedNode) { n.Flags = []string{"noaddr"} }},
		{name: "disconnected", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "deleting", edit: func(n *plan.ObservedNode) { n.Deleting = true }},
		{name: "not ready", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base
			if tc.edit != nil {
				tc.edit(&n)
			}
			if got := observor.ObservedNodeHealthy(n); got != tc.want {
				t.Fatalf("ObservedNodeHealthy() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestObservedNodeForgettableGhost(t *testing.T) {
	base := plan.ObservedNode{PodExists: true, RedisSeen: true, Ready: true}
	cases := []struct {
		name string
		edit func(*plan.ObservedNode)
		want bool
	}{
		{name: "pod gone", edit: func(n *plan.ObservedNode) { n.PodExists = false }, want: true},
		{name: "deleting", edit: func(n *plan.ObservedNode) { n.Deleting = true }, want: true},
		{name: "fail", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }, want: true},
		{name: "fail?", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail?"} }, want: true},
		{name: "handshake", edit: func(n *plan.ObservedNode) { n.Flags = []string{"handshake"} }, want: true},
		{name: "noaddr", edit: func(n *plan.ObservedNode) { n.Flags = []string{"noaddr"} }, want: true},
		{name: "disconnected", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }, want: true},
		{name: "slot owner", edit: func(n *plan.ObservedNode) { n.PodExists = false; n.Slots = "0-1" }},
		{name: "not ready only", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base
			if tc.edit != nil {
				tc.edit(&n)
			}
			if got := observor.ObservedNodeForgettableGhost(n); got != tc.want {
				t.Fatalf("ObservedNodeForgettableGhost() = %v, want %v", got, tc.want)
			}
		})
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
