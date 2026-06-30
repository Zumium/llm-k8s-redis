package planner

import (
	"context"
	"errors"
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

type stubPlanner struct {
	called bool
	plan   *plan.Plan
	err    error
}

func (p *stubPlanner) Plan(_ context.Context, _ Request) (*plan.Plan, error) {
	p.called = true
	return p.plan, p.err
}

func TestGoPlannerCreatesInitialCluster(t *testing.T) {
	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{Spec: sampleSpec()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "go-create-3" || len(got.Steps) != 16 {
		t.Fatalf("plan = %#v", got)
	}
	if err := plan.NewValidator().Validate(got, sampleSpec()); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerCreateVariants(t *testing.T) {
	cases := []struct {
		name       string
		shards     int32
		replicas   int32
		wantSteps  int
		wantRanges []string
	}{
		{name: "one shard", shards: 1, replicas: 1, wantSteps: 8, wantRanges: []string{"0-16383"}},
		{name: "two shards", shards: 2, replicas: 1, wantSteps: 16, wantRanges: []string{"0-8191", "8192-16383"}},
		{name: "three shards", shards: 3, replicas: 1, wantSteps: 24, wantRanges: []string{"0-5460", "5461-10921", "10922-16383"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := sampleSpec()
			spec.Shards = tc.shards
			spec.ReplicasPerShard = tc.replicas

			got, err := NewGoPlanner(nil).Plan(context.Background(), Request{Spec: spec})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.PlanID != "go-create-3" || got.TargetGeneration != spec.Generation || len(got.Steps) != tc.wantSteps {
				t.Fatalf("plan = %#v", got)
			}
			if got.Steps[len(got.Steps)-1].Action != plan.ActionVerifyCluster {
				t.Fatalf("last step = %#v", got.Steps[len(got.Steps)-1])
			}
			if got.Steps[0].ID != "ensure-redis-0" {
				t.Fatalf("first step = %#v", got.Steps[0])
			}
			gotRanges := addSlotRanges(got)
			if !sameStrings(gotRanges, tc.wantRanges) {
				t.Fatalf("slot ranges = %#v, want %#v", gotRanges, tc.wantRanges)
			}
			if err := plan.NewValidator().Validate(got, spec); err != nil {
				t.Fatalf("generated plan did not validate: %v", err)
			}
		})
	}
}

func TestGoPlannerScalesReplicas(t *testing.T) {
	spec := sampleSpec()
	spec.ReplicasPerShard = 2
	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec: spec,
		ObservedState: ObservedState{
			NextPodOrdinal: 4,
			Nodes:          healthyObservedNodes(),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "go-replica-scaleout-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
	if got.Steps[0].ID != "ensure-redis-4" {
		t.Fatalf("first step = %#v", got.Steps[0])
	}
	if got.Steps[1].ID != "ensure-redis-5" || got.Steps[2].ID != "wait-redis-4" || got.Steps[3].ID != "wait-redis-5" {
		t.Fatalf("replica provisioning is not grouped: %#v", got.Steps[:4])
	}
	if fallback := firstAction(got, plan.ActionMeetNode); fallback.Params["sourcePod"] != "redis-0" || fallback.Params["targetPod"] != "redis-4" {
		t.Fatalf("first meet = %#v", fallback)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 4, ObservedNodes: healthyObservedNodes()}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerScalesReplicasIn(t *testing.T) {
	fallback := &stubPlanner{err: errors.New("fallback called")}
	nodes := overProvisionedObservedNodes()

	got, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 6, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fallback.called {
		t.Fatal("fallback should not be called")
	}
	if got.PlanID != "go-replica-scalein-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
	if len(got.Steps) != 5 {
		t.Fatalf("steps = %#v", got.Steps)
	}
	if got.Steps[0].ID != "forget-redis-4" || got.Steps[1].ID != "delete-redis-4" || got.Steps[2].ID != "forget-redis-5" || got.Steps[3].ID != "delete-redis-5" {
		t.Fatalf("scale-in steps = %#v", got.Steps[:4])
	}
	if got.Steps[0].Params["lastKnownNodeId"] != "node-4" {
		t.Fatalf("forget params = %#v", got.Steps[0].Params)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 6, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerScalesShardsOut(t *testing.T) {
	fallback := &stubPlanner{err: errors.New("fallback called")}
	spec := withShards(sampleSpec(), 3)
	nodes := healthyObservedNodes()

	got, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          spec,
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fallback.called {
		t.Fatal("fallback should not be called")
	}
	if got.PlanID != "go-shard-scaleout-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
	if got.Steps[0].ID != "ensure-redis-4" || got.Steps[1].ID != "ensure-redis-5" {
		t.Fatalf("new pods = %#v", got.Steps[:2])
	}
	if !hasMigration(got, "redis-0", "redis-2", "5462-8191") || !hasMigration(got, "redis-2", "redis-4", "10923-16383") {
		t.Fatalf("migrations = %#v", migrationStepsOf(got))
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 4, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerScalesShardsIn(t *testing.T) {
	fallback := &stubPlanner{err: errors.New("fallback called")}
	spec := withShards(sampleSpec(), 2)
	nodes := threeShardObservedNodes()

	got, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          spec,
		ObservedState: ObservedState{NextPodOrdinal: 6, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fallback.called {
		t.Fatal("fallback should not be called")
	}
	if got.PlanID != "go-shard-scalein-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
	if got.Steps[0].ID != "ensure-redis-6" || got.Steps[3].ID != "ensure-redis-9" {
		t.Fatalf("new pods = %#v", got.Steps[:4])
	}
	if !hasMigration(got, "redis-4", "redis-8", "10923-16383") {
		t.Fatalf("migrations = %#v", migrationStepsOf(got))
	}
	if got.Steps[len(got.Steps)-2].ID != "delete-redis-5" {
		t.Fatalf("old pods not deleted before verify: %#v", got.Steps[len(got.Steps)-6:])
	}
	for _, s := range got.Steps {
		if s.Action != plan.ActionForgetNode {
			continue
		}
		pod := s.Params["pod"].(string)
		if s.Params["lastKnownNodeId"] != "node-"+pod[len("redis-"):] {
			t.Fatalf("forget step missing lastKnownNodeId: %#v", s)
		}
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 6, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerRepairsBeforeTransform(t *testing.T) {
	spec := sampleSpec()
	spec.ReplicasPerShard = 2
	nodes := append(healthyObservedNodes(), ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}})
	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec: spec,
		ObservedState: ObservedState{
			NextPodOrdinal: 5,
			Nodes:          nodes,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Steps[0].Action != plan.ActionForgetNode || got.Steps[1].Action != plan.ActionDeleteNode {
		t.Fatalf("repair is not first: %#v", got.Steps[:2])
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 5, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerRepairsNonUniformReplicas(t *testing.T) {
	nodes := healthyObservedNodes()
	nodes = nodes[:3]

	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec: sampleSpec(),
		ObservedState: ObservedState{
			NextPodOrdinal: 4,
			Nodes:          nodes,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "go-repair-3" || got.Steps[0].ID != "ensure-redis-4" {
		t.Fatalf("plan = %#v", got)
	}
	replicate := firstAction(got, plan.ActionReplicateNode)
	if replicate.Params["masterPod"] != "redis-2" || replicate.Params["replicaPod"] != "redis-4" {
		t.Fatalf("replicate = %#v", replicate)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 4, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerRepairsThreeShardMixedReplicaCounts(t *testing.T) {
	spec := withShards(withReplicas(sampleSpec(), 3), 3)
	nodes := threeShardMixedReplicaObservedNodes()
	fallback := &stubPlanner{err: errors.New("fallback called")}

	got, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          spec,
		ObservedState: ObservedState{NextPodOrdinal: 10, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fallback.called {
		t.Fatal("fallback should not be called")
	}
	if got.PlanID != "go-repair-3" || got.Steps[0].ID != "ensure-redis-10" {
		t.Fatalf("plan = %#v", got)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 10, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerRetriesDeterministicPlanWithValidationFeedback(t *testing.T) {
	nodes := healthyObservedNodes()[:3]
	fallback := &stubPlanner{err: errors.New("fallback called")}

	got, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
		ValidationFeedback: []ValidationFeedback{{
			Error: "previous LLM plan was rejected",
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fallback.called {
		t.Fatal("fallback should not be called")
	}
	if got.PlanID != "go-repair-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
}

func TestGoPlannerCleansPureGhost(t *testing.T) {
	nodes := append(healthyObservedNodes(), ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}})
	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Steps) != 3 {
		t.Fatalf("steps = %#v", got.Steps)
	}
	if got.Steps[0].Action != plan.ActionForgetNode || got.Steps[1].Action != plan.ActionDeleteNode || got.Steps[2].Action != plan.ActionVerifyCluster {
		t.Fatalf("steps = %#v", got.Steps)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 5, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerGhostForgetParams(t *testing.T) {
	cases := []struct {
		name          string
		ghost         ObservedNode
		wantLastKnown bool
		wantPod       bool
	}{
		{name: "missing pod", ghost: ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}}, wantLastKnown: true, wantPod: true},
		{name: "existing failed pod", ghost: ObservedNode{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail?"}}, wantLastKnown: true, wantPod: true},
		{name: "unmapped missing pod", ghost: ObservedNode{PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}}, wantLastKnown: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := append(healthyObservedNodes(), tc.ghost)
			got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
				Spec:          sampleSpec(),
				ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: nodes},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			forget := firstAction(got, plan.ActionForgetNode)
			_, hasLastKnown := forget.Params["lastKnownNodeId"]
			_, hasPod := forget.Params["pod"]
			if hasLastKnown != tc.wantLastKnown {
				t.Fatalf("lastKnownNodeId present = %v, want %v in %#v", hasLastKnown, tc.wantLastKnown, forget)
			}
			if hasPod != tc.wantPod {
				t.Fatalf("pod present = %v, want %v in %#v", hasPod, tc.wantPod, forget)
			}
			if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 5, ObservedNodes: nodes}); err != nil {
				t.Fatalf("generated plan did not validate: %v", err)
			}
		})
	}
}

func TestGoPlannerGhostForgetAlwaysUsesLastKnownNodeID(t *testing.T) {
	nodes := append(healthyObservedNodes(), ObservedNode{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail?"}})

	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	forget := firstAction(got, plan.ActionForgetNode)
	if forget.Params["lastKnownNodeId"] != "node-4" {
		t.Fatalf("forget params = %#v", forget.Params)
	}
	if firstAction(got, plan.ActionDeleteNode).Action != "" {
		t.Fatalf("delete step generated: %#v", got.Steps)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 5, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerDoesNotDeleteReusedGhostPodName(t *testing.T) {
	nodes := append(healthyObservedNodes(),
		ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "old-node-4", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}},
		ObservedNode{Pod: "redis-4", PodExists: true},
	)

	req := Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: nodes},
	}
	got := buildRepairPlan(req, topologyFromObserved(nodes))
	if got == nil {
		t.Fatal("repair plan is nil")
	}
	forget := firstAction(got, plan.ActionForgetNode)
	if forget.Params["lastKnownNodeId"] != "old-node-4" {
		t.Fatalf("forget params = %#v", forget.Params)
	}
	if firstAction(got, plan.ActionDeleteNode).Action != "" {
		t.Fatalf("delete step generated: %#v", got.Steps)
	}
}

func TestGoPlannerReplacesDisconnectedReplica(t *testing.T) {
	nodes := withReplicaLinkState(healthyObservedNodes(), "redis-1", "disconnected")
	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "go-repair-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
	if forget := firstAction(got, plan.ActionForgetNode); forget.Params["lastKnownNodeId"] != "node-1" {
		t.Fatalf("forget = %#v", forget)
	}
	if ensure := firstAction(got, plan.ActionEnsureNode); ensure.Params["pod"] != "redis-4" {
		t.Fatalf("ensure = %#v", ensure)
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: sampleSpec(), NextPodOrdinal: 4, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerFallsBackForNotReadyOnlyReplica(t *testing.T) {
	nodes := healthyObservedNodes()
	nodes[1].Ready = false
	fallbackErr := errors.New("fallback called")
	fallback := &stubPlanner{err: fallbackErr}
	_, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec:          sampleSpec(),
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
	})
	if !errors.Is(err, fallbackErr) || !fallback.called {
		t.Fatalf("fallback err=%v called=%v", err, fallback.called)
	}
}

func TestGoPlannerUsesStatusNextPodOrdinal(t *testing.T) {
	spec := sampleSpec()
	spec.ReplicasPerShard = 2
	nodes := healthyObservedNodes()
	nodes = append(nodes, ObservedNode{Pod: "redis-9"})

	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          spec,
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Steps[0].ID != "ensure-redis-4" {
		t.Fatalf("first step = %#v", got.Steps[0])
	}
	if err := plan.NewValidator().Validate(got, plan.ValidationContext{Spec: spec, NextPodOrdinal: 4, ObservedNodes: nodes}); err != nil {
		t.Fatalf("generated plan did not validate: %v", err)
	}
}

func TestGoPlannerMapsReplicaByMasterID(t *testing.T) {
	spec := sampleSpec()
	spec.ReplicasPerShard = 2
	nodes := healthyObservedNodes()
	nodes[1].MasterPod = ""
	nodes[3].MasterPod = ""

	got, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          spec,
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: nodes},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "go-replica-scaleout-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}
}

func TestGoPlannerFallsBackForFailedExistingMaster(t *testing.T) {
	for _, tc := range []struct {
		name  string
		patch func([]ObservedNode) []ObservedNode
	}{
		{name: "fail flag", patch: withMasterFlags("redis-0", []string{"master", "fail"})},
		{name: "disconnected", patch: withMasterLinkState("redis-0", "disconnected")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fallbackErr := errors.New("fallback called")
			fallback := &stubPlanner{err: fallbackErr}
			_, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
				Spec:          withReplicas(sampleSpec(), 2),
				ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: tc.patch(healthyObservedNodes())},
			})
			if !errors.Is(err, fallbackErr) || !fallback.called {
				t.Fatalf("fallback err=%v called=%v", err, fallback.called)
			}
		})
	}
}

func TestGoPlannerFallsBackForUnsupportedPlan(t *testing.T) {
	fallbackErr := errors.New("fallback called")
	fallback := &stubPlanner{err: fallbackErr}
	_, err := NewGoPlanner(fallback).Plan(context.Background(), Request{
		Spec: plan.ClusterSpec{Name: "example", Generation: 3, Shards: 2, ReplicasPerShard: 0, Image: "redis:7.2", MemorySize: "2Gi"},
	})
	if !errors.Is(err, fallbackErr) || !fallback.called {
		t.Fatalf("fallback err=%v called=%v", err, fallback.called)
	}
}

func TestGoPlannerFallbackCases(t *testing.T) {
	cases := []struct {
		name string
		req  Request
	}{
		{name: "unsupported with validation feedback", req: Request{Spec: withShards(withReplicas(sampleSpec(), 2), 3), ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: healthyObservedNodes()}, ValidationFeedback: []ValidationFeedback{{Error: "try again"}}}},
		{name: "replica scalein", req: Request{Spec: withReplicas(sampleSpec(), 0), ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: healthyObservedNodes()}}},
		{name: "unready master", req: Request{Spec: withReplicas(sampleSpec(), 2), ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: withMasterReady(healthyObservedNodes(), "redis-0", false)}}},
		{name: "unready deficient master", req: Request{Spec: sampleSpec(), ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: withMasterReady(healthyObservedNodes()[:3], "redis-2", false)}}},
		{name: "missing master", req: Request{Spec: sampleSpec(), ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: healthyObservedNodes()[:2]}}},
		{name: "ghost without identifier", req: Request{Spec: sampleSpec(), ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: append(healthyObservedNodes(), ObservedNode{PodExists: false, RedisSeen: true, Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}})}}},
		{name: "slot owning failed node", req: Request{Spec: sampleSpec(), ObservedState: ObservedState{NextPodOrdinal: 5, Nodes: append(healthyObservedNodes(), ObservedNode{Pod: "redis-4", PodExists: false, RedisSeen: true, NodeID: "node-4", Role: "master", Slots: "1", Flags: []string{"fail"}})}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fallbackErr := errors.New("fallback called")
			fallback := &stubPlanner{err: fallbackErr}
			_, err := NewGoPlanner(fallback).Plan(context.Background(), tc.req)
			if !errors.Is(err, fallbackErr) || !fallback.called {
				t.Fatalf("fallback err=%v called=%v", err, fallback.called)
			}
		})
	}
}

func TestGoPlannerUnsupportedWithoutFallback(t *testing.T) {
	_, err := NewGoPlanner(nil).Plan(context.Background(), Request{
		Spec:          withReplicas(sampleSpec(), 0),
		ObservedState: ObservedState{NextPodOrdinal: 4, Nodes: healthyObservedNodes()},
	})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v", err)
	}
}

func healthyObservedNodes() []ObservedNode {
	return []ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "node-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "node-1", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "node-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "node-3", Role: "replica", MasterID: "node-2", MasterPod: "redis-2", Ready: true},
	}
}

func overProvisionedObservedNodes() []ObservedNode {
	return append(healthyObservedNodes(),
		ObservedNode{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		ObservedNode{Pod: "redis-5", PodExists: true, RedisSeen: true, NodeID: "node-5", Role: "replica", MasterID: "node-2", MasterPod: "redis-2", Ready: true},
	)
}

func threeShardObservedNodes() []ObservedNode {
	return []ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "node-0", Role: "master", Slots: "0-5461", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "node-1", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "node-2", Role: "master", Slots: "5462-10922", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "node-3", Role: "replica", MasterID: "node-2", MasterPod: "redis-2", Ready: true},
		{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "node-4", Role: "master", Slots: "10923-16383", Ready: true},
		{Pod: "redis-5", PodExists: true, RedisSeen: true, NodeID: "node-5", Role: "replica", MasterID: "node-4", MasterPod: "redis-4", Ready: true},
	}
}

func threeShardMixedReplicaObservedNodes() []ObservedNode {
	return []ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "node-0", Role: "master", Slots: "0-5461", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "node-1", Role: "master", Slots: "5462-10922", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "node-2", Role: "master", Slots: "10923-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "node-3", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "node-4", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-5", PodExists: true, RedisSeen: true, NodeID: "node-5", Role: "replica", MasterID: "node-1", MasterPod: "redis-1", Ready: true},
		{Pod: "redis-6", PodExists: true, RedisSeen: true, NodeID: "node-6", Role: "replica", MasterID: "node-1", MasterPod: "redis-1", Ready: true},
		{Pod: "redis-7", PodExists: true, RedisSeen: true, NodeID: "node-7", Role: "replica", MasterID: "node-1", MasterPod: "redis-1", Ready: true},
		{Pod: "redis-8", PodExists: true, RedisSeen: true, NodeID: "node-8", Role: "replica", MasterID: "node-2", MasterPod: "redis-2", Ready: true},
		{Pod: "redis-9", PodExists: true, RedisSeen: true, NodeID: "node-9", Role: "replica", MasterID: "node-2", MasterPod: "redis-2", Ready: true},
	}
}

func addSlotRanges(p *plan.Plan) []string {
	var out []string
	for _, s := range p.Steps {
		if s.Action == plan.ActionAddSlots {
			out = append(out, s.Params["slots"].(string))
		}
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func firstAction(p *plan.Plan, action plan.ActionType) plan.Step {
	for _, s := range p.Steps {
		if s.Action == action {
			return s
		}
	}
	return plan.Step{}
}

func hasMigration(p *plan.Plan, source, target, slots string) bool {
	for _, s := range p.Steps {
		if s.Action == plan.ActionMigrateSlots && s.Params["sourcePod"] == source && s.Params["targetPod"] == target && s.Params["slots"] == slots {
			return true
		}
	}
	return false
}

func migrationStepsOf(p *plan.Plan) []plan.Step {
	var out []plan.Step
	for _, s := range p.Steps {
		if s.Action == plan.ActionMigrateSlots {
			out = append(out, s)
		}
	}
	return out
}

func withShards(spec plan.ClusterSpec, shards int32) plan.ClusterSpec {
	spec.Shards = shards
	return spec
}

func withReplicas(spec plan.ClusterSpec, replicas int32) plan.ClusterSpec {
	spec.ReplicasPerShard = replicas
	return spec
}

func withMasterReady(nodes []ObservedNode, pod string, ready bool) []ObservedNode {
	out := append([]ObservedNode(nil), nodes...)
	for i := range out {
		if out[i].Pod == pod {
			out[i].Ready = ready
		}
	}
	return out
}

func withMasterFlags(pod string, flags []string) func([]ObservedNode) []ObservedNode {
	return func(nodes []ObservedNode) []ObservedNode {
		out := append([]ObservedNode(nil), nodes...)
		for i := range out {
			if out[i].Pod == pod {
				out[i].Flags = flags
			}
		}
		return out
	}
}

func withMasterLinkState(pod, state string) func([]ObservedNode) []ObservedNode {
	return func(nodes []ObservedNode) []ObservedNode {
		out := append([]ObservedNode(nil), nodes...)
		for i := range out {
			if out[i].Pod == pod {
				out[i].LinkState = state
			}
		}
		return out
	}
}

func withReplicaLinkState(nodes []ObservedNode, pod, state string) []ObservedNode {
	out := append([]ObservedNode(nil), nodes...)
	for i := range out {
		if out[i].Pod == pod {
			out[i].LinkState = state
		}
	}
	return out
}
