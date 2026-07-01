package validator

import (
	"strings"
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func TestSimulatePlan_AcceptsMigrationToNewShard(t *testing.T) {
	p := planWith(
		ensure("ensure-4", "redis-4"),
		ensure("ensure-5", "redis-5"),
		wait("wait-4", "redis-4"),
		wait("wait-5", "redis-5"),
		meet("meet-4", "redis-0", "redis-4"),
		meet("meet-5", "redis-0", "redis-5"),
		replicate("replicate-5", "redis-4", "redis-5"),
		migrate("migrate", "redis-0", "redis-4", "0-1"),
		verify(3, 1),
	)
	if err := simulatePlan(testSpec(), observedFromTopology(topology()), p); err != nil {
		t.Fatalf("expected migration to pass, got %v", err)
	}
}

func TestSimulatePlan_MapsReplicaByMasterIDBeforeMaster(t *testing.T) {
	nodes := []plan.ObservedNode{
		{Pod: "redis-1", PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: "node-1", Role: "replica", MasterID: "node-0", Ready: true},
		{Pod: "redis-0", PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: "node-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-3", PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: "node-3", Role: "replica", MasterID: "node-2", Ready: true},
		{Pod: "redis-2", PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: "node-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	if err := simulatePlan(testSpec(), nodes, planWith(verify(2, 1))); err != nil {
		t.Fatalf("expected masterID-only replicas before masters to pass, got %v", err)
	}
}

func TestSimulatePlan_RejectsUnsafeTransitions(t *testing.T) {
	tests := []struct {
		name string
		plan *plan.Plan
		want string
		ctx  func() []plan.ObservedNode
	}{
		{
			name: "wait before ensure",
			plan: planWith(wait("wait", "redis-4")),
			want: "was not declared",
		},
		{
			name: "meet before ready",
			plan: planWith(ensure("ensure", "redis-4"), meet("meet", "redis-0", "redis-4")),
			want: "is not ready",
		},
		{
			name: "add slots before replica",
			plan: func() *plan.Plan {
				p := validCreatePlan()
				p.Steps[13], p.Steps[11] = p.Steps[11], p.Steps[13]
				return p
			}(),
			want: "has no replica",
			ctx:  func() []plan.ObservedNode { return nil },
		},
		{
			name: "replicate slot owner",
			plan: planWith(replicate("replicate", "redis-2", "redis-0")),
			want: "already owns slots",
		},
		{
			name: "migrate wrong source",
			plan: planWith(
				ensure("ensure-4", "redis-4"),
				ensure("ensure-5", "redis-5"),
				wait("wait-4", "redis-4"),
				wait("wait-5", "redis-5"),
				meet("meet-4", "redis-0", "redis-4"),
				meet("meet-5", "redis-0", "redis-5"),
				replicate("replicate-5", "redis-4", "redis-5"),
				migrate("migrate", "redis-0", "redis-4", "8192"),
			),
			want: "belongs to pod",
		},
		{
			name: "forget slot owner",
			plan: planWith(forget("forget", "redis-0")),
			want: "still owns slots",
		},
		{
			name: "delete active member",
			plan: planWith(del("delete", "redis-1")),
			want: "still an active cluster member",
		},
		{
			name: "verify empty master",
			plan: planWith(
				ensure("ensure", "redis-4"),
				wait("wait", "redis-4"),
				meet("meet", "redis-0", "redis-4"),
				verify(2, 1),
			),
			want: "owns no slots",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := observedFromTopology(topology())
			if tt.ctx != nil {
				nodes = tt.ctx()
			}
			err := simulatePlan(testSpec(), nodes, tt.plan)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}
