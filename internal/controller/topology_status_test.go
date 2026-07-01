package controller

import (
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func TestObservedNodesMatchSpecRejectsUnhealthyNodes(t *testing.T) {
	base := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
	}
	spec := toClusterSpec(testCluster())
	if !observedNodesMatchSpec(base, spec) {
		t.Fatal("healthy observed nodes should match spec")
	}
	cases := []struct {
		name string
		pod  string
		edit func(*plan.ObservedNode)
	}{
		{name: "fail", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }},
		{name: "fail?", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail?"} }},
		{name: "handshake", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.Flags = []string{"handshake"} }},
		{name: "noaddr", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Flags = []string{"noaddr"} }},
		{name: "disconnected master", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "disconnected replica", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "not ready", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := append([]plan.ObservedNode(nil), base...)
			for i := range nodes {
				if nodes[i].Pod == tc.pod {
					tc.edit(&nodes[i])
				}
			}
			if observedNodesMatchSpec(nodes, spec) {
				t.Fatal("unhealthy observed nodes should not match spec")
			}
		})
	}
}

func TestWaitingForRedisFailover(t *testing.T) {
	base := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	spec := toClusterSpec(testCluster())
	cases := []struct {
		name string
		edit func(*plan.ObservedNode)
		want bool
	}{
		{name: "pod gone", edit: func(n *plan.ObservedNode) { n.PodExists = false }, want: true},
		{name: "deleting", edit: func(n *plan.ObservedNode) { n.Deleting = true }, want: true},
		{name: "fail", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }, want: true},
		{name: "disconnected", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "not ready", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := append([]plan.ObservedNode(nil), base...)
			tc.edit(&nodes[0])
			if got := waitingForRedisFailover(nodes, spec); got != tc.want {
				t.Fatalf("waitingForRedisFailover() = %v, want %v", got, tc.want)
			}
		})
	}
}
