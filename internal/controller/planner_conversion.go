package controller

import (
	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
)

func toClusterSpec(c *v1alpha1.RedisCluster) plan.ClusterSpec {
	return plan.ClusterSpec{
		Name:             c.Name,
		Generation:       c.Generation,
		Shards:           c.Spec.Shards,
		ReplicasPerShard: c.Spec.ReplicasPerShard,
		Image:            c.Spec.Image,
		MemorySize:       c.Spec.MemorySize,
	}
}

func toPlannerRequest(c *v1alpha1.RedisCluster, spec plan.ClusterSpec) planner.Request {
	return planner.Request{
		Spec: spec,
		ObservedState: planner.ObservedState{
			ActivePlan:     c.Status.ActivePlan,
			NextPodOrdinal: int(c.Status.NextPodOrdinal),
		},
	}
}

func apiObservedNodes(nodes []plan.ObservedNode) []v1alpha1.ObservedNode {
	out := make([]v1alpha1.ObservedNode, len(nodes))
	for i, n := range nodes {
		out[i] = v1alpha1.ObservedNode{
			Pod: n.Pod, PodExists: n.PodExists, Image: n.Image, RedisSeen: n.RedisSeen, NodeID: n.NodeID, Role: n.Role,
			Slots: n.Slots, MasterID: n.MasterID, MasterPod: n.MasterPod, Ready: n.Ready, Deleting: n.Deleting,
			Flags: append([]string{}, n.Flags...), LinkState: n.LinkState,
		}
	}
	return out
}
