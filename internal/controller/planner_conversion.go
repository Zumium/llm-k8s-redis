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

func validationContext(c *v1alpha1.RedisCluster, spec plan.ClusterSpec, nodes []plan.ObservedNode) plan.ValidationContext {
	return plan.ValidationContext{
		Spec:           spec,
		Topology:       toPlanTopology(c.Status.Topology),
		NextPodOrdinal: int(c.Status.NextPodOrdinal),
		ObservedNodes:  nodes,
	}
}

func toPlanTopology(t *v1alpha1.ClusterTopology) *plan.ClusterTopology {
	if t == nil {
		return nil
	}
	out := &plan.ClusterTopology{Shards: make([]plan.ShardTopology, 0, len(t.Shards))}
	for _, sh := range t.Shards {
		ps := plan.ShardTopology{
			ID:     sh.ID,
			Master: toPlanNode(sh.Master),
		}
		for _, r := range sh.Replicas {
			ps.Replicas = append(ps.Replicas, toPlanNode(r))
		}
		out.Shards = append(out.Shards, ps)
	}
	return out
}

func toPlanNode(n v1alpha1.NodeTopology) plan.NodeTopology {
	return plan.NodeTopology{
		Pod:    n.Pod,
		NodeID: n.NodeID,
		Slots:  n.Slots,
		Ready:  n.Ready,
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
