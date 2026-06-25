package controller

import (
	"context"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

func (r *RedisClusterReconciler) shouldRefreshTopology(ctx context.Context, c *v1alpha1.RedisCluster) bool {
	if c.Status.Topology == nil {
		return false
	}
	active := c.Status.ActivePlan
	if active != nil && planState(active) == plan.PlanStateRunning {
		return false
	}
	if r.podSetDrifted(ctx, c) {
		return true
	}
	elapsed := time.Since(c.Status.TopologyObservedAt.Time)
	return elapsed >= r.TopologyStaleThreshold
}

func (r *RedisClusterReconciler) podSetDrifted(ctx context.Context, c *v1alpha1.RedisCluster) bool {
	if c.Status.Topology == nil {
		return false
	}
	selector, err := labels.Parse(labelCluster + "=" + c.Name)
	if err != nil {
		return false
	}
	var podList corev1.PodList
	if err := r.List(ctx, &podList, &client.ListOptions{
		Namespace:     c.Name,
		LabelSelector: selector,
	}); err != nil {
		return false
	}
	live := livePodSignature(podList.Items)
	want := topologyPodSignature(c.Status.Topology)
	return !maps.Equal(live, want)
}

func livePodSignature(pods []corev1.Pod) map[string]bool {
	out := map[string]bool{}
	for i := range pods {
		out[pods[i].Name] = podReady(&pods[i])
	}
	return out
}

func topologyPodSignature(topo *v1alpha1.ClusterTopology) map[string]bool {
	out := map[string]bool{}
	if topo == nil {
		return out
	}
	for i := range topo.Shards {
		sh := &topo.Shards[i]
		if sh.Master.Pod != "" {
			out[sh.Master.Pod] = sh.Master.Ready
		}
		for j := range sh.Replicas {
			r := &sh.Replicas[j]
			if r.Pod != "" {
				out[r.Pod] = r.Ready
			}
		}
	}
	return out
}

func topologyMatchesSpec(topology *v1alpha1.ClusterTopology, spec plan.ClusterSpec) bool {
	if topology == nil || len(topology.Shards) == 0 {
		return false
	}
	if len(topology.Shards) != int(spec.Shards) {
		return false
	}
	for _, sh := range topology.Shards {
		if len(sh.Replicas) != int(spec.ReplicasPerShard) {
			return false
		}
		if sh.Master.Slots == "" {
			return false
		}
		if !sh.Master.Ready {
			return false
		}
		for _, r := range sh.Replicas {
			if !r.Ready {
				return false
			}
		}
	}
	return true
}

func observedNodesMatchSpec(nodes []plan.ObservedNode, spec plan.ClusterSpec) bool {
	if len(nodes) == 0 || spec.Shards <= 0 || spec.ReplicasPerShard < 0 {
		return false
	}
	nodeIDToPod := map[string]string{}
	for _, n := range nodes {
		if n.NodeID != "" && n.Pod != "" {
			nodeIDToPod[n.NodeID] = n.Pod
		}
	}
	shards := map[string]int{}
	covered := map[int]string{}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "master" || n.Slots == "" {
			continue
		}
		if n.Pod == "" || !n.Ready || n.Deleting {
			return false
		}
		if _, ok := shards[n.Pod]; ok {
			return false
		}
		slots, err := rediscluster.ParseSlotSpec(n.Slots)
		if err != nil || len(slots) == 0 {
			return false
		}
		for _, slot := range slots {
			if _, ok := covered[slot]; ok {
				return false
			}
			covered[slot] = n.Pod
		}
		shards[n.Pod] = 0
	}
	if len(shards) != int(spec.Shards) || len(covered) != 16384 {
		return false
	}
	for _, n := range nodes {
		if !n.PodExists || !n.RedisSeen || n.Role != "replica" {
			continue
		}
		masterPod := n.MasterPod
		if masterPod == "" && n.MasterID != "" {
			masterPod = nodeIDToPod[n.MasterID]
		}
		if _, ok := shards[masterPod]; !ok {
			continue
		}
		if n.Pod == "" || !n.Ready || n.Deleting {
			return false
		}
		shards[masterPod]++
	}
	for _, replicas := range shards {
		if replicas != int(spec.ReplicasPerShard) {
			return false
		}
	}
	return true
}
