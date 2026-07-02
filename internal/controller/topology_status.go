package controller

import (
	"reflect"
	"slices"
	"strings"
	"time"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/observor"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

const verifyStableTimeout = 2 * time.Minute

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
		if n.PodExists && n.Image != spec.Image {
			return false
		}
		if !n.PodExists || !n.RedisSeen || n.Role != "master" || n.Slots == "" {
			continue
		}
		if n.Pod == "" || !observor.ObservedNodeHealthy(n) {
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
		if n.Pod == "" || !observor.ObservedNodeHealthy(n) {
			return false
		}
		shards[masterPod]++
	}
	for _, n := range nodes {
		if !n.PodExists && !n.RedisSeen {
			continue
		}
		if n.Role == "master" && n.Slots != "" {
			if _, ok := shards[n.Pod]; ok {
				continue
			}
		}
		if n.Role == "replica" {
			masterPod := n.MasterPod
			if masterPod == "" && n.MasterID != "" {
				masterPod = nodeIDToPod[n.MasterID]
			}
			if _, ok := shards[masterPod]; ok {
				continue
			}
		}
		return false
	}
	for _, replicas := range shards {
		if replicas != int(spec.ReplicasPerShard) {
			return false
		}
	}
	return true
}

func observedStatusNodesEqual(a, b []v1alpha1.ObservedNode) bool {
	a = normalizedObservedStatusNodes(a)
	b = normalizedObservedStatusNodes(b)
	return reflect.DeepEqual(a, b)
}

func normalizedObservedStatusNodes(nodes []v1alpha1.ObservedNode) []v1alpha1.ObservedNode {
	out := slices.Clone(nodes)
	for i := range out {
		if len(out[i].Flags) == 0 {
			out[i].Flags = nil
		} else {
			out[i].Flags = slices.Clone(out[i].Flags)
			slices.Sort(out[i].Flags)
		}
	}
	slices.SortFunc(out, func(a, b v1alpha1.ObservedNode) int {
		switch {
		case a.Pod != b.Pod:
			return strings.Compare(a.Pod, b.Pod)
		case a.NodeID != b.NodeID:
			return strings.Compare(a.NodeID, b.NodeID)
		case a.Role != b.Role:
			return strings.Compare(a.Role, b.Role)
		default:
			return strings.Compare(a.MasterID, b.MasterID)
		}
	})
	return out
}

func planningInstabilityReason(nodes []plan.ObservedNode, spec plan.ClusterSpec) string {
	if waitingForRedisFailover(nodes, spec) {
		return "waiting for Redis native failover to promote a replica"
	}
	for _, n := range nodes {
		if n.RedisSeen && n.Role == "master" && strings.ContainsAny(n.Slots, "[]") {
			return "waiting for Redis slot migration/importing state to settle"
		}
		if n.PodExists && n.Deleting {
			return "waiting for deleting pods to settle"
		}
		if n.PodExists && !n.Ready {
			return "waiting for pods to become ready"
		}
		if n.PodExists && !n.RedisSeen {
			return "waiting for Redis nodes to join cluster"
		}
		if n.RedisSeen && n.Role == "unknown" {
			return "waiting for Redis node roles to settle"
		}
		if n.RedisSeen && !observor.ObservedNodeHealthy(n) {
			return "waiting for Redis nodes to become healthy"
		}
	}
	return ""
}

func clusterStableWaitTimedOut(c *v1alpha1.RedisCluster) bool {
	for _, cond := range c.Status.Conditions {
		if cond.Type == ConditionPlanned && cond.Reason == "WaitingForClusterStable" {
			return time.Since(cond.LastTransitionTime.Time) >= verifyStableTimeout
		}
	}
	return false
}

func clusterStableWaitStarted(c *v1alpha1.RedisCluster) bool {
	for _, cond := range c.Status.Conditions {
		if cond.Type == ConditionPlanned && cond.Reason == "WaitingForClusterStable" {
			return true
		}
	}
	return false
}

func waitingForRedisFailover(nodes []plan.ObservedNode, spec plan.ClusterSpec) bool {
	healthyMasters := 0
	failedSlotMaster := false
	for _, n := range nodes {
		if !n.RedisSeen || n.Role != "master" || n.Slots == "" {
			continue
		}
		if !n.PodExists || n.Deleting || hasRedisFailoverFlag(n.Flags) {
			failedSlotMaster = true
			continue
		}
		if n.PodExists {
			healthyMasters++
		}
	}
	return failedSlotMaster && healthyMasters < int(spec.Shards)
}

func hasRedisFailoverFlag(flags []string) bool {
	for _, f := range flags {
		if strings.EqualFold(f, "fail") || strings.EqualFold(f, "fail?") {
			return true
		}
	}
	return false
}
