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

// 严格判断 live Redis 视角是否已经满足 spec；true 表示 planner 不应再生成 plan。
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

// status 中的 observed nodes 是上一轮快照；转换后复用 live-state 匹配逻辑。
func observedStatusNodesMatchSpec(nodes []v1alpha1.ObservedNode, spec plan.ClusterSpec) bool {
	if len(nodes) == 0 {
		return false
	}
	out := make([]plan.ObservedNode, len(nodes))
	for i, n := range nodes {
		out[i] = plan.ObservedNode{
			Pod: n.Pod, PodExists: n.PodExists, RedisSeen: n.RedisSeen, NodeID: n.NodeID, Role: n.Role,
			Slots: n.Slots, MasterID: n.MasterID, MasterPod: n.MasterPod, Ready: n.Ready, Deleting: n.Deleting,
			Flags: append([]string{}, n.Flags...), LinkState: n.LinkState,
		}
	}
	return observedNodesMatchSpec(out, spec)
}

// 稳定性只比较有意义的字段，忽略列表顺序和 flags 顺序。
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

// 返回非空原因时，planner 前必须等待；超时后才把这个坏状态交给 repair plan。
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

// 等待窗口从 WaitingForClusterStable condition 的 transition time 开始计时。
func clusterStableWaitTimedOut(c *v1alpha1.RedisCluster) bool {
	for _, cond := range c.Status.Conditions {
		if cond.Type == ConditionPlanned && cond.Reason == "WaitingForClusterStable" {
			return time.Since(cond.LastTransitionTime.Time) >= verifyStableTimeout
		}
	}
	return false
}

// 空观测需要特殊处理：第一次空观测先等，第二次仍为空才认为“稳定为空”。
func clusterStableWaitStarted(c *v1alpha1.RedisCluster) bool {
	for _, cond := range c.Status.Conditions {
		if cond.Type == ConditionPlanned && cond.Reason == "WaitingForClusterStable" {
			return true
		}
	}
	return false
}

// 少一个健康 master 且旧 slot master 已 fail/deleting 时，优先等 Redis 自己 failover。
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
