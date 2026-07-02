package action

import (
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

const ConditionHealthy = "Healthy"

func setCondition(c *v1alpha1.RedisCluster, t string, s metav1.ConditionStatus, reason, msg string) {
	cond := metav1.Condition{
		Type:               t,
		Status:             s,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: c.Generation,
		LastTransitionTime: metav1.Now(),
	}
	for i := range c.Status.Conditions {
		if c.Status.Conditions[i].Type == t {
			if c.Status.Conditions[i].Status == s && c.Status.Conditions[i].Reason == reason && c.Status.Conditions[i].Message == msg {
				cond.LastTransitionTime = c.Status.Conditions[i].LastTransitionTime
			}
			c.Status.Conditions[i] = cond
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, cond)
}

func apiObservedNodes(nodes []plan.ObservedNode) []v1alpha1.ObservedNode {
	out := make([]v1alpha1.ObservedNode, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, v1alpha1.ObservedNode{
			Pod: n.Pod, PodExists: n.PodExists, RedisSeen: n.RedisSeen, NodeID: n.NodeID, Role: n.Role,
			Slots: n.Slots, MasterID: n.MasterID, MasterPod: n.MasterPod, Ready: n.Ready, Deleting: n.Deleting,
			Flags: append([]string{}, n.Flags...), LinkState: n.LinkState,
		})
	}
	return out
}

func bumpNextPodOrdinalFromObserved(c *v1alpha1.RedisCluster, nodes []plan.ObservedNode) {
	max := int(c.Status.NextPodOrdinal) - 1
	for _, n := range nodes {
		if ord, ok := controllerRedisPodOrdinal(n.Pod); ok && ord > max {
			max = ord
		}
	}
	if next := int32(max + 1); next > c.Status.NextPodOrdinal {
		c.Status.NextPodOrdinal = next
	}
}

func controllerRedisPodOrdinal(pod string) (int, bool) {
	s, ok := strings.CutPrefix(pod, "redis-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
