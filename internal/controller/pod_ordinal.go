package controller

import (
	"strconv"
	"strings"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

func bumpNextPodOrdinal(c *v1alpha1.RedisCluster) bool {
	before := c.Status.NextPodOrdinal
	max := int(before) - 1
	for _, pod := range statusTopologyPods(c.Status.Topology) {
		if n, ok := controllerRedisPodOrdinal(pod); ok && n > max {
			max = n
		}
	}
	if c.Status.ActivePlan != nil {
		p, err := statusToPlan(c.Status.ActivePlan)
		if err == nil {
			for _, s := range p.Steps {
				if s.Action != plan.ActionEnsureNode {
					continue
				}
				pod, ok := paramString(s.Params, "pod")
				if !ok {
					continue
				}
				if n, ok := controllerRedisPodOrdinal(pod); ok && n > max {
					max = n
				}
			}
		}
	}
	next := int32(max + 1)
	if next < 0 {
		next = 0
	}
	if next > c.Status.NextPodOrdinal {
		c.Status.NextPodOrdinal = next
		return true
	}
	return false
}

func advanceNextPodOrdinalFromPlan(c *v1alpha1.RedisCluster, p *plan.Plan) {
	if p == nil {
		return
	}
	next := c.Status.NextPodOrdinal
	for _, s := range p.Steps {
		if s.Action != plan.ActionEnsureNode {
			continue
		}
		pod, ok := paramString(s.Params, "pod")
		if !ok {
			continue
		}
		if n, ok := controllerRedisPodOrdinal(pod); ok && int32(n+1) > next {
			next = int32(n + 1)
		}
	}
	c.Status.NextPodOrdinal = next
}

func statusTopologyPods(t *v1alpha1.ClusterTopology) []string {
	if t == nil {
		return nil
	}
	out := []string{}
	for _, sh := range t.Shards {
		if sh.Master.Pod != "" {
			out = append(out, sh.Master.Pod)
		}
		for _, r := range sh.Replicas {
			if r.Pod != "" {
				out = append(out, r.Pod)
			}
		}
	}
	return out
}

func controllerRedisPodOrdinal(pod string) (int, bool) {
	if !strings.HasPrefix(pod, "redis-") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(pod, "redis-"))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
