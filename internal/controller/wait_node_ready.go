package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func (e *ActionExecutor) waitNodeReady(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("wait node ready started")
	ns, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "namespace")
	if !ok {
		return outcome, err
	}
	podName, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "pod")
	if !ok {
		return outcome, err
	}

	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}
	if !precededEnsureNode(p, stepIndex, ns, podName) {
		return paramErr("step %q: pod %s/%s was not declared by a preceding EnsureNode", p.Steps[stepIndex].ID, ns, podName)
	}

	pod, outcome, err, ok := e.getPod(ctx, ns, podName)
	if !ok {
		return outcome, err
	}

	if !podReady(pod) {
		logger.Info("pod not ready", "namespace", ns, "pod", podName)
		return running("pod %s/%s not ready yet", ns, podName), nil
	}
	addr := podRedisAddr(pod)
	if addr == "" {
		logger.Info("pod has no IP", "namespace", ns, "pod", podName)
		return running("pod %s/%s has no IP yet", ns, podName), nil
	}

	logger.Info("checking redis readiness", "namespace", ns, "pod", podName, "addr", addr)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		logger.Error(err, "build redis client failed", "addr", addr)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Info("redis ping failed", "addr", addr, "duration", time.Since(pingStart), "error", err)
		return running("redis at %s not reachable: %v", addr, err), nil
	}
	logger.Info("redis ping succeeded", "addr", addr, "duration", time.Since(pingStart))
	myIDStart := time.Now()
	nodeID, err := rc.ClusterMyID(ctx)
	if err != nil {
		logger.Info("cluster myid failed", "addr", addr, "duration", time.Since(myIDStart), "error", err)
		return running("redis at %s CLUSTER MYID failed: %v", addr, err), nil
	}
	if strings.TrimSpace(nodeID) == "" {
		logger.Info("cluster myid returned empty node id", "addr", addr, "duration", time.Since(myIDStart))
		return running("redis at %s returned empty node id", addr), nil
	}
	logger.Info("cluster myid succeeded", "addr", addr, "nodeID", strings.TrimSpace(nodeID), "duration", time.Since(myIDStart))

	refreshExistingTopologyNode(cluster, podName, nodeID)
	return completed("pod %s/%s ready; nodeId=%s", ns, podName, nodeID), nil
}

func precededEnsureNode(p *plan.Plan, stepIndex int, ns, podName string) bool {
	for i := 0; i < stepIndex; i++ {
		s := p.Steps[i]
		if s.Action != plan.ActionEnsureNode {
			continue
		}
		ens, ok1 := paramString(s.Params, "namespace")
		ep, ok2 := paramString(s.Params, "pod")
		if ok1 && ok2 && ens == ns && ep == podName {
			return true
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func refreshExistingTopologyNode(cluster *v1alpha1.RedisCluster, podName, nodeID string) {
	if cluster.Status.Topology == nil {
		return
	}
	for i := range cluster.Status.Topology.Shards {
		sh := &cluster.Status.Topology.Shards[i]
		if sh.Master.Pod == podName {
			sh.Master.NodeID = nodeID
			sh.Master.Ready = true
		}
		for j := range sh.Replicas {
			if sh.Replicas[j].Pod == podName {
				sh.Replicas[j].NodeID = nodeID
				sh.Replicas[j].Ready = true
			}
		}
	}
}
