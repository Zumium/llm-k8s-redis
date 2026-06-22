package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

func (e *ActionExecutor) meetNode(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("meet node started")
	ns, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "namespace")
	if !ok {
		return outcome, err
	}
	sourcePod, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "sourcePod")
	if !ok {
		return outcome, err
	}
	targetPod, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "targetPod")
	if !ok {
		return outcome, err
	}

	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}
	if sourcePod == targetPod {
		return paramErr("sourcePod %q and targetPod must differ", sourcePod)
	}
	sourceExists := podInTopology(cluster, sourcePod)
	targetExists := podInTopology(cluster, targetPod)
	if !sourceExists && !precededEnsureNode(p, stepIndex, ns, sourcePod) {
		return paramErr("source pod %s/%s was not declared by a preceding EnsureNode", ns, sourcePod)
	}
	if !targetExists && !precededEnsureNode(p, stepIndex, ns, targetPod) {
		return paramErr("target pod %s/%s was not declared by a preceding EnsureNode", ns, targetPod)
	}
	if !sourceExists && !precededWaitNodeReady(p, stepIndex, ns, sourcePod) {
		return paramErr("source pod %s/%s has not completed a preceding WaitNodeReady", ns, sourcePod)
	}
	if !targetExists && !precededWaitNodeReady(p, stepIndex, ns, targetPod) {
		return paramErr("target pod %s/%s has not completed a preceding WaitNodeReady", ns, targetPod)
	}

	srcPod, outcome, err, ok := e.getPod(ctx, ns, sourcePod)
	if !ok {
		return outcome, err
	}
	tgtPod, outcome, err, ok := e.getPod(ctx, ns, targetPod)
	if !ok {
		return outcome, err
	}

	if srcPod.Status.PodIP == "" {
		logger.Info("source pod has no IP", "namespace", ns, "pod", sourcePod)
		return running("source pod %s/%s has no IP yet", ns, sourcePod), nil
	}
	if tgtPod.Status.PodIP == "" {
		logger.Info("target pod has no IP", "namespace", ns, "pod", targetPod)
		return running("target pod %s/%s has no IP yet", ns, targetPod), nil
	}

	sourceAddr := podRedisAddr(srcPod)
	logger.Info("checking meet state", "namespace", ns, "sourcePod", sourcePod, "targetPod", targetPod, "sourceAddr", sourceAddr, "targetIP", tgtPod.Status.PodIP)
	rc, err := e.RedisFactory(sourceAddr)
	if err != nil {
		logger.Error(err, "build redis client for meet failed", "addr", sourceAddr)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for source %s: %v", sourceAddr, err)}, err
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Info("source redis ping failed", "addr", sourceAddr, "duration", time.Since(pingStart), "error", err)
		return running("source redis at %s not reachable: %v", sourceAddr, err), nil
	}
	logger.Info("source redis ping succeeded", "addr", sourceAddr, "duration", time.Since(pingStart))

	nodesStart := time.Now()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before meet failed", "addr", sourceAddr, "duration", time.Since(nodesStart), "error", err)
		return running("source redis at %s CLUSTER NODES failed: %v", sourceAddr, err), nil
	}
	logger.Info("cluster nodes before meet read", "addr", sourceAddr, "duration", time.Since(nodesStart))
	if clusterNodesContainsIP(nodesOut, tgtPod.Status.PodIP) {
		logger.Info("target already visible in cluster gossip", "targetPod", targetPod, "targetIP", tgtPod.Status.PodIP)
		return completed("target %s/%s already in cluster gossip via source %s", ns, targetPod, sourcePod), nil
	}

	meetStart := time.Now()
	logger.Info("cluster meet started", "targetIP", tgtPod.Status.PodIP, "port", redisClientPort)
	if err := rc.ClusterMeet(ctx, tgtPod.Status.PodIP, int(redisClientPort)); err != nil {
		logger.Error(err, "cluster meet failed", "duration", time.Since(meetStart), "targetIP", tgtPod.Status.PodIP)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER MEET %s:%d: %v", tgtPod.Status.PodIP, redisClientPort, err)}, err
	}
	logger.Info("cluster meet issued", "duration", time.Since(meetStart), "targetIP", tgtPod.Status.PodIP)

	afterStart := time.Now()
	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes after meet failed", "addr", sourceAddr, "duration", time.Since(afterStart), "error", err)
		return running("source redis at %s CLUSTER NODES after meet failed: %v", sourceAddr, err), nil
	}
	logger.Info("cluster nodes after meet read", "addr", sourceAddr, "duration", time.Since(afterStart))
	if clusterNodesContainsIP(nodesOut, tgtPod.Status.PodIP) {
		logger.Info("target joined cluster gossip", "targetPod", targetPod, "targetIP", tgtPod.Status.PodIP)
		return completed("target %s/%s joined cluster gossip via source %s", ns, targetPod, sourcePod), nil
	}
	logger.Info("target not visible after cluster meet", "targetPod", targetPod, "targetIP", tgtPod.Status.PodIP)
	return running("CLUSTER MEET issued; target %s/%s not yet visible in CLUSTER NODES", ns, targetPod), nil
}

func precededWaitNodeReady(p *plan.Plan, stepIndex int, ns, podName string) bool {
	for i := 0; i < stepIndex; i++ {
		s := p.Steps[i]
		if s.Action != plan.ActionWaitNodeReady {
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

func podInTopology(cluster *v1alpha1.RedisCluster, podName string) bool {
	topo := cluster.Status.Topology
	if topo == nil {
		return false
	}
	for _, sh := range topo.Shards {
		if sh.Master.Pod == podName {
			return true
		}
		for _, r := range sh.Replicas {
			if r.Pod == podName {
				return true
			}
		}
	}
	return false
}

func clusterNodesContainsIP(nodesOut, ip string) bool {
	if ip == "" {
		return false
	}
	for _, line := range strings.Split(nodesOut, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Contains(line, " "+ip+":") || strings.HasPrefix(line, ip) {
			return true
		}
	}
	return false
}
