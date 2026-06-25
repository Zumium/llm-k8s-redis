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
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) forgetNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("forget node started")
	ns, outcome, err, ok := requireString(step.Params, "namespace")
	if !ok {
		return outcome, err
	}
	podName, outcome, err, ok := requireString(step.Params, "pod")
	if !ok {
		return outcome, err
	}
	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}

	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, fmt.Errorf("%s", outcome.Message)
	}
	seed, ok := pickSeedPod(pods)
	if !ok {
		logger.Info("no seed pod available for forget node", "pod", podName, "pods", len(pods))
		return running("no ready managed pod available to forget %s", podName), nil
	}
	logger.Info("forget node seed selected", "seedPod", seed.Name, "addr", podRedisAddr(&seed), "targetPod", podName)
	rc, err := e.RedisFactory(podRedisAddr(&seed))
	if err != nil {
		logger.Error(err, "build redis client for forget node failed", "seedPod", seed.Name)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client: %v", err)}, err
	}
	defer rc.Close()
	nodesStart := time.Now()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before forget node failed", "duration", time.Since(nodesStart), "error", err)
		return running("CLUSTER NODES failed before ForgetNode: %v", err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes before forget node read", "duration", time.Since(nodesStart), "entries", len(entries))
	nodeID, outcome, err, ok := e.forgetTargetNodeID(step, podName, pods, entries)
	if !ok {
		return outcome, err
	}
	logger.Info("forget node target resolved", "pod", podName, "nodeID", nodeID)
	target := rediscluster.FindByID(entries, nodeID)
	if target == nil {
		logger.Info("node already forgotten", "nodeID", nodeID)
		return completed("node %s already forgotten", nodeID), nil
	}
	if target.HasSlots() {
		logger.Info("node still owns slots before forget", "nodeID", nodeID, "slots", target.Slots)
		return paramErr("cannot forget node %s because it still owns slots", nodeID)
	}

	peers := forgetPeers(entries, pods, nodeID)
	if len(peers) == 0 {
		logger.Info("no healthy peers for forget node", "nodeID", nodeID)
		return running("no healthy peers available to forget node %s", nodeID), nil
	}
	logger.Info("forget peers selected", "nodeID", nodeID, "peers", len(peers))
	for _, peer := range peers {
		start := time.Now()
		logger.Info("cluster forget peer started", "peerPod", peer.Name, "nodeID", nodeID)
		peerRC, err := e.RedisFactory(podRedisAddr(peer))
		if err != nil {
			logger.Error(err, "build redis client for forget peer failed", "peerPod", peer.Name)
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s/%s: %v", peer.Namespace, peer.Name, err)}, err
		}
		if err := peerRC.ClusterForget(ctx, nodeID); err != nil && !isUnknownNodeErr(err) {
			_ = peerRC.Close()
			logger.Error(err, "cluster forget peer failed", "peerPod", peer.Name, "duration", time.Since(start))
			return StepOutcome{Status: plan.StepStateFailed, Message: err.Error()}, err
		}
		_ = peerRC.Close()
		logger.Info("cluster forget peer finished", "peerPod", peer.Name, "duration", time.Since(start))
	}
	afterStart := time.Now()
	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes after forget node failed", "duration", time.Since(afterStart), "error", err)
		return running("CLUSTER NODES failed after ForgetNode: %v", err), nil
	}
	if rediscluster.FindByID(rediscluster.ParseNodes(nodesOut), nodeID) != nil {
		logger.Info("node still visible after forget", "nodeID", nodeID, "duration", time.Since(afterStart))
		return running("node %s still visible after CLUSTER FORGET", nodeID), nil
	}
	logger.Info("node forgotten", "nodeID", nodeID, "duration", time.Since(afterStart))
	return completed("node %s forgotten", nodeID), nil
}

func isUnknownNodeErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "unknown node")
}

func (e *ActionExecutor) forgetTargetNodeID(step plan.Step, podName string, pods []corev1.Pod, entries []rediscluster.Entry) (string, StepOutcome, error, bool) {
	if nodeID, ok := paramString(step.Params, "lastKnownNodeId"); ok && nodeID != "" {
		return nodeID, StepOutcome{}, nil, true
	}
	for i := range pods {
		if pods[i].Name != podName {
			continue
		}
		entry := rediscluster.FindByIP(entries, pods[i].Status.PodIP)
		if entry == nil {
			return "", completed("pod %s already absent from CLUSTER NODES", podName), nil, false
		}
		return entry.ID, StepOutcome{}, nil, true
	}
	o, e2 := paramErr("lastKnownNodeId is required when pod %q no longer exists", podName)
	return "", o, e2, false
}

func forgetPeers(entries []rediscluster.Entry, pods []corev1.Pod, targetNodeID string) []*corev1.Pod {
	out := []*corev1.Pod{}
	for i := range pods {
		if !podReady(&pods[i]) || pods[i].Status.PodIP == "" {
			continue
		}
		entry := rediscluster.FindByIP(entries, pods[i].Status.PodIP)
		if entry == nil || entry.ID == targetNodeID || !entry.Healthy() {
			continue
		}
		out = append(out, &pods[i])
	}
	return out
}
