package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

func (e *ActionExecutor) forgetNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
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
		return running("no ready managed pod available to forget %s", podName), nil
	}
	rc, err := e.RedisFactory(podRedisAddr(&seed))
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client: %v", err)}, err
	}
	defer rc.Close()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("CLUSTER NODES failed before ForgetNode: %v", err), nil
	}
	entries := parseClusterNodes(nodesOut)
	nodeID, outcome, err, ok := e.forgetTargetNodeID(step, podName, pods, entries)
	if !ok {
		return outcome, err
	}
	target := findByID(entries, nodeID)
	if target == nil {
		return completed("node %s already forgotten", nodeID), nil
	}
	if target.hasSlots() {
		return paramErr("cannot forget node %s because it still owns slots", nodeID)
	}

	peers := forgetPeers(entries, pods, nodeID)
	if len(peers) == 0 {
		return running("no healthy peers available to forget node %s", nodeID), nil
	}
	for _, peer := range peers {
		peerRC, err := e.RedisFactory(podRedisAddr(peer))
		if err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s/%s: %v", peer.Namespace, peer.Name, err)}, err
		}
		if err := peerRC.ClusterForget(ctx, nodeID); err != nil && !isUnknownNodeErr(err) {
			_ = peerRC.Close()
			return StepOutcome{Status: plan.StepStateFailed, Message: err.Error()}, err
		}
		_ = peerRC.Close()
	}
	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		return running("CLUSTER NODES failed after ForgetNode: %v", err), nil
	}
	if findByID(parseClusterNodes(nodesOut), nodeID) != nil {
		return running("node %s still visible after CLUSTER FORGET", nodeID), nil
	}
	return completed("node %s forgotten", nodeID), nil
}

func isUnknownNodeErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "unknown node")
}

func (e *ActionExecutor) forgetTargetNodeID(step plan.Step, podName string, pods []corev1.Pod, entries []clusterNodeEntry) (string, StepOutcome, error, bool) {
	if nodeID, ok := paramString(step.Params, "lastKnownNodeId"); ok && nodeID != "" {
		return nodeID, StepOutcome{}, nil, true
	}
	for i := range pods {
		if pods[i].Name != podName {
			continue
		}
		entry := findByIP(entries, pods[i].Status.PodIP)
		if entry == nil {
			return "", running("pod %s is not visible in CLUSTER NODES yet", podName), nil, false
		}
		return entry.ID, StepOutcome{}, nil, true
	}
	o, e2 := paramErr("lastKnownNodeId is required when pod %q no longer exists", podName)
	return "", o, e2, false
}

func forgetPeers(entries []clusterNodeEntry, pods []corev1.Pod, targetNodeID string) []*corev1.Pod {
	out := []*corev1.Pod{}
	for i := range pods {
		if !podReady(&pods[i]) || pods[i].Status.PodIP == "" {
			continue
		}
		entry := findByIP(entries, pods[i].Status.PodIP)
		if entry == nil || entry.ID == targetNodeID || !entry.healthy() {
			continue
		}
		out = append(out, &pods[i])
	}
	return out
}
