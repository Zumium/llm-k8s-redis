package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) deleteNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
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

	target := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, target); err != nil {
		if apierrors.IsNotFound(err) {
			return completed("pod %s/%s already deleted", ns, podName), nil
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}
	if target.Labels[labelCluster] != cluster.Name || target.Labels[labelManagedBy] != "redis-cluster-controller" {
		return paramErr("pod %s/%s is not managed by this RedisCluster controller", ns, podName)
	}

	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, fmt.Errorf("%s", outcome.Message)
	}
	seed, ok := pickSeedPodExcept(pods, podName)
	if !ok {
		return running("no ready managed pod available to verify deletion safety for %s", podName), nil
	}
	rc, err := e.RedisFactory(podRedisAddr(&seed))
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client: %v", err)}, err
	}
	defer rc.Close()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("CLUSTER NODES failed before DeleteNode: %v", err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	if target.Status.PodIP != "" && rediscluster.FindByIP(entries, target.Status.PodIP) != nil {
		return paramErr("cannot delete pod %s/%s because it is still a Redis Cluster member", ns, podName)
	}
	podToNodeID, _ := lastKnownNodeMaps(cluster)
	if nodeID := podToNodeID[podName]; nodeID != "" && rediscluster.FindByID(entries, nodeID) != nil {
		return paramErr("cannot delete pod %s/%s because node %s is still a Redis Cluster member", ns, podName, nodeID)
	}
	for _, entry := range entries {
		if entry.IsMaster() && entry.HasSlots() && len(rediscluster.HealthyReplicasOf(entries, entry.ID)) == 0 {
			return paramErr("cannot delete pod %s/%s because master %s has no healthy replica", ns, podName, entry.ID)
		}
	}
	if err := e.Delete(ctx, target); err != nil {
		if apierrors.IsNotFound(err) {
			return completed("pod %s/%s already deleted", ns, podName), nil
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("delete pod: %v", err)}, err
	}
	return running("pod %s/%s deletion requested", ns, podName), nil
}

func pickSeedPodExcept(pods []corev1.Pod, excludedName string) (corev1.Pod, bool) {
	for _, p := range pods {
		if p.Name != excludedName && podReady(&p) && p.Status.PodIP != "" {
			return p, true
		}
	}
	return corev1.Pod{}, false
}
