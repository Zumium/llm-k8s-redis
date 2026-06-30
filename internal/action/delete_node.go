package action

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) deleteNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("delete node started")
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
	getStart := time.Now()
	logger.Info("getting pod for delete node", "namespace", ns, "pod", podName)
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, target); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("pod already deleted", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
			return completed("pod %s/%s already deleted", ns, podName), nil
		}
		logger.Error(err, "get pod for delete node failed", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}
	logger.Info("pod found for delete node", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
	if target.Labels[labelCluster] != cluster.Name || target.Labels[labelManagedBy] != "redis-cluster-controller" {
		logger.Info("pod is not managed by this controller", "namespace", ns, "pod", podName)
		return paramErr("pod %s/%s is not managed by this RedisCluster controller", ns, podName)
	}

	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, fmt.Errorf("%s", outcome.Message)
	}
	seed, ok := pickSeedPodExcept(pods, podName)
	if !ok {
		logger.Info("no seed pod available for delete safety check", "pod", podName, "pods", len(pods))
		return running("no ready managed pod available to verify deletion safety for %s", podName), nil
	}
	logger.Info("delete safety seed selected", "seedPod", seed.Name, "addr", podRedisAddr(&seed), "targetPod", podName)
	rc, err := e.RedisFactory(podRedisAddr(&seed))
	if err != nil {
		logger.Error(err, "build redis client for delete safety failed", "seedPod", seed.Name)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client: %v", err)}, err
	}
	defer rc.Close()
	nodesStart := time.Now()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before delete node failed", "duration", time.Since(nodesStart), "error", err)
		return running("CLUSTER NODES failed before DeleteNode: %v", err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes before delete node read", "duration", time.Since(nodesStart), "entries", len(entries))
	if target.Status.PodIP != "" && rediscluster.FindByIP(entries, target.Status.PodIP) != nil {
		logger.Info("delete blocked: pod still cluster member by ip", "pod", podName, "ip", target.Status.PodIP)
		return paramErr("cannot delete pod %s/%s because it is still a Redis Cluster member", ns, podName)
	}
	podToNodeID, _ := lastKnownNodeMaps(cluster)
	if nodeID := podToNodeID[podName]; nodeID != "" && rediscluster.FindByID(entries, nodeID) != nil {
		logger.Info("delete blocked: pod still cluster member by node id", "pod", podName, "nodeID", nodeID)
		return paramErr("cannot delete pod %s/%s because node %s is still a Redis Cluster member", ns, podName, nodeID)
	}
	for _, entry := range entries {
		if entry.IsMaster() && entry.HasSlots() && len(rediscluster.HealthyReplicasOf(entries, entry.ID)) == 0 {
			logger.Info("delete blocked: master has no healthy replica", "masterID", entry.ID)
			return paramErr("cannot delete pod %s/%s because master %s has no healthy replica", ns, podName, entry.ID)
		}
	}
	deleteStart := time.Now()
	logger.Info("deleting pod", "namespace", ns, "pod", podName)
	if err := e.Delete(ctx, target); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("pod already deleted during delete", "namespace", ns, "pod", podName, "duration", time.Since(deleteStart))
			return completed("pod %s/%s already deleted", ns, podName), nil
		}
		logger.Error(err, "delete pod failed", "namespace", ns, "pod", podName, "duration", time.Since(deleteStart))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("delete pod: %v", err)}, err
	}
	logger.Info("pod deletion requested", "namespace", ns, "pod", podName, "duration", time.Since(deleteStart))
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
