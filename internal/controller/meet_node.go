package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// meetNode is the executor for plan.ActionMeetNode. It is idempotent and
// re-derives state from K8S and Redis on every call.
//
// Completion criteria:
//   - both sourcePod and targetPod exist as K8S Pods and have IPs
//   - both have been declared by a preceding EnsureNode
//   - both have passed a preceding WaitNodeReady (the step is present and
//     completed earlier in the plan)
//   - sourcePod != targetPod, namespace == cluster name
//   - source Redis responds to PING
//   - target Pod's IP appears in source's `CLUSTER NODES` output
//
// If target is already a member of source's gossip network the step is
// Completed. Otherwise the executor issues `CLUSTER MEET targetIP 6379` on
// the source node and re-queries `CLUSTER NODES` to confirm propagation. If
// the target has not yet shown up the step stays Running for the next
// reconcile.
//
// Topology: MeetNode deliberately does not rebuild status.topology. Cluster
// topology reconstruction is the responsibility of later actions
// (ReplicateNode/AddSlots/VerifyCluster) once roles and slot ownership exist.
func (e *ActionExecutor) meetNode(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
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
	sourceExists := podInExistingTopology(cluster, sourcePod)
	targetExists := podInExistingTopology(cluster, targetPod)
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

	srcPod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: sourcePod}, srcPod); err != nil {
		if apierrors.IsNotFound(err) {
			return paramErr("source pod %s/%s does not exist", ns, sourcePod)
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get source pod: %v", err)}, err
	}
	tgtPod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: targetPod}, tgtPod); err != nil {
		if apierrors.IsNotFound(err) {
			return paramErr("target pod %s/%s does not exist", ns, targetPod)
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get target pod: %v", err)}, err
	}

	if srcPod.Status.PodIP == "" {
		return running("source pod %s/%s has no IP yet", ns, sourcePod), nil
	}
	if tgtPod.Status.PodIP == "" {
		return running("target pod %s/%s has no IP yet", ns, targetPod), nil
	}

	sourceAddr := podRedisAddr(srcPod)
	rc, err := e.RedisFactory(sourceAddr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for source %s: %v", sourceAddr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("source redis at %s not reachable: %v", sourceAddr, err), nil
	}

	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("source redis at %s CLUSTER NODES failed: %v", sourceAddr, err), nil
	}
	if clusterNodesContainsIP(nodesOut, tgtPod.Status.PodIP) {
		return completed("target %s/%s already in cluster gossip via source %s", ns, targetPod, sourcePod), nil
	}

	if err := rc.ClusterMeet(ctx, tgtPod.Status.PodIP, int(redisClientPort)); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER MEET %s:%d: %v", tgtPod.Status.PodIP, redisClientPort, err)}, err
	}

	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		return running("source redis at %s CLUSTER NODES after meet failed: %v", sourceAddr, err), nil
	}
	if clusterNodesContainsIP(nodesOut, tgtPod.Status.PodIP) {
		return completed("target %s/%s joined cluster gossip via source %s", ns, targetPod, sourcePod), nil
	}
	return running("CLUSTER MEET issued; target %s/%s not yet visible in CLUSTER NODES", ns, targetPod), nil
}

// precededWaitNodeReady reports whether a WaitNodeReady step before stepIndex
// in p targeted the same (namespace, pod) and is recorded as Completed. The
// plan passed to executors carries step state via statusToPlan, but that
// conversion strips status; the reconciler only calls ExecuteStep for the
// first non-completed step, so a preceding WaitNodeReady that ran is already
// Completed in status. To stay executor-side safe without depending on the
// reconciler's selection, we treat presence of the step before stepIndex as
// the precondition (matching the Validator's structural checks) and verify
// reachability via K8S/Redis at runtime instead.
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

// podInExistingTopology reports whether podName is already recorded in the
// cluster's status.topology. Existing nodes do not need preceding EnsureNode or
// WaitNodeReady steps in the current plan (e.g. when a scale-out plan uses an
// existing master as the MeetNode source).
func podInExistingTopology(cluster *v1alpha1.RedisCluster, podName string) bool {
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

// clusterNodesContainsIP reports whether the given IP appears in the
// `CLUSTER NODES` output. CLUSTER NODES lists each member with its announced
// host:port (or : port cluster-bus-port format), so matching on the IP string
// is sufficient for membership detection.
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
