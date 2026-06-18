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

// waitNodeReady is the executor for plan.ActionWaitNodeReady. It is idempotent
// and never relies on in-memory state from a previous reconcile.
//
// Completion criteria (all must hold):
//   - the referenced Pod exists
//   - the Pod's Ready condition is True
//   - the Pod has a PodIP
//   - Redis responds to PING
//   - Redis returns a non-empty Cluster node id via CLUSTER MYID
//
// Until all of the above are satisfied the step stays Running so the
// reconciler retries after a short delay. Param/safety violations or a
// missing Pod fail the step.
//
// Topology: per the agreed minimal approach (Plan A), WaitNodeReady does NOT
// invent a shard/master/replica structure. It only refreshes NodeID/Ready on
// an existing NodeTopology entry that already references this Pod; if no such
// entry exists yet, the node id is observed but not persisted. Later actions
// (MeetNode/ReplicateNode/VerifyCluster) populate the full topology from the
// live Redis Cluster.
func (e *ActionExecutor) waitNodeReady(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
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

	pod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return paramErr("pod %s/%s does not exist", ns, podName)
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}

	if !podReady(pod) {
		return running("pod %s/%s not ready yet", ns, podName), nil
	}
	addr := podRedisAddr(pod)
	if addr == "" {
		return running("pod %s/%s has no IP yet", ns, podName), nil
	}

	rc, err := e.RedisFactory(addr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("redis at %s not reachable: %v", addr, err), nil
	}
	nodeID, err := rc.ClusterMyID(ctx)
	if err != nil {
		return running("redis at %s CLUSTER MYID failed: %v", addr, err), nil
	}
	if strings.TrimSpace(nodeID) == "" {
		return running("redis at %s returned empty node id", addr), nil
	}

	refreshExistingTopologyNode(cluster, podName, nodeID)
	return completed("pod %s/%s ready; nodeId=%s", ns, podName, nodeID), nil
}

// precededEnsureNode reports whether an EnsureNode step before stepIndex in p
// declared the same (namespace, pod) pair. It is the safety re-check for the
// WaitNodeReady precondition "pod must have been declared by EnsureNode".
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

// podReady reports whether the Pod's Ready condition is True.
func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// refreshExistingTopologyNode refreshes NodeID and Ready=true on any
// NodeTopology entry that already references podName. It does NOT create new
// shards or nodes: WaitNodeReady observes a single node's id, not the cluster
// topology, so inventing a shard/master/replica structure would be a lie.
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
