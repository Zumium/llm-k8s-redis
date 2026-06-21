package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

func (e *ActionExecutor) replicateNode(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	ns, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "namespace")
	if !ok {
		return outcome, err
	}
	masterPod, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "masterPod")
	if !ok {
		return outcome, err
	}
	replicaPod, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "replicaPod")
	if !ok {
		return outcome, err
	}

	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}
	if masterPod == replicaPod {
		return paramErr("masterPod %q and replicaPod must differ", masterPod)
	}
	masterExists := podInTopology(cluster, masterPod)
	replicaExists := podInTopology(cluster, replicaPod)
	if !masterExists && !precededEnsureNode(p, stepIndex, ns, masterPod) {
		return paramErr("master pod %s/%s was not declared by a preceding EnsureNode", ns, masterPod)
	}
	if !replicaExists && !precededEnsureNode(p, stepIndex, ns, replicaPod) {
		return paramErr("replica pod %s/%s was not declared by a preceding EnsureNode", ns, replicaPod)
	}
	if !masterExists && !precededWaitNodeReady(p, stepIndex, ns, masterPod) {
		return paramErr("master pod %s/%s has not completed a preceding WaitNodeReady", ns, masterPod)
	}
	if !replicaExists && !precededWaitNodeReady(p, stepIndex, ns, replicaPod) {
		return paramErr("replica pod %s/%s has not completed a preceding WaitNodeReady", ns, replicaPod)
	}

	masterK8SPod, outcome, err, ok := e.getPod(ctx, ns, masterPod)
	if !ok {
		return outcome, err
	}
	replicaK8SPod, outcome, err, ok := e.getPod(ctx, ns, replicaPod)
	if !ok {
		return outcome, err
	}

	if masterK8SPod.Status.PodIP == "" {
		return running("master pod %s/%s has no IP yet", ns, masterPod), nil
	}
	if replicaK8SPod.Status.PodIP == "" {
		return running("replica pod %s/%s has no IP yet", ns, replicaPod), nil
	}

	replicaAddr := podRedisAddr(replicaK8SPod)
	rc, err := e.RedisFactory(replicaAddr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for replica %s: %v", replicaAddr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("replica redis at %s not reachable: %v", replicaAddr, err), nil
	}

	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("replica redis at %s CLUSTER NODES failed: %v", replicaAddr, err), nil
	}
	entries := parseClusterNodes(nodesOut)
	masterEntry := findByIP(entries, masterK8SPod.Status.PodIP)
	if masterEntry == nil {
		return running("master %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, masterPod, masterK8SPod.Status.PodIP), nil
	}
	if !masterEntry.isMaster() {
		return paramErr("pod %s/%s is not a master (flags=%v)", ns, masterPod, masterEntry.Flags)
	}
	replicaEntry := findByIP(entries, replicaK8SPod.Status.PodIP)
	if replicaEntry == nil {
		return running("replica %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, replicaPod, replicaK8SPod.Status.PodIP), nil
	}
	if replicaEntry.hasFlag("fail") || replicaEntry.hasFlag("fail?") {
		return paramErr("replica %s/%s is in a failed state (flags=%v)", ns, replicaPod, replicaEntry.Flags)
	}

	if replicaEntry.MasterID == masterEntry.ID {
		refreshExistingTopologyNode(cluster, masterPod, masterEntry.ID)
		refreshExistingTopologyNode(cluster, replicaPod, replicaEntry.ID)
		return completed("replica %s/%s already replicates master %s/%s", ns, replicaPod, ns, masterPod), nil
	}

	if replicaEntry.hasSlots() {
		return paramErr("replica %s/%s owns slots and cannot be turned into a replica", ns, replicaPod)
	}

	if err := rc.ClusterReplicate(ctx, masterEntry.ID); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER REPLICATE %s: %v", masterEntry.ID, err)}, err
	}

	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		return running("replica redis at %s CLUSTER NODES after replicate failed: %v", replicaAddr, err), nil
	}
	entries = parseClusterNodes(nodesOut)
	replicaEntry = findByIP(entries, replicaK8SPod.Status.PodIP)
	if replicaEntry == nil {
		return running("replica %s/%s disappeared after CLUSTER REPLICATE", ns, replicaPod), nil
	}
	if replicaEntry.MasterID == masterEntry.ID {
		refreshExistingTopologyNode(cluster, masterPod, masterEntry.ID)
		refreshExistingTopologyNode(cluster, replicaPod, replicaEntry.ID)
		return completed("replica %s/%s now replicates master %s/%s", ns, replicaPod, ns, masterPod), nil
	}
	return running("CLUSTER REPLICATE issued; replica %s/%s not yet shown as replica of master", ns, replicaPod), nil
}
