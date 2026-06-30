package action

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) replicateNode(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("replicate node started")
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

	if outcome, err, ok := validateClusterNamespace(cluster, ns); !ok {
		return outcome, err
	}
	if masterPod == replicaPod {
		return paramErr("masterPod %q and replicaPod must differ", masterPod)
	}
	if !podDeclaredOrInTopology(cluster, p, stepIndex, ns, masterPod) {
		return paramErr("master pod %s/%s was not declared by a preceding EnsureNode", ns, masterPod)
	}
	if !podDeclaredOrInTopology(cluster, p, stepIndex, ns, replicaPod) {
		return paramErr("replica pod %s/%s was not declared by a preceding EnsureNode", ns, replicaPod)
	}
	if !podWaitedOrInTopology(cluster, p, stepIndex, ns, masterPod) {
		return paramErr("master pod %s/%s has not completed a preceding WaitNodeReady", ns, masterPod)
	}
	if !podWaitedOrInTopology(cluster, p, stepIndex, ns, replicaPod) {
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
		logger.Info("master pod has no IP", "namespace", ns, "pod", masterPod)
		return running("master pod %s/%s has no IP yet", ns, masterPod), nil
	}
	if replicaK8SPod.Status.PodIP == "" {
		logger.Info("replica pod has no IP", "namespace", ns, "pod", replicaPod)
		return running("replica pod %s/%s has no IP yet", ns, replicaPod), nil
	}

	replicaAddr := podRedisAddr(replicaK8SPod)
	logger.Info("checking replication state", "namespace", ns, "masterPod", masterPod, "replicaPod", replicaPod, "replicaAddr", replicaAddr)
	rc, err := e.RedisFactory(replicaAddr)
	if err != nil {
		logger.Error(err, "build redis client for replicate failed", "addr", replicaAddr)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for replica %s: %v", replicaAddr, err)}, err
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Info("replica redis ping failed", "addr", replicaAddr, "duration", time.Since(pingStart), "error", err)
		return running("replica redis at %s not reachable: %v", replicaAddr, err), nil
	}
	logger.Info("replica redis ping succeeded", "addr", replicaAddr, "duration", time.Since(pingStart))

	nodesStart := time.Now()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before replicate failed", "addr", replicaAddr, "duration", time.Since(nodesStart), "error", err)
		return running("replica redis at %s CLUSTER NODES failed: %v", replicaAddr, err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes before replicate read", "addr", replicaAddr, "duration", time.Since(nodesStart), "entries", len(entries))
	masterEntry := rediscluster.FindByIP(entries, masterK8SPod.Status.PodIP)
	if masterEntry == nil {
		logger.Info("master not visible before replicate", "masterPod", masterPod, "masterIP", masterK8SPod.Status.PodIP)
		return running("master %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, masterPod, masterK8SPod.Status.PodIP), nil
	}
	if !masterEntry.IsMaster() {
		logger.Info("target master pod is not a master", "masterPod", masterPod, "flags", masterEntry.Flags)
		return paramErr("pod %s/%s is not a master (flags=%v)", ns, masterPod, masterEntry.Flags)
	}
	replicaEntry := rediscluster.FindByIP(entries, replicaK8SPod.Status.PodIP)
	if replicaEntry == nil {
		logger.Info("replica not visible before replicate", "replicaPod", replicaPod, "replicaIP", replicaK8SPod.Status.PodIP)
		return running("replica %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, replicaPod, replicaK8SPod.Status.PodIP), nil
	}
	if replicaEntry.HasFlag("fail") || replicaEntry.HasFlag("fail?") {
		logger.Info("replica is failed before replicate", "replicaPod", replicaPod, "flags", replicaEntry.Flags)
		return paramErr("replica %s/%s is in a failed state (flags=%v)", ns, replicaPod, replicaEntry.Flags)
	}

	if replicaEntry.MasterID == masterEntry.ID {
		logger.Info("replica already follows master", "replicaPod", replicaPod, "masterPod", masterPod, "masterID", masterEntry.ID)
		refreshExistingTopologyNode(cluster, masterPod, masterEntry.ID)
		refreshExistingTopologyNode(cluster, replicaPod, replicaEntry.ID)
		return completed("replica %s/%s already replicates master %s/%s", ns, replicaPod, ns, masterPod), nil
	}

	if replicaEntry.HasSlots() {
		logger.Info("replica owns slots before replicate", "replicaPod", replicaPod, "slots", replicaEntry.Slots)
		return paramErr("replica %s/%s owns slots and cannot be turned into a replica", ns, replicaPod)
	}

	replicateStart := time.Now()
	logger.Info("cluster replicate started", "replicaPod", replicaPod, "masterID", masterEntry.ID)
	if err := rc.ClusterReplicate(ctx, masterEntry.ID); err != nil {
		logger.Error(err, "cluster replicate failed", "duration", time.Since(replicateStart), "masterID", masterEntry.ID)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER REPLICATE %s: %v", masterEntry.ID, err)}, err
	}
	logger.Info("cluster replicate issued", "duration", time.Since(replicateStart), "masterID", masterEntry.ID)

	afterStart := time.Now()
	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes after replicate failed", "addr", replicaAddr, "duration", time.Since(afterStart), "error", err)
		return running("replica redis at %s CLUSTER NODES after replicate failed: %v", replicaAddr, err), nil
	}
	entries = rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes after replicate read", "addr", replicaAddr, "duration", time.Since(afterStart), "entries", len(entries))
	replicaEntry = rediscluster.FindByIP(entries, replicaK8SPod.Status.PodIP)
	if replicaEntry == nil {
		logger.Info("replica disappeared after replicate", "replicaPod", replicaPod)
		return running("replica %s/%s disappeared after CLUSTER REPLICATE", ns, replicaPod), nil
	}
	if replicaEntry.MasterID == masterEntry.ID {
		logger.Info("replica now follows master", "replicaPod", replicaPod, "masterPod", masterPod, "masterID", masterEntry.ID)
		refreshExistingTopologyNode(cluster, masterPod, masterEntry.ID)
		refreshExistingTopologyNode(cluster, replicaPod, replicaEntry.ID)
		return completed("replica %s/%s now replicates master %s/%s", ns, replicaPod, ns, masterPod), nil
	}
	logger.Info("replica not yet shown as replica of master", "replicaPod", replicaPod, "masterPod", masterPod)
	return running("CLUSTER REPLICATE issued; replica %s/%s not yet shown as replica of master", ns, replicaPod), nil
}
