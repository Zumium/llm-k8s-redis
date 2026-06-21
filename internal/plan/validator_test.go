package plan

import "testing"

func validCreatePlan() *Plan {
	return &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "create-001",
		TargetGeneration: 1,
		Summary:          "Create Redis Cluster with 2 shards and 1 replica per shard",
		Steps: []Step{
			{ID: "ensure-redis-0", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-2", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-3", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-3", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "wait-redis-2", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "wait-redis-3", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-3"}},
			{ID: "meet-redis-1", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "meet-redis-2", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2"}},
			{ID: "meet-redis-3", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-3"}},
			{ID: "replicate-redis-1", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "replicate-redis-3", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-3"}},
			{ID: "add-slots-redis-0", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-8191"}},
			{ID: "add-slots-redis-2", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-2", "slots": "8192-16383"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func spec() ClusterSpec {
	return ClusterSpec{Name: "example", Generation: 1, Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"}
}

func replicaScaleOutSpec() ClusterSpec {
	s := spec()
	s.ReplicasPerShard = 2
	return s
}

func shardScaleOutSpec() ClusterSpec {
	s := spec()
	s.Shards = 3
	return s
}

func topology() *ClusterTopology {
	return &ClusterTopology{Shards: []ShardTopology{
		{ID: "shard-0", Master: NodeTopology{Pod: "redis-0", Slots: "0-8191", Ready: true}, Replicas: []NodeTopology{{Pod: "redis-1", Ready: true}}},
		{ID: "shard-1", Master: NodeTopology{Pod: "redis-2", Slots: "8192-16383", Ready: true}, Replicas: []NodeTopology{{Pod: "redis-3", Ready: true}}},
	}}
}

func validReplicaScaleOutPlan() *Plan {
	return &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "replica-scaleout-001",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "ensure-redis-0", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "ensure-redis-2", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-2", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "ensure-redis-4", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-5", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-5", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-4", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "wait-redis-5", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-5"}},
			{ID: "meet-redis-4", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-4"}},
			{ID: "meet-redis-5", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-2", "targetPod": "redis-5"}},
			{ID: "replicate-redis-4", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-4"}},
			{ID: "replicate-redis-5", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-5"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 2, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func validShardScaleOutPlan() *Plan {
	return &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "shard-scaleout-001",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "ensure-redis-4", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-5", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-5", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-4", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "wait-redis-5", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-5"}},
			{ID: "meet-redis-4", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-4"}},
			{ID: "meet-redis-5", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-5"}},
			{ID: "replicate-redis-5", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-4", "replicaPod": "redis-5"}},
			{ID: "migrate-redis-0-redis-2", Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "5462-8191"}},
			{ID: "migrate-redis-2-redis-4", Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-2", "targetPod": "redis-4", "slots": "10923-16383"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 3, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func stepIndex(t *testing.T, p *Plan, id string) int {
	t.Helper()
	for i, s := range p.Steps {
		if s.ID == id {
			return i
		}
	}
	t.Fatalf("step %q not found", id)
	return -1
}

func TestValidate_ValidCreate(t *testing.T) {
	if err := NewValidator().Validate(validCreatePlan(), spec()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_CreateRejectsNonRedisPrefixedPods(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["pod"] = "example-0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for non-redis-prefixed pod name")
	}
}

func TestValidate_CreateRejectsNonContiguousPods(t *testing.T) {
	p := validCreatePlan()
	p.Steps[3].Params["pod"] = "redis-4"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for non-contiguous pod names")
	}
}

func TestValidate_CreateRejectsPodsNotStartingFromZero(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["pod"] = "redis-1"
	p.Steps[1].Params["pod"] = "redis-2"
	p.Steps[2].Params["pod"] = "redis-3"
	p.Steps[3].Params["pod"] = "redis-4"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for pod names not starting from redis-0")
	}
}

func TestValidate_CreateRejectsNegativeOrdinal(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["pod"] = "redis--1"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for negative ordinal")
	}
}

func TestValidate_ValidReplicaScaleOut(t *testing.T) {
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(validReplicaScaleOutPlan(), ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_ValidShardScaleOut(t *testing.T) {
	ctx := ValidationContext{Spec: shardScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(validShardScaleOutPlan(), ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_ShardScaleOutRejectsAddSlots(t *testing.T) {
	p := validShardScaleOutPlan()
	p.Steps = append(p.Steps[:len(p.Steps)-1], Step{ID: "bad-add-slots", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-4", "slots": "10923-10924"}}, p.Steps[len(p.Steps)-1])
	ctx := ValidationContext{Spec: shardScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for AddSlots in shard scaleout")
	}
}

func TestValidate_ShardScaleOutRejectsWrongMigrationSlots(t *testing.T) {
	p := validShardScaleOutPlan()
	p.Steps[stepIndex(t, p, "migrate-redis-2-redis-4")].Params["slots"] = "10924-16383"
	ctx := ValidationContext{Spec: shardScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for incomplete MigrateSlots matrix")
	}
}

func TestValidate_ShardScaleOutRejectsShardAndReplicaChange(t *testing.T) {
	s := shardScaleOutSpec()
	s.ReplicasPerShard = 2
	ctx := ValidationContext{Spec: s, Topology: topology()}
	if err := NewValidator().Validate(validShardScaleOutPlan(), ctx); err == nil {
		t.Fatal("expected error when shards and replicasPerShard both change")
	}
}

func TestValidate_ReplicaScaleOutRejectsSlotActions(t *testing.T) {
	p := validReplicaScaleOutPlan()
	p.Steps = append(p.Steps[:len(p.Steps)-1], Step{ID: "bad-add-slots", Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-1"}}, p.Steps[len(p.Steps)-1])
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for AddSlots in replica scaleout")
	}
}

func TestValidate_ReplicaScaleOutRejectsShardChange(t *testing.T) {
	s := replicaScaleOutSpec()
	s.Shards = 3
	ctx := ValidationContext{Spec: s, Topology: topology()}
	if err := NewValidator().Validate(validReplicaScaleOutPlan(), ctx); err == nil {
		t.Fatal("expected error when shards changes")
	}
}

func TestValidate_ReplicaScaleOutRejectsNonContiguousNewPods(t *testing.T) {
	p := validReplicaScaleOutPlan()
	p.Steps[5].Params["pod"] = "redis-6"
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for non-contiguous new pod names")
	}
}

func TestValidate_ReplicaScaleOutRejectsNonRedisPrefixedExistingPods(t *testing.T) {
	topo := topology()
	topo.Shards[1].Master.Pod = "redis-example-2"
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topo}
	if err := NewValidator().Validate(validReplicaScaleOutPlan(), ctx); err == nil {
		t.Fatal("expected error for non-redis-prefixed existing pod")
	}
}

func TestValidate_ReplicaScaleOutRejectsNonContiguousNewPodsDirect(t *testing.T) {
	existing := map[string]bool{
		"redis-0": true,
		"redis-1": true,
		"redis-2": true,
	}
	newPods := map[string]bool{
		"redis-3": true,
		"redis-5": true,
	}
	if err := validateSequentialNewPods(existing, newPods, 3); err == nil {
		t.Fatal("expected error for non-contiguous new pods")
	}
}

func TestValidate_ReplicaScaleOutUsesNextPodOrdinal(t *testing.T) {
	topo := topology()
	topo.Shards[0].Replicas[0].Pod = "redis-4"
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topo, NextPodOrdinal: 5}
	p := validReplicaScaleOutPlan()
	p.Steps[4].Params["pod"] = "redis-5"
	p.Steps[5].Params["pod"] = "redis-6"
	p.Steps[6].Params["pod"] = "redis-5"
	p.Steps[7].Params["pod"] = "redis-6"
	p.Steps[8].Params["targetPod"] = "redis-5"
	p.Steps[9].Params["targetPod"] = "redis-6"
	p.Steps[10].Params["replicaPod"] = "redis-5"
	p.Steps[11].Params["replicaPod"] = "redis-6"
	if err := NewValidator().Validate(p, ctx); err != nil {
		t.Fatalf("expected next ordinal pods to pass, got %v", err)
	}
	p.Steps[4].Params["pod"] = "redis-1"
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected reused ordinal to fail")
	}
}

func validReplicaReplacementPlan() *Plan {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "repair-1",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "ensure", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "meet", Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-4"}},
			{ID: "replicate", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-4"}},
			{ID: "forget", Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "node-1"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	return p
}

func TestValidate_ForgetNodeRejectsSlotOwner(t *testing.T) {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "forget-slot-owner",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "forget", Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected ForgetNode to reject slot owner")
	}
}

func TestValidate_ForgetNodeAcceptsNonSlotMember(t *testing.T) {
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(validReplicaReplacementPlan(), ctx); err != nil {
		t.Fatalf("expected ForgetNode repair plan to pass, got %v", err)
	}
}

func TestValidate_RepairRejectsSkippedNextPodOrdinal(t *testing.T) {
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	p := validReplicaReplacementPlan()
	for i := range p.Steps {
		for _, key := range []string{"pod", "targetPod", "replicaPod"} {
			if p.Steps[i].Params[key] == "redis-4" {
				p.Steps[i].Params[key] = "redis-5"
			}
		}
	}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected repair plan to reject skipped nextPodOrdinal")
	}
}

func TestValidate_UsesObservedNodesOverStaleTopology(t *testing.T) {
	ctx := ValidationContext{
		Spec:     spec(),
		Topology: topology(),
		ObservedNodes: []ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "node-0", Role: "master", Slots: "0-8191", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "node-1", Role: "replica", MasterID: "node-0", MasterPod: "redis-0", Ready: true},
		},
		NextPodOrdinal: 4,
	}
	if err := NewValidator().Validate(validReplicaReplacementPlan(), ctx); err == nil {
		t.Fatal("expected observed nodes to override stale two-shard topology")
	}
}

func TestValidate_DeleteNodeRejectsActiveMember(t *testing.T) {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "delete-active-member",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "delete", Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected DeleteNode to reject active Redis member")
	}
}

func TestValidate_DeleteNodeRejectsUnknownNode(t *testing.T) {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "delete-unknown",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "delete", Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-9"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected DeleteNode to reject unknown node")
	}
}

func TestValidate_DeleteNodeAcceptsForgottenNode(t *testing.T) {
	p := validReplicaReplacementPlan()
	verify := p.Steps[len(p.Steps)-1]
	p.Steps = append(p.Steps[:len(p.Steps)-1], Step{ID: "delete", Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}}, verify)
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err != nil {
		t.Fatalf("expected DeleteNode after ForgetNode to pass, got %v", err)
	}
}

func TestValidate_DeleteNodeAcceptsNeverJoinedNode(t *testing.T) {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "delete-never-joined",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "ensure", Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "delete", Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err != nil {
		t.Fatalf("expected DeleteNode of never-joined node to pass, got %v", err)
	}
}

func TestValidate_ForgetNodeRejectsLeavingSlotMasterWithoutReplica(t *testing.T) {
	p := &Plan{
		DSLVersion:       DSLVersion,
		PlanID:           "delete-last-replica",
		TargetGeneration: 1,
		Steps: []Step{
			{ID: "forget", Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "delete", Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "verify", Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	ctx := ValidationContext{Spec: spec(), Topology: topology(), NextPodOrdinal: 4}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected deleting the last replica to fail")
	}
}

func TestValidate_MeetNodeRejectsSelfMeet(t *testing.T) {
	p := validCreatePlan()
	p.Steps[stepIndex(t, p, "meet-redis-1")].Params["targetPod"] = "redis-0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected self MeetNode to fail")
	}
}

func TestValidate_BadDSLVersion(t *testing.T) {
	p := validCreatePlan()
	p.DSLVersion = "wrong"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for bad dslVersion")
	}
}

func TestValidate_TargetGenerationMismatch(t *testing.T) {
	p := validCreatePlan()
	p.TargetGeneration = 2
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for targetGeneration mismatch")
	}
}

func TestValidate_UnknownAction(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Action = "RunCommand"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for non-whitelisted action")
	}
}

func TestValidate_DuplicateStepID(t *testing.T) {
	p := validCreatePlan()
	p.Steps[1].ID = "ensure-redis-0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for duplicate step id")
	}
}

func TestValidate_NamespaceMismatch(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["namespace"] = "other"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for namespace mismatch")
	}
}

func TestValidate_RejectsActionSchemaErrors(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Plan)
	}{
		{
			name: "missing string",
			edit: func(p *Plan) { delete(p.Steps[0].Params, "image") },
		},
		{
			name: "wrong string type",
			edit: func(p *Plan) { p.Steps[0].Params["image"] = 7 },
		},
		{
			name: "empty string",
			edit: func(p *Plan) { p.Steps[0].Params["image"] = "" },
		},
		{
			name: "bad pod name",
			edit: func(p *Plan) { p.Steps[0].Params["pod"] = "example-0" },
		},
		{
			name: "verify integer missing",
			edit: func(p *Plan) { delete(p.Steps[stepIndex(t, p, "verify")].Params, "expectedShards") },
		},
		{
			name: "verify integer wrong type",
			edit: func(p *Plan) { p.Steps[stepIndex(t, p, "verify")].Params["expectedShards"] = "2" },
		},
		{
			name: "verify integer float",
			edit: func(p *Plan) { p.Steps[stepIndex(t, p, "verify")].Params["expectedShards"] = 2.5 },
		},
		{
			name: "verify bool missing",
			edit: func(p *Plan) { delete(p.Steps[stepIndex(t, p, "verify")].Params, "requireClusterStateOk") },
		},
		{
			name: "verify bool wrong type",
			edit: func(p *Plan) { p.Steps[stepIndex(t, p, "verify")].Params["requireClusterStateOk"] = "true" },
		},
		{
			name: "verify bool false",
			edit: func(p *Plan) { p.Steps[stepIndex(t, p, "verify")].Params["requireClusterStateOk"] = false },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validCreatePlan()
			tt.edit(p)
			if err := NewValidator().Validate(p, spec()); err == nil {
				t.Fatal("expected schema error")
			}
		})
	}
}

func TestValidateStepSchema_Actions(t *testing.T) {
	valid := []Step{
		{Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
		{Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
		{Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
		{Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
		{Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-1"}},
		{Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "0-1"}},
		{Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
		{Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "node-1"}},
		{Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
		{Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
	}
	for _, step := range valid {
		if err := validateStepSchema(step); err != nil {
			t.Fatalf("%s should pass schema validation: %v", step.Action, err)
		}
	}

	invalidForget := Step{Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": ""}}
	if err := validateStepSchema(invalidForget); err == nil {
		t.Fatal("expected empty lastKnownNodeId to fail")
	}
}

func TestValidateStepSchema_ActionFailures(t *testing.T) {
	tests := []struct {
		name string
		step Step
	}{
		{name: "EnsureNode missing pod", step: Step{Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "image": "redis:7.2", "memorySize": "2Gi"}}},
		{name: "EnsureNode bad pod", step: Step{Action: ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "example-0", "image": "redis:7.2", "memorySize": "2Gi"}}},
		{name: "WaitNodeReady missing pod", step: Step{Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example"}}},
		{name: "WaitNodeReady bad pod", step: Step{Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "example-0"}}},
		{name: "MeetNode missing targetPod", step: Step{Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0"}}},
		{name: "MeetNode bad sourcePod", step: Step{Action: ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "example-0", "targetPod": "redis-1"}}},
		{name: "ReplicateNode missing replicaPod", step: Step{Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0"}}},
		{name: "ReplicateNode bad masterPod", step: Step{Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "example-0", "replicaPod": "redis-1"}}},
		{name: "AddSlots missing slots", step: Step{Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0"}}},
		{name: "AddSlots bad pod", step: Step{Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "example-0", "slots": "0-1"}}},
		{name: "AddSlots invalid slots", step: Step{Action: ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-99999"}}},
		{name: "MigrateSlots missing targetPod", step: Step{Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "slots": "0-1"}}},
		{name: "MigrateSlots bad sourcePod", step: Step{Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "example-0", "targetPod": "redis-2", "slots": "0-1"}}},
		{name: "MigrateSlots invalid slots", step: Step{Action: ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "bad"}}},
		{name: "ForgetNode missing pod", step: Step{Action: ActionForgetNode, Params: map[string]any{"namespace": "example"}}},
		{name: "ForgetNode bad pod", step: Step{Action: ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "example-1"}}},
		{name: "DeleteNode missing pod", step: Step{Action: ActionDeleteNode, Params: map[string]any{"namespace": "example"}}},
		{name: "DeleteNode bad pod", step: Step{Action: ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": "example-1"}}},
		{name: "VerifyCluster missing expectedShards", step: Step{Action: ActionVerifyCluster, Params: map[string]any{"expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}}},
		{name: "VerifyCluster false bool", step: Step{Action: ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": false, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateStepSchema(tt.step); err == nil {
				t.Fatal("expected schema error")
			}
		})
	}
}

func TestValidate_EnsureNodeCount(t *testing.T) {
	s := spec()
	s.ReplicasPerShard = 2 // want 6 nodes, plan has 4
	if err := NewValidator().Validate(validCreatePlan(), s); err == nil {
		t.Fatal("expected error for EnsureNode count mismatch")
	}
}

func TestValidate_ImageMismatch(t *testing.T) {
	p := validCreatePlan()
	p.Steps[0].Params["image"] = "redis:7.0"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for image mismatch")
	}
}

func TestValidate_AddSlotsIncompleteCoverage(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["slots"] = "0-4095"
	p.Steps[15].Params["slots"] = "8192-16383"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for incomplete slot coverage")
	}
}

func TestValidate_AddSlotsOverlap(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["slots"] = "0-8192"
	p.Steps[15].Params["slots"] = "8192-16383"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for overlapping slots")
	}
}

func TestValidate_AddSlotsUndeclaredPod(t *testing.T) {
	p := validCreatePlan()
	p.Steps[14].Params["pod"] = "redis-9"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for AddSlots targeting undeclared pod")
	}
}

func TestValidate_CreateRejectsAddSlotsBeforeReplica(t *testing.T) {
	p := validCreatePlan()
	addSlots := p.Steps[stepIndex(t, p, "add-slots-redis-0")]
	p.Steps = append(p.Steps[:11], append([]Step{addSlots}, p.Steps[11:13]...)...)
	p.Steps = append(p.Steps, validCreatePlan().Steps[14:]...)
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for AddSlots before ReplicateNode")
	}
}

func TestValidate_CreateRejectsReplicatingSlotOwner(t *testing.T) {
	p := validCreatePlan()
	verify := p.Steps[len(p.Steps)-1]
	p.Steps = append(p.Steps[:len(p.Steps)-1], Step{ID: "bad-replicate-slot-owner", Action: ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-0"}}, verify)
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for ReplicateNode targeting a slot owner")
	}
}

func TestValidate_CreateRejectsAddSlotsToReplica(t *testing.T) {
	p := validCreatePlan()
	p.Steps[stepIndex(t, p, "add-slots-redis-0")].Params["pod"] = "redis-1"
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for AddSlots targeting a replica")
	}
}

func TestValidate_CreateRejectsMeetBeforeReady(t *testing.T) {
	p := validCreatePlan()
	wait := stepIndex(t, p, "wait-redis-1")
	p.Steps = append(p.Steps[:wait], p.Steps[wait+1:]...)
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error for MeetNode before WaitNodeReady")
	}
}

func TestValidate_LastStepNotVerify(t *testing.T) {
	p := validCreatePlan()
	p.Steps = append(p.Steps, Step{ID: "wait-again", Action: ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}})
	if err := NewValidator().Validate(p, spec()); err == nil {
		t.Fatal("expected error when last step is not VerifyCluster")
	}
}

func TestParseSlots(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"0-2", 3},
		{"0,1,2", 3},
		{"0-1,3-4", 4},
		{"5", 1},
	}
	for _, c := range cases {
		got, err := parseSlots(c.in)
		if err != nil {
			t.Fatalf("parseSlots(%q) error: %v", c.in, err)
		}
		if len(got) != c.want {
			t.Fatalf("parseSlots(%q) = %d slots, want %d", c.in, len(got), c.want)
		}
	}
	if _, err := parseSlots("0-99999"); err == nil {
		t.Fatal("expected error for out-of-bounds slot")
	}
}

func TestValidate_ReplicaScaleOutRejectsReplicateBeforeReady(t *testing.T) {
	p := validReplicaScaleOutPlan()
	replicate := stepIndex(t, p, "replicate-redis-4")
	wait := stepIndex(t, p, "wait-redis-4")
	step := p.Steps[replicate]
	p.Steps = append(p.Steps[:replicate], p.Steps[replicate+1:]...)
	p.Steps = append(p.Steps[:wait], append([]Step{step}, p.Steps[wait:]...)...)
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for ReplicateNode before WaitNodeReady")
	}
}

func TestValidate_ReplicaScaleOutRejectsReplicaAssignedToReplica(t *testing.T) {
	p := validReplicaScaleOutPlan()
	p.Steps[stepIndex(t, p, "replicate-redis-4")].Params["masterPod"] = "redis-1"
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for assigning replica to non-master")
	}
}

func TestValidate_ReplicaScaleOutRejectsUnknownMaster(t *testing.T) {
	p := validReplicaScaleOutPlan()
	p.Steps[stepIndex(t, p, "replicate-redis-4")].Params["masterPod"] = "redis-9"
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for assigning replica to unknown master")
	}
}

func TestValidate_ReplicaScaleOutRejectsVerifyMismatch(t *testing.T) {
	p := validReplicaScaleOutPlan()
	p.Steps[stepIndex(t, p, "verify")].Params["expectedReplicasPerShard"] = 1
	ctx := ValidationContext{Spec: replicaScaleOutSpec(), Topology: topology()}
	if err := NewValidator().Validate(p, ctx); err == nil {
		t.Fatal("expected error for VerifyCluster replica mismatch")
	}
}
