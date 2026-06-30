package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RedisClusterSpec defines the desired state of a Redis Cluster.
type RedisClusterSpec struct {
	// Shards is the number of shards (masters). Minimum 1.
	// +kubebuilder:validation:Minimum=1
	Shards int32 `json:"shards"`

	// ReplicasPerShard is the number of replicas for each shard's master. Minimum 1.
	// +kubebuilder:validation:Minimum=1
	ReplicasPerShard int32 `json:"replicasPerShard"`

	// Image is the Redis container image used for every node.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// MemorySize is the per-node Redis memory limit, e.g. "2Gi".
	// +kubebuilder:validation:MinLength=1
	MemorySize string `json:"memorySize"`
}

// NodeTopology describes a single Redis node as observed by the controller.
type NodeTopology struct {
	// Pod is the K8S Pod name hosting this node.
	Pod string `json:"pod,omitempty"`
	// NodeID is the Redis Cluster node id, discovered from the live cluster.
	NodeID string `json:"nodeId,omitempty"`
	// Slots is the slot range(s) owned by this node, e.g. "0-8191".
	Slots string `json:"slots,omitempty"`
	// Ready is true when the Pod is Ready and Redis responds to PING.
	Ready bool `json:"ready,omitempty"`
}

// ShardTopology describes a single shard: one master and its replicas.
type ShardTopology struct {
	// ID is the shard identifier, e.g. "shard-0".
	ID string `json:"id,omitempty"`
	// Master is the master node of this shard.
	Master NodeTopology `json:"master,omitempty"`
	// Replicas are the replica nodes backing this shard's master.
	Replicas []NodeTopology `json:"replicas,omitempty"`
}

// ClusterTopology is the observed Redis Cluster topology.
type ClusterTopology struct {
	Shards []ShardTopology `json:"shards,omitempty"`
}

// ObservedNode records the current Kubernetes and Redis view of one managed node.
type ObservedNode struct {
	Pod       string   `json:"pod,omitempty"`
	PodExists bool     `json:"podExists,omitempty"`
	RedisSeen bool     `json:"redisSeen,omitempty"`
	NodeID    string   `json:"nodeId,omitempty"`
	Role      string   `json:"role,omitempty"`
	Slots     string   `json:"slots,omitempty"`
	MasterID  string   `json:"masterId,omitempty"`
	MasterPod string   `json:"masterPod,omitempty"`
	Ready     bool     `json:"ready,omitempty"`
	Deleting  bool     `json:"deleting,omitempty"`
	Flags     []string `json:"flags,omitempty"`
	LinkState string   `json:"linkState,omitempty"`
}

// StepStatus is the persisted state of a single plan step.
type StepStatus struct {
	// ID is the step identifier.
	ID string `json:"id,omitempty"`
	// Action is the action type executed by this step.
	Action string `json:"action,omitempty"`
	// Status is the execution state: Pending, Running, Completed, Failed.
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	Status string `json:"status,omitempty"`
	// Message is a human-readable detail about the step state.
	Message string `json:"message,omitempty"`
	// Params are the action parameters, persisted as raw JSON so executors can
	// resume after a controller restart without re-querying the planner.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Params apiextensionsv1.JSON `json:"params,omitempty"`
	// LastTransitionTime is the last time Status changed.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// PlanStatus is the persisted form of the active reconciliation plan.
type PlanStatus struct {
	// ID is the plan identifier.
	ID string `json:"id,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed;Superseded
	Status string `json:"status,omitempty"`
	// TargetGeneration is the RedisCluster generation this plan targets.
	TargetGeneration int64 `json:"targetGeneration,omitempty"`
	// Summary is the human-readable plan summary produced by the planner.
	Summary string `json:"summary,omitempty"`
	// CurrentStep is the id of the step currently being executed.
	CurrentStep string `json:"currentStep,omitempty"`
	// Steps is the ordered list of step states.
	Steps []StepStatus `json:"steps,omitempty"`
}

// RedisClusterStatus defines the observed state of a RedisCluster.
type RedisClusterStatus struct {
	// ObservedGeneration is the most recent RedisCluster generation handled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions summarize the cluster's readiness and plan acceptance.
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	// Topology is the last observed Redis Cluster topology.
	Topology *ClusterTopology `json:"topology,omitempty"`
	// TopologyObservedAt is the last time Topology was refreshed from Redis.
	TopologyObservedAt metav1.Time `json:"topologyObservedAt,omitempty"`
	// NextPodOrdinal is the next redis-N ordinal that may be allocated.
	NextPodOrdinal int32 `json:"nextPodOrdinal,omitempty"`
	// ObservedNodes is the last full managed node inventory used for planning.
	ObservedNodes []ObservedNode `json:"observedNodes,omitempty"`
	// ActivePlan is the plan currently being executed, if any.
	ActivePlan *PlanStatus `json:"activePlan,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,path=redisclusters,shortName=rcluster;rclusters
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shards`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicasPerShard`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`

// RedisCluster is the Schema for the redisclusters API. It is cluster-scoped:
// each RedisCluster owns a dedicated namespace whose name equals the cluster name.
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster.
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}
