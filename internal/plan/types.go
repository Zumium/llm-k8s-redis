// Package plan defines the LLM-generated Plan DSL types and the deterministic
// Validator that enforces safety invariants before a plan is persisted and
// executed by the controller.
//
// The controller treats plans as immutable once accepted: a plan is validated
// up front, persisted into status.activePlan, and then executed step by step.
// This package deliberately depends only on primitive spec fields (ClusterSpec)
// so it can be unit tested without a K8S client.
package plan

// DSLVersion is the version tag carried by every plan.
const DSLVersion = "redis.ops/v1alpha1"

// ActionType is the kind of a single step. Only whitelisted actions are
// permitted; the controller exposes no generic command execution action.
type ActionType string

const (
	ActionEnsureNode    ActionType = "EnsureNode"
	ActionWaitNodeReady ActionType = "WaitNodeReady"
	ActionMeetNode      ActionType = "MeetNode"
	ActionReplicateNode ActionType = "ReplicateNode"
	ActionAddSlots      ActionType = "AddSlots"
	ActionMigrateSlots  ActionType = "MigrateSlots"
	ActionForgetNode    ActionType = "ForgetNode"
	ActionDeleteNode    ActionType = "DeleteNode"
	ActionVerifyCluster ActionType = "VerifyCluster"
)

// StepState is the execution state of a single step.
type StepState string

const (
	StepStatePending   StepState = "Pending"
	StepStateRunning   StepState = "Running"
	StepStateCompleted StepState = "Completed"
	StepStateFailed    StepState = "Failed"
)

// PlanState is the aggregate state of a plan.
type PlanState string

const (
	PlanStatePending    PlanState = "Pending"
	PlanStateRunning    PlanState = "Running"
	PlanStateCompleted  PlanState = "Completed"
	PlanStateFailed     PlanState = "Failed"
	PlanStateSuperseded PlanState = "Superseded"
)

// Plan is the LLM-generated, JSON-encoded reconciliation plan.
type Plan struct {
	DSLVersion       string `json:"dslVersion"`
	PlanID           string `json:"planId"`
	TargetGeneration int64  `json:"targetGeneration"`
	Summary          string `json:"summary,omitempty"`
	Steps            []Step `json:"steps"`
}

// Step is a single ordered action within a plan.
type Step struct {
	ID     string         `json:"id"`
	Action ActionType     `json:"action"`
	Params map[string]any `json:"params"`
}

// ClusterSpec is the projection of a RedisCluster CR needed to validate a plan.
// It is passed by value to keep this package free of K8S client dependencies.
type ClusterSpec struct {
	Name             string
	Generation       int64
	Shards           int32
	ReplicasPerShard int32
	Image            string
	MemorySize       string
}

type ObservedNode struct {
	Pod       string
	PodExists bool
	RedisSeen bool
	NodeID    string
	Role      string
	Slots     string
	MasterID  string
	MasterPod string
	Ready     bool
	Deleting  bool
	Flags     []string
	LinkState string
}

type ClusterTopology struct {
	Shards []ShardTopology
}

type ShardTopology struct {
	ID       string
	Master   NodeTopology
	Replicas []NodeTopology
}

type NodeTopology struct {
	Pod    string
	NodeID string
	Slots  string
	Ready  bool
}
