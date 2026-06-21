// Package planner implements the LLM-driven plan generation layer.
//
// The planner sits between the controller and the LLM: it takes a domain-level
// Request (cluster spec + observed state), builds a prompt, calls an llm.Client,
// parses the JSON response into a plan.Plan, and returns it. The controller's
// deterministic Validator is the final safety gate — the planner never bypasses
// it.
package planner

import (
	"context"
	"errors"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// Planner is the domain-level interface the controller depends on. It produces
// a plan.Plan for the given Request. The returned plan is NOT trusted: the
// controller runs it through the Validator before persisting it.
type Planner interface {
	Plan(ctx context.Context, req Request) (*plan.Plan, error)
}

// Request carries everything the planner needs to generate a plan: the desired
// spec and the observed cluster state.
type Request struct {
	// Spec is the projection of the cluster's spec used for plan generation
	// and validation.
	Spec plan.ClusterSpec

	// ObservedState is the last-known Redis/K8S state, used by the planner
	// to generate incremental (non-Create) plans.
	ObservedState ObservedState
}

// ObservedState is the cluster snapshot available to the planner.
type ObservedState struct {
	// ActivePlan is the previously executed plan, if any. Useful for
	// re-planning after a failure.
	ActivePlan     *v1alpha1.PlanStatus
	NextPodOrdinal int
	Nodes          []ObservedNode
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

// ErrNotConfigured is returned by NoopPlanner.
var ErrNotConfigured = errors.New("planner not configured; provide a Planner implementation")

// NoopPlanner is a stub used when no real planner is wired. It always returns
// ErrNotConfigured, causing the controller to mark the cluster Failed.
type NoopPlanner struct{}

// Plan implements Planner by returning ErrNotConfigured.
func (NoopPlanner) Plan(_ context.Context, _ Request) (*plan.Plan, error) {
	return nil, ErrNotConfigured
}
