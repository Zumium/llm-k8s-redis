// Package controller implements the RedisCluster reconciler.
//
// RBAC markers are declared at package scope because controller-gen v0.21
// treats +kubebuilder:rbac as a package-level marker.
//
// +kubebuilder:rbac:groups=redis.example.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.example.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.example.com,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
package controller
