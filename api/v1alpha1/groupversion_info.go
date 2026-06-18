// Package v1alpha1 contains the RedisCluster Custom Resource Definition types.
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group version used to register RedisCluster objects.
	GroupVersion = schema.GroupVersion{Group: "redis.example.com", Version: "v1alpha1"}

	// SchemeBuilder registers RedisCluster types into the scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds RedisCluster types to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&RedisCluster{}, &RedisClusterList{})
}

// Resource returns a GroupResource for the given resource name.
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}
