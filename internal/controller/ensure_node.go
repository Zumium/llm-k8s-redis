package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

const (
	redisContainerName = "redis"
	redisDataPath      = "/data"
	redisClientPort    = int32(6379)
	redisBusPort       = int32(16379)

	labelManagedBy = "app.kubernetes.io/managed-by"
	labelName      = "app.kubernetes.io/name"
	labelInstance  = "app.kubernetes.io/instance"
	labelCluster   = "redis.example.com/cluster"
	labelPod       = "redis.example.com/pod"

	annImage      = "redis.example.com/image"
	annMemorySize = "redis.example.com/memory-size"
	annMaxmemory  = "redis.example.com/maxmemory-bytes"
)

// ensureNode is the executor for plan.ActionEnsureNode. It is idempotent:
//
//  1. Validate params (namespace/pod/image/memorySize) and re-check the
//     safety invariants against the live RedisCluster spec.
//  2. Ensure the Pod exists with the desired spec (image, args, resources,
//     ownerReference, labels, annotations). Missing metadata is patched in;
//     immutable spec drift (image/args/resources) fails the step.
//  3. Once the Pod has an IP and Redis responds to PING, ensure maxmemory
//     matches spec.memorySize via CONFIG GET/SET.
//
// Until step 3 completes the step stays Running so the reconciler retries.
func (e *ActionExecutor) ensureNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
	ns, outcome, err, ok := requireString(step.Params, "namespace")
	if !ok {
		return outcome, err
	}
	podName, outcome, err, ok := requireString(step.Params, "pod")
	if !ok {
		return outcome, err
	}
	image, outcome, err, ok := requireString(step.Params, "image")
	if !ok {
		return outcome, err
	}
	memorySize, outcome, err, ok := requireString(step.Params, "memorySize")
	if !ok {
		return outcome, err
	}

	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}
	if image != cluster.Spec.Image {
		return paramErr("image %q must equal spec.image %q", image, cluster.Spec.Image)
	}
	if memorySize != cluster.Spec.MemorySize {
		return paramErr("memorySize %q must equal spec.memorySize %q", memorySize, cluster.Spec.MemorySize)
	}

	wantBytes, err := memoryBytes(memorySize)
	if err != nil {
		return paramErr("invalid memorySize %q: %v", memorySize, err)
	}

	pod := &corev1.Pod{}
	err = e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod)
	switch {
	case err == nil:
		return e.reconcileExistingPod(ctx, cluster, pod, image, memorySize, wantBytes)
	case errors.IsNotFound(err):
		return e.createPod(ctx, cluster, ns, podName, image, memorySize, wantBytes)
	default:
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}
}

// createPod builds and creates the desired Pod, then returns Running because
// the Pod has no IP yet; maxmemory is configured once Redis is reachable.
func (e *ActionExecutor) createPod(ctx context.Context, cluster *v1alpha1.RedisCluster, ns, podName, image, memorySize string, wantBytes int64) (StepOutcome, error) {
	pod := desiredPod(cluster, ns, podName, image, memorySize, wantBytes)
	if err := ctrl.SetControllerReference(cluster, pod, e.Scheme); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("set owner reference: %v", err)}, err
	}
	if err := e.Create(ctx, pod); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("create pod: %v", err)}, err
	}
	return running("pod %s/%s created; waiting for pod IP and redis", ns, podName), nil
}

// reconcileExistingPod verifies an existing Pod matches the desired state,
// patches missing metadata, then drives the Redis maxmemory check.
func (e *ActionExecutor) reconcileExistingPod(ctx context.Context, cluster *v1alpha1.RedisCluster, pod *corev1.Pod, image, memorySize string, wantBytes int64) (StepOutcome, error) {
	if !metav1.IsControlledBy(pod, cluster) {
		patched := pod.DeepCopy()
		if err := ctrl.SetControllerReference(cluster, patched, e.Scheme); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("set owner reference: %v", err)}, err
		}
		if err := e.Update(ctx, patched); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("update pod owner reference: %v", err)}, err
		}
	}
	if drift := podSpecDrift(pod, image, memorySize); drift != "" {
		return paramErr("pod %s/%s spec drift: %s", pod.Namespace, pod.Name, drift)
	}
	if msg := ensurePodMetadata(pod, cluster.Name, pod.Name, image, memorySize, wantBytes); msg != "" {
		patched := pod.DeepCopy()
		ensurePodMetadata(patched, cluster.Name, patched.Name, image, memorySize, wantBytes)
		if err := e.Update(ctx, patched); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("update pod metadata: %v", err)}, err
		}
	}
	return e.ensureMaxmemory(ctx, pod, wantBytes)
}

// ensureMaxmemory waits for the Pod to have an IP and for Redis to respond,
// then makes sure CONFIG maxmemory equals wantBytes.
func (e *ActionExecutor) ensureMaxmemory(ctx context.Context, pod *corev1.Pod, wantBytes int64) (StepOutcome, error) {
	addr := podRedisAddr(pod)
	if addr == "" {
		return running("pod %s/%s has no IP yet", pod.Namespace, pod.Name), nil
	}
	rc, err := e.RedisFactory(addr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("redis at %s not ready: %v", addr, err), nil
	}
	got, err := rc.ConfigGet(ctx, "maxmemory")
	if err != nil {
		return running("redis at %s CONFIG GET maxmemory failed: %v", addr, err), nil
	}
	gotBytes, err := strconv.ParseInt(strings.TrimSpace(got), 10, 64)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse maxmemory %q: %v", got, err)}, err
	}
	if gotBytes == wantBytes {
		return completed("pod %s/%s ready; maxmemory=%d", pod.Namespace, pod.Name, wantBytes), nil
	}
	if err := rc.ConfigSet(ctx, "maxmemory", strconv.FormatInt(wantBytes, 10)); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CONFIG SET maxmemory: %v", err)}, err
	}
	got, err = rc.ConfigGet(ctx, "maxmemory")
	if err != nil {
		return running("redis at %s CONFIG GET maxmemory after set failed: %v", addr, err), nil
	}
	gotBytes, err = strconv.ParseInt(strings.TrimSpace(got), 10, 64)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse maxmemory after set %q: %v", got, err)}, err
	}
	if gotBytes != wantBytes {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("maxmemory mismatch after set: got %d want %d", gotBytes, wantBytes)}, fmt.Errorf("maxmemory mismatch after set: got %d want %d", gotBytes, wantBytes)
	}
	return completed("pod %s/%s ready; maxmemory set to %d", pod.Namespace, pod.Name, wantBytes), nil
}

// desiredPod builds the Pod object the controller manages for a Redis node.
func desiredPod(cluster *v1alpha1.RedisCluster, ns, podName, image, memorySize string, wantBytes int64) *corev1.Pod {
	labels := nodeLabels(cluster.Name, podName)
	annotations := nodeAnnotations(image, memorySize, wantBytes)
	q := resource.MustParse(memorySize)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   ns,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    redisContainerName,
				Image:   image,
				Command: redisCommand(),
				Args:    redisArgs(),
				Ports: []corev1.ContainerPort{
					{Name: "client", ContainerPort: redisClientPort, Protocol: corev1.ProtocolTCP},
					{Name: "bus", ContainerPort: redisBusPort, Protocol: corev1.ProtocolTCP},
				},
				WorkingDir: redisDataPath,
				VolumeMounts: []corev1.VolumeMount{{
					Name: "data", MountPath: redisDataPath,
				}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: q},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: q},
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{
							Command: []string{"redis-cli", "-p", strconv.Itoa(int(redisClientPort)), "ping"},
						},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       5,
				},
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
}

func redisCommand() []string {
	return []string{"redis-server"}
}

func redisArgs() []string {
	return []string{
		"--port", strconv.Itoa(int(redisClientPort)),
		"--cluster-enabled", "yes",
		"--cluster-config-file", "nodes.conf",
		"--cluster-node-timeout", "5000",
		"--appendonly", "yes",
	}
}

func nodeLabels(clusterName, podName string) map[string]string {
	return map[string]string{
		labelManagedBy: "redis-cluster-controller",
		labelName:      "redis",
		labelInstance:  clusterName,
		labelCluster:   clusterName,
		labelPod:       podName,
	}
}

func nodeAnnotations(image, memorySize string, wantBytes int64) map[string]string {
	return map[string]string{
		annImage:      image,
		annMemorySize: memorySize,
		annMaxmemory:  strconv.FormatInt(wantBytes, 10),
	}
}

// podSpecDrift returns a non-empty message describing immutable spec drift
// (image, command, args, resources) that EnsureNode will not auto-fix, or ""
// if the spec matches.
func podSpecDrift(pod *corev1.Pod, image, memorySize string) string {
	c := containerOf(pod)
	if c == nil {
		return "missing redis container"
	}
	if c.Image != image {
		return fmt.Sprintf("image %q != desired %q", c.Image, image)
	}
	if !stringSliceEqual(c.Command, redisCommand()) {
		return "command mismatch"
	}
	if !stringSliceEqual(c.Args, redisArgs()) {
		return "args mismatch"
	}
	wantQ := resource.MustParse(memorySize)
	if mem, ok := c.Resources.Requests[corev1.ResourceMemory]; !ok || !mem.Equal(wantQ) {
		return "memory request mismatch"
	}
	if mem, ok := c.Resources.Limits[corev1.ResourceMemory]; !ok || !mem.Equal(wantQ) {
		return "memory limit mismatch"
	}
	return ""
}

// ensurePodMetadata returns "" when labels/annotations are already complete,
// otherwise a short description of what was missing. It mutates pod in place
// to fill the gaps.
func ensurePodMetadata(pod *corev1.Pod, clusterName, podName, image, memorySize string, wantBytes int64) string {
	want := nodeLabels(clusterName, podName)
	missing := false
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	for k, v := range want {
		if pod.Labels[k] != v {
			pod.Labels[k] = v
			missing = true
		}
	}
	wantAnn := nodeAnnotations(image, memorySize, wantBytes)
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	for k, v := range wantAnn {
		if pod.Annotations[k] != v {
			pod.Annotations[k] = v
			missing = true
		}
	}
	if missing {
		return "metadata patched"
	}
	return ""
}

func containerOf(pod *corev1.Pod) *corev1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == redisContainerName {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

// podRedisAddr returns host:port for the Pod's Redis client port, or "" if
// the Pod has no IP yet.
func podRedisAddr(pod *corev1.Pod) string {
	if pod.Status.PodIP == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", pod.Status.PodIP, redisClientPort)
}

// memoryBytes parses a Kubernetes quantity string (e.g. "2Gi") into bytes.
func memoryBytes(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	return q.Value(), nil
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
