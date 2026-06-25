package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
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

func (e *ActionExecutor) ensureNode(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("ensure node started")
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
	getStart := time.Now()
	logger.Info("getting pod for ensure node", "namespace", ns, "pod", podName)
	err = e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod)
	switch {
	case err == nil:
		logger.Info("pod found for ensure node", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
		return e.reconcileExistingPod(ctx, cluster, pod, image, memorySize, wantBytes)
	case errors.IsNotFound(err):
		logger.Info("pod missing for ensure node", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
		return e.createPod(ctx, cluster, ns, podName, image, memorySize, wantBytes)
	default:
		logger.Error(err, "get pod for ensure node failed", "namespace", ns, "pod", podName, "duration", time.Since(getStart))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}
}

func (e *ActionExecutor) createPod(ctx context.Context, cluster *v1alpha1.RedisCluster, ns, podName, image, memorySize string, wantBytes int64) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	pod := desiredPod(cluster, ns, podName, image, memorySize, wantBytes)
	if err := ctrl.SetControllerReference(cluster, pod, e.Scheme); err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("set owner reference: %v", err)}, err
	}
	start := time.Now()
	logger.Info("creating pod", "namespace", ns, "pod", podName, "image", image, "memorySize", memorySize)
	if err := e.Create(ctx, pod); err != nil {
		logger.Error(err, "create pod failed", "namespace", ns, "pod", podName, "duration", time.Since(start))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("create pod: %v", err)}, err
	}
	logger.Info("pod created", "namespace", ns, "pod", podName, "duration", time.Since(start))
	return running("pod %s/%s created; waiting for pod IP and redis", ns, podName), nil
}

func (e *ActionExecutor) reconcileExistingPod(ctx context.Context, cluster *v1alpha1.RedisCluster, pod *corev1.Pod, image, memorySize string, wantBytes int64) (StepOutcome, error) {
	logger := log.FromContext(ctx).WithValues("namespace", pod.Namespace, "pod", pod.Name)
	logger.Info("reconciling existing pod")
	if !metav1.IsControlledBy(pod, cluster) {
		patched := pod.DeepCopy()
		if err := ctrl.SetControllerReference(cluster, patched, e.Scheme); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("set owner reference: %v", err)}, err
		}
		start := time.Now()
		logger.Info("updating pod owner reference")
		if err := e.Update(ctx, patched); err != nil {
			logger.Error(err, "update pod owner reference failed", "duration", time.Since(start))
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("update pod owner reference: %v", err)}, err
		}
		logger.Info("pod owner reference updated", "duration", time.Since(start))
	}
	if drift := podSpecDrift(pod, image, memorySize); drift != "" {
		logger.Info("pod spec drift detected", "drift", drift)
		return paramErr("pod %s/%s spec drift: %s", pod.Namespace, pod.Name, drift)
	}
	if msg := ensurePodMetadata(pod, cluster.Name, pod.Name, image, memorySize, wantBytes); msg != "" {
		patched := pod.DeepCopy()
		ensurePodMetadata(patched, cluster.Name, patched.Name, image, memorySize, wantBytes)
		start := time.Now()
		logger.Info("updating pod metadata", "reason", msg)
		if err := e.Update(ctx, patched); err != nil {
			logger.Error(err, "update pod metadata failed", "duration", time.Since(start))
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("update pod metadata: %v", err)}, err
		}
		logger.Info("pod metadata updated", "duration", time.Since(start))
	}
	return e.ensureMaxmemory(ctx, pod, wantBytes)
}

func (e *ActionExecutor) ensureMaxmemory(ctx context.Context, pod *corev1.Pod, wantBytes int64) (StepOutcome, error) {
	logger := log.FromContext(ctx).WithValues("namespace", pod.Namespace, "pod", pod.Name)
	addr := podRedisAddr(pod)
	if addr == "" {
		logger.Info("pod has no IP for maxmemory")
		return running("pod %s/%s has no IP yet", pod.Namespace, pod.Name), nil
	}
	logger.Info("ensuring redis maxmemory", "addr", addr, "wantBytes", wantBytes)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		logger.Error(err, "build redis client for maxmemory failed", "addr", addr)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Info("redis ping for maxmemory failed", "addr", addr, "duration", time.Since(pingStart), "error", err)
		return running("redis at %s not ready: %v", addr, err), nil
	}
	logger.Info("redis ping for maxmemory succeeded", "addr", addr, "duration", time.Since(pingStart))
	getStart := time.Now()
	got, err := rc.ConfigGet(ctx, "maxmemory")
	if err != nil {
		logger.Info("config get maxmemory failed", "addr", addr, "duration", time.Since(getStart), "error", err)
		return running("redis at %s CONFIG GET maxmemory failed: %v", addr, err), nil
	}
	logger.Info("config get maxmemory succeeded", "addr", addr, "duration", time.Since(getStart))
	gotBytes, err := strconv.ParseInt(strings.TrimSpace(got), 10, 64)
	if err != nil {
		logger.Error(err, "parse maxmemory failed", "value", got)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse maxmemory %q: %v", got, err)}, err
	}
	if gotBytes == wantBytes {
		logger.Info("maxmemory already matches", "gotBytes", gotBytes)
		return completed("pod %s/%s ready; maxmemory=%d", pod.Namespace, pod.Name, wantBytes), nil
	}
	setStart := time.Now()
	logger.Info("config set maxmemory started", "gotBytes", gotBytes, "wantBytes", wantBytes)
	if err := rc.ConfigSet(ctx, "maxmemory", strconv.FormatInt(wantBytes, 10)); err != nil {
		logger.Error(err, "config set maxmemory failed", "duration", time.Since(setStart))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CONFIG SET maxmemory: %v", err)}, err
	}
	logger.Info("config set maxmemory succeeded", "duration", time.Since(setStart))
	got, err = rc.ConfigGet(ctx, "maxmemory")
	if err != nil {
		logger.Info("config get maxmemory after set failed", "addr", addr, "error", err)
		return running("redis at %s CONFIG GET maxmemory after set failed: %v", addr, err), nil
	}
	gotBytes, err = strconv.ParseInt(strings.TrimSpace(got), 10, 64)
	if err != nil {
		logger.Error(err, "parse maxmemory after set failed", "value", got)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse maxmemory after set %q: %v", got, err)}, err
	}
	if gotBytes != wantBytes {
		logger.Info("maxmemory mismatch after set", "gotBytes", gotBytes, "wantBytes", wantBytes)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("maxmemory mismatch after set: got %d want %d", gotBytes, wantBytes)}, fmt.Errorf("maxmemory mismatch after set: got %d want %d", gotBytes, wantBytes)
	}
	logger.Info("maxmemory set and verified", "gotBytes", gotBytes)
	return completed("pod %s/%s ready; maxmemory set to %d", pod.Namespace, pod.Name, wantBytes), nil
}

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

func podRedisAddr(pod *corev1.Pod) string {
	if pod.Status.PodIP == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", pod.Status.PodIP, redisClientPort)
}

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
