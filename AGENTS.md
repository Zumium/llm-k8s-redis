# AGENTS.md

K8S controller for Redis Cluster lifecycle management. An LLM-generated plan
(DSL) drives reconciliation; the controller validates plans deterministically
and executes one step per reconcile. Go + controller-runtime, cluster-scoped CRD.

## Philosophy

Simpler code is better code. Code that a junior SDE can understand easily is
good code.

## Commands

```bash
make build         # generate + fmt + vet + build manager binary
make test          # generate + fmt + vet + go test ./... (fake-client only, no envtest)
make run           # run controller against current kubeconfig
make manifests     # regenerate CRD (config/crd/bases) + RBAC (config/rbac) via controller-gen
make generate      # regenerate api/v1alpha1/zz_generated.deepcopy.go
make tidy          # go mod tidy
```

Run a single test: `go test ./internal/planner/ -run TestLLMPlanner_ValidPlan -v`

## Toolchain gotchas

- **GOPROXY**: `go env -w GOPROXY=https://goproxy.cn,direct` is required in this
  environment. Without it, `go mod tidy` hangs on downloads. The setting is
  already persisted via `go env -w`; do not "fix" it back to `direct`.
- **controller-gen** is not pre-installed. `make generate` / `make manifests`
  auto-install it to `$(go env GOPATH)/bin` on first run. If deepcopy output
  looks incomplete (missing `DeepCopyInto` for sub-structs), the
  `+kubebuilder:object:generate=true` marker in `api/v1alpha1/doc.go` is missing
  or the file was deleted — regenerate after restoring it.
- **Go 1.26** required (see `go.mod`). Older Go toolchains will fail.

## Architecture

Layering (each arrow is a compile-time interface boundary):

```
controller.RedisClusterReconciler
  -> planner.Planner          (domain interface, internal/planner/types.go)
       -> planner.LLMPlanner
            -> planner.Client
                 -> planner.OpenAIClient (official OpenAI Go SDK, internal/planner/openai_client.go)
plan.Validator                (deterministic safety gate, internal/plan/validator.go)
```

- **LLM config is ConfigMap-driven at startup.** `planner.LoadConfigMap` reads
  `baseUrl`/`apiKey`/`model`/`maxTokens`/`temperature`/`reasoningEffort` from a ConfigMap
  (default `llm-config` in the controller's namespace) and builds a
  `planner.OpenAIClient` (backed by `github.com/openai/openai-go/v3`). It does
  not hot-reload; rotate model/baseUrl/apiKey by restarting the controller after
  editing the ConfigMap. Flags: `--llm-configmap-name`,
  `--llm-configmap-namespace`, `--disable-llm-planner`.
- **`planner.Client` is the provider-agnostic interface; `planner.OpenAIClient` is the
  only implementation.** The OpenAI SDK is OpenAI-compatible, so `baseUrl` can
  point at Azure OpenAI, vLLM, Ollama, or Anthropic's OpenAI-compatible endpoint.
  A native Anthropic Messages API adapter is not yet built.
- **Planner output is never trusted.** `LLMPlanner` fixes `DSLVersion` and
  `TargetGeneration` but every other field is re-checked by `plan.Validator`
  before the plan is persisted to `status.activePlan`.
- **One step per reconcile.** The reconciler executes at most one pending step,
  updates status, and returns. Do not loop over steps in a single reconcile.
- **Controller is cluster-scoped.** Each `RedisCluster` owns a namespace with
  the same name; the namespace gets an `ownerReference` so K8S GCs it on delete.
  A finalizer gates CR deletion until the namespace is gone.

## Conventions that differ from defaults

- **`StepStatus.Params` is `apiextensionsv1.JSON`, not `map[string]any`.** CRD
  schemas cannot express `any`; raw JSON is the storage type. `controller.go`
  marshals/unmarshals at the `planToStatus`/`statusToPlan` boundary. Do not
  change the API field type back to a map — `make manifests` will fail.
- **Use `mgr.GetEventRecorder(name)`, not `GetEventRecorderFor`.** The latter is
  deprecated in controller-runtime v0.24. The new `events.EventRecorder.Eventf`
  signature is `(regarding, related, eventtype, reason, action, note, args...)`.
- **RBAC markers are package-scoped in controller-gen v0.21.** Put
  `+kubebuilder:rbac` markers in `internal/controller/doc.go` (package doc
  comment), NOT on the `Reconcile` method — method-scoped rbac markers are
  silently ignored and `role.yaml` won't be generated.
- **No comments in code unless asked** (per repo convention).
- **Design docs are authoritative**: `docs/DESIGN.md` (CRD + principles),
  `docs/OPERATIONS.md` (Plan structure + per-operation safety checks),
  `docs/ACTIONS.md` (9 whitelisted actions + params + safety invariants). When
  adding an action or operation, update the doc and the Validator together.

## Test environment

A known-good test environment used for development. Keep this section in sync
with `cluster-configs/` and the live `llm-config` ConfigMap.

- **Host**: <ip> (user <user>, SSH key-based, sudo passwordless)
- **KIND cluster**: `kind`, 1 control-plane + 5 workers, K8s v1.36.1
  - Provisioned from `cluster-configs/kind-1-control-plane-5-workers.yaml`
  - Managed with `<home-path>go/bin/kind` (v0.32.0)
- **Working tree on remote**: `~/projects/llm-k8s-redis` (not a git checkout;
  it's a tarball-synced copy of this repo). Re-sync from WSL with:
  ```bash
  tar --exclude='.git' --exclude='bin' --exclude='cover.out' -czf - . \
    | ssh <user>@<ip> 'tar -xzf - -C ~/projects/llm-k8s-redis'
  ```
- **Fast controller image deployment**: when only the controller binary changed,
  build the Linux binary locally, copy just `Dockerfile` + `manager`, build on
  the remote Docker daemon, load into KIND, then restart the manager:
  ```bash
  mkdir -p /tmp/llm-k8s-redis-image
  env GOCACHE=/tmp/llm-k8s-redis-go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /tmp/llm-k8s-redis-image/manager ./cmd/manager

  ssh <user>@<ip> \
    'rm -rf /tmp/llm-k8s-redis-image && mkdir -p /tmp/llm-k8s-redis-image'
  scp Dockerfile /tmp/llm-k8s-redis-image/manager \
    <user>@<ip>:/tmp/llm-k8s-redis-image/

  ssh <user>@<ip> \
    'cd /tmp/llm-k8s-redis-image && docker build -t controller:latest .'
  ssh <user>@<ip> \
    '<home-path>go/bin/kind load docker-image controller:latest --name kind'
  ssh <user>@<ip> \
    'kubectl -n redis-cluster-system rollout restart deployment/controller-manager &&
     kubectl -n redis-cluster-system rollout status deployment/controller-manager --timeout=120s &&
     kubectl -n redis-cluster-system get deploy,pods -o wide'
  ```
  This path does not touch the live `llm-config` ConfigMap.
- **LLM** (live ConfigMap `llm-config` in namespace `redis-cluster-system`):
  - `provider: openai`
  - `baseUrl: https://api.deepseek.com/v1`
  - `model: deepseek-v4-flash`
  - `reasoningEffort: max`  (DeepSeek's "max 思考力度"; controller maps to top-level `reasoning_effort=max`)
  - `apiKey`: **not committed**. Stored only in the live cluster ConfigMap;
    restart the controller after rotating it.
    Inspect with `kubectl -n redis-cluster-system edit cm llm-config`.

## Module path caveat

`go.mod` declares `module github.com/example/llm-k8s-redis` — a placeholder,
not a real GitHub repo. Imports throughout the tree use this path. Do not
rewrite imports to a different path without user instruction.

## What is NOT implemented yet

- Anthropic-native `planner.Client` adapter. `OpenAIClient` can still use an
  Anthropic OpenAI-compatible `baseUrl`; the `provider` key is ignored.
- Executor actions for `MigrateSlots`, `ForgetNode`, and `DeleteNode`.
  `ActionExecutor` dispatches six of the nine whitelisted actions (EnsureNode,
  WaitNodeReady, MeetNode, ReplicateNode, AddSlots, VerifyCluster). The
  remaining three are whitelisted by the Validator but lack executor
  implementations — files `migrate_slots.go`, `forget_node.go`, and
  `delete_node.go` do not yet exist in `internal/controller/`.
- Validators for shard `ScaleOut` / `ScaleIn` / `UpdateMemorySize` / `Delete`.
  Only `Create` and `ReplicaScaleOut` (increasing `replicasPerShard` on an
  existing cluster) have operation-specific safety checks today. Shard-level
  ScaleOut, any ScaleIn, UpdateMemorySize, and cluster Delete have no
  validator path.
- `envtest`-based integration tests; all tests use `fake.Client`.

## Verification before declaring done

After any change touching `api/`, `internal/`, or `cmd/`:

```bash
make manifests generate   # regen CRD/RBAC + deepcopy
go build ./...            # must be clean
go test ./...             # must pass
go vet ./...
```
