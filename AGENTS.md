# AGENTS.md

K8S controller for Redis Cluster lifecycle management. An LLM-generated plan
(DSL) drives reconciliation; the controller validates plans deterministically
and executes one step per reconcile. Go + controller-runtime, cluster-scoped CRD.

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
       -> planner.DynamicPlanner  (resolves client/model per call from a source)
            -> llm.Client         (provider-agnostic, internal/llm/types.go)
                 -> llm.OpenAIClient (official OpenAI Go SDK, internal/llm/openai_client.go)
                 -> llm.ConfigMapSource (loads baseUrl/apiKey/model from a ConfigMap, hot-reloads)
plan.Validator                (deterministic safety gate, internal/plan/validator.go)
```

- **LLM config is ConfigMap-driven.** `llm.ConfigMapSource` (a manager.Runnable)
  reads `provider`/`baseUrl`/`apiKey`/`model`/`maxTokens`/`temperature` from a
  ConfigMap (default `llm-config` in the controller's namespace) and builds an
  `llm.OpenAIClient` (backed by `github.com/openai/openai-go/v3`). It polls every
  15s for hot-reload. `planner.DynamicPlanner` reads the current client/model on
  every `Plan()` call. Flags: `--llm-configmap-name`, `--llm-configmap-namespace`,
  `--disable-llm-planner`.
- **`llm.Client` is the provider-agnostic interface; `llm.OpenAIClient` is the
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

## Module path caveat

`go.mod` declares `module github.com/example/llm-k8s-redis` — a placeholder,
not a real GitHub repo. Imports throughout the tree use this path. Do not
rewrite imports to a different path without user instruction.

## What is NOT implemented yet

- Anthropic-native `llm.Client` adapter (the OpenAI SDK `OpenAIClient` works
  with Anthropic's OpenAI-compatible endpoint via `provider: anthropic`; a
  native Messages API adapter is not yet built).
- Redis action executors (`EnsureNode`, `AddSlots`, etc.) — `NoopExecutor`
  returns `ErrExecutorNotConfigured`, so active plans currently fail at first
  step.
- Validators for `Delete` / `ScaleOut` / `ScaleIn` / `UpdateMemorySize` — only
  `Create` has operation-specific safety checks today.
- `envtest`-based integration tests; all tests use `fake.Client`.

## Verification before declaring done

After any change touching `api/`, `internal/`, or `cmd/`:

```bash
make manifests generate   # regen CRD/RBAC + deepcopy
go build ./...            # must be clean
go test ./...             # must pass
go vet ./...
```
