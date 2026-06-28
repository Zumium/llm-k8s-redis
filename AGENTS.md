# AGENTS.md

K8S Redis Cluster controller. Reconciliation is driven by an LLM-generated DSL
plan, then deterministically validated and executed one step per reconcile.
Go + controller-runtime, cluster-scoped CRD.

## Principles

Keep code simple enough for a junior SDE to read. Prefer less code.

## Commands

```bash
make build       # generate + fmt + vet + build
make test        # generate + fmt + vet + go test ./...; fake client only
make run         # run against current kubeconfig
make manifests   # regenerate CRD/RBAC via controller-gen
make generate    # regenerate deepcopy
make tidy        # go mod tidy
```

Single test: `go test ./internal/planner/ -run TestLLMPlanner_ValidPlan -v`

## Toolchain

- Keep `GOPROXY=https://goproxy.cn,direct`; do not reset it to `direct`.
- Go 1.26 is required.
- `controller-gen` is auto-installed by `make generate` / `make manifests`.
  If deepcopy output is incomplete, restore `+kubebuilder:object:generate=true`
  in `api/v1alpha1/doc.go` and regenerate.

## Architecture

```text
controller.RedisClusterReconciler
  -> planner.Planner
       -> planner.LLMPlanner
            -> planner.Client
                 -> planner.OpenAIClient
plan.Validator
```

- LLM config is loaded from ConfigMap at startup only:
  `baseUrl`/`apiKey`/`model`/`maxTokens`/`temperature`/`reasoningEffort`.
  Defaults use `llm-config` in the controller namespace. Restart after config
  changes. Flags: `--llm-configmap-name`, `--llm-configmap-namespace`,
  `--disable-llm-planner`.
- `planner.OpenAIClient` is the only `planner.Client`; OpenAI-compatible
  `baseUrl` may point at Azure OpenAI, vLLM, Ollama, or Anthropic-compatible
  endpoints. No native Anthropic Messages adapter yet.
- Never trust planner output. `LLMPlanner` fixes `DSLVersion` and
  `TargetGeneration`; `plan.Validator` re-checks everything else before
  persisting `status.activePlan`.
- Execute at most one pending step per reconcile.
- Controller is cluster-scoped. Each `RedisCluster` owns a same-name namespace;
  ownerReference enables GC and a finalizer gates CR deletion.

## Conventions

- `StepStatus.Params` is `apiextensionsv1.JSON`, not `map[string]any`; keep
  marshal/unmarshal at `planToStatus` / `statusToPlan`.
- Use `mgr.GetEventRecorder(name)`, not deprecated `GetEventRecorderFor`.
  `Eventf` args are `(regarding, related, eventtype, reason, action, note, ...)`.
- Put `+kubebuilder:rbac` markers in `internal/controller/doc.go`, not methods.
- No code comments unless asked.
- Design docs are authoritative: `docs/DESIGN.md`, `docs/OPERATIONS.md`,
  `docs/ACTIONS.md`. Update docs and Validator together for action/operation
  changes.
- `go.mod` module path is `github.com/Zumium/llm-k8s-redis`; do not rewrite
  imports without instruction.

## Test Environment

- Host: `<ip>`, user `<user>`, SSH key, passwordless sudo.
- KIND: cluster `kind`, 1 control-plane + 5 workers, K8s v1.36.1, config
  `cluster-configs/kind-1-control-plane-5-workers.yaml`, binary
  `<kind-binary-path>` v0.32.0.
- Remote tree: `~/projects/llm-k8s-redis`, tar-synced, not a git checkout:
  ```bash
  tar --exclude='.git' --exclude='bin' --exclude='cover.out' -czf - . \
    | ssh <user>@<ip> 'tar -xzf - -C ~/projects/llm-k8s-redis'
  ```
- Fast deploy when only controller binary changed: build local linux
  `./cmd/manager`, copy `Dockerfile` + `manager` to remote, `docker build -t
  controller:latest .`, `kind load docker-image controller:latest --name kind`,
  then restart `redis-cluster-system/controller-manager`. Do not touch live
  `llm-config`.
- Live `redis-cluster-system/llm-config`: `provider=openai`,
  `baseUrl=https://api.deepseek.com/v1`, `model=deepseek-v4-flash`,
  `reasoningEffort=max`, `apiKey` only in cluster. Restart controller after
  rotating it.

## Not Implemented Yet

- Native Anthropic `planner.Client`; `provider` key is ignored.
- Executor actions for `MigrateSlots`, `ForgetNode`, `DeleteNode`; Validator
  whitelists them but no executor files exist.
- Validators for shard `ScaleOut` / `ScaleIn` / `UpdateMemorySize` / `Delete`;
  only `Create` and `ReplicaScaleOut` have operation-specific checks.
- `envtest` integration tests; tests use `fake.Client`.

## Verification

Delegate local tests, deployment, and remote test-like verification completely to
OpenCode using model `deepseek-v4-flash` with `--variant max`. Codex should not
run verification commands directly; it should ask OpenCode to run them and report
OpenCode's results.

After touching `api/`, `internal/`, or `cmd/`:

```bash
make manifests generate
go build ./...
go test ./...
go vet ./...
```
