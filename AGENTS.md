# AGENTS.md

Redis cluster controller. Go 1.26, controller-runtime, cluster-scoped CRD.

## Rules

- Prefer less code. No comments unless asked.
- Keep `GOPROXY=https://goproxy.cn,direct`.
- Module: `github.com/Zumium/llm-k8s-redis`; do not rewrite imports.
- `StepStatus.Params` is `apiextensionsv1.JSON`; marshal at `planToStatus` /
  `statusToPlan`.
- Use `mgr.GetEventRecorder(name)`. `Eventf`: `(regarding, related, type,
  reason, action, note, ...)`.
- RBAC markers live in `internal/controller/doc.go`.
- Docs: `docs/DESIGN.md`, `docs/OPERATIONS.md`, `docs/ACTIONS.md`,
  `docs/PLANNER.md`. Update docs + Validator together for DSL changes.

## Architecture

```text
RedisClusterReconciler
  -> GoPlanner
       -> LLMPlanner
            -> LLMClient
                 -> OpenAIClient
plan.Validator
```

- ConfigMap loaded at startup only. Restart after changes.
- Default LLM ConfigMap: `redis-cluster-system/llm-config`.
- Config keys: `baseUrl`, `apiKey`, `model`, `maxTokens`, `temperature`,
  `reasoningEffort`, `planValidationRetries`, `embeddingModel`,
  `embeddingBaseUrl`, `embeddingApiKey`.
- Embedding keys enable RAG plan retrieval only when all three are set.
- Flags: `--llm-configmap-name`, `--llm-configmap-namespace`,
  `--disable-llm-planner`, `--topology-refresh-interval`,
  `--topology-stale-threshold`.
- `OpenAIClient` is the only `LLMClient`; `provider` is ignored. No native
  Anthropic client.
- Never trust planner output. `LLMPlanner` fixes `DSLVersion` /
  `TargetGeneration`; `GoPlanner` validates deterministic plans; Validator
  re-checks before `status.activePlan`.
- Execute one pending step per reconcile, except `MigrateSlots` batches.
- Each `RedisCluster` owns a same-name namespace; finalizer gates CR deletion.
- No envtest; tests use `fake.Client`.

## Commands

```bash
make build
make test
make run
make manifests
make generate
make tidy
```

## Remote

- KIND: `kind`, 1 control-plane + 5 workers, K8s v1.36.1,
  `cluster-configs/kind-1-control-plane-5-workers.yaml`, kind v0.32.0.
- Remote tree: `~/projects/llm-k8s-redis`, tar-synced, not git.
- Fast deploy: build linux `./cmd/manager`, copy `Dockerfile` + `manager`,
  build/load `controller:latest`, restart `redis-cluster-system/controller-manager`.
- Do not touch live `redis-cluster-system/llm-config`; API key exists only
  in-cluster.

## Verification

Delegate tests/deploy/remote checks to `agy`:

```bash
agy --model 'Gemini 3.1 Pro (High)' --print '<prompt>'
```

After touching `api/`, `internal/`, or `cmd/`, ask `agy` to run:

```bash
make manifests generate
go build ./...
go test ./...
go vet ./...
```
