# llm-k8s-redis

An LLM-driven Kubernetes controller for Redis Cluster lifecycle management.
The LLM generates operation plans as a constrained DSL (JSON); the controller
validates them deterministically and executes one step per reconcile.

## Architecture

```
User edits RedisCluster CR spec
        │
        ▼
  Reconciler (controller-runtime)
        │
        ├── reads live K8S Pods + Redis CLUSTER NODES
        │
        ▼
  LLM Planner ───► generates Action Plan (JSON DSL)
        │
        ▼
  Validator ───► deterministic safety VM; rejects unsafe plans
        │
        ▼
  Executor ───► executes one step per reconcile (idempotent)
```

The LLM is a **planner**, not an executor. It only produces a sequence of
whitelisted actions. The controller validates the plan against safety invariants
before persisting it to `status.activePlan`, then executes one step per
reconcile with live-state safety checks.

## Features

- **Create** a Redis Cluster from scratch
- **ScaleOut** shards or replicas (balanced slot redistribution)
- **ScaleIn** replicas or shards (full-replacement strategy for shard shrink)
- **Upgrade** Redis images by editing `spec.image` (full node replacement)
- **Repair** single-Pod drift (replica/master deletion, with activePlan awareness)
- 9 whitelisted actions: `EnsureNode`, `WaitNodeReady`, `MeetNode`, `ReplicateNode`,
  `AddSlots`, `MigrateSlots`, `ForgetNode`, `DeleteNode`, `VerifyCluster`
- Lazy topology refresh with bounded staleness SLO
- Cluster-scoped CRD, one namespace per RedisCluster with ownerReference

## Quick Start

### Prerequisites

- Go 1.26+
- Kubernetes cluster (1.31+)
- An OpenAI-compatible LLM endpoint (OpenAI, DeepSeek, vLLM, Ollama, Anthropic OpenAI-compatible)

### Build

```bash
make build
```

### Deploy

1. Install the CRD and RBAC:

   ```bash
   make manifests
   kubectl apply -f config/crd/bases/
   kubectl apply -f config/rbac/
   ```

2. Create the LLM configuration ConfigMap in the controller's namespace:

   ```yaml
   apiVersion: v1
   kind: ConfigMap
   metadata:
     name: llm-config
     namespace: redis-cluster-system
   data:
     baseUrl: https://api.deepseek.com/v1
     apiKey: sk-...
     model: deepseek-v4-flash
     maxTokens: "4096"
     temperature: "0.1"
     reasoningEffort: max
   ```

3. Deploy the controller and create a RedisCluster CR:

   ```yaml
   apiVersion: redis.example.com/v1alpha1
   kind: RedisCluster
   metadata:
     name: my-cluster
   spec:
     shards: 3
     replicasPerShard: 1
     image: redis:7.2
     memorySize: 2Gi
   ```

4. The controller will call the LLM, validate the plan, and begin execution.
   Monitor with:

   ```bash
   kubectl get rediscluster my-cluster -o yaml
   ```

## Configuration

The controller reads LLM configuration from a ConfigMap at startup (no hot-reload).

| ConfigMap key     | Description                                    |
| ----------------- | ---------------------------------------------- |
| `baseUrl`         | OpenAI-compatible endpoint base URL            |
| `apiKey`          | API key                                        |
| `model`           | Model name                                     |
| `maxTokens`       | Max completion tokens (optional)               |
| `temperature`     | Sampling temperature (optional)                |
| `reasoningEffort` | Reasoning effort for supporting models         |

CLI flags:

| Flag                        | Default                       | Description              |
| --------------------------- | ----------------------------- | ------------------------ |
| `--disable-llm-planner`     | `false`                       | Fallback to no-op planner |
| `--llm-configmap-name`      | `llm-config`                  | ConfigMap name           |
| `--llm-configmap-namespace` | `redis-cluster-system`        | ConfigMap namespace      |

Disable the LLM planner with `--disable-llm-planner` to run the controller
without an LLM backend (no plans will be generated).

## Operations

### Create

Set `spec.shards` and `spec.replicasPerShard`. The controller ensures all Pods
exist, joins them to the Redis Cluster gossip, sets up replication, distributes
slots, and verifies cluster health.

### ScaleOut

Increase `spec.shards` or `spec.replicasPerShard` (not both at once). New Pods
use monotonic `redis-<N>` names from the highest historical ordinal + 1.
Slot redistribution follows a deterministic balanced allocation.

### ScaleIn

Decrease `spec.replicasPerShard` or `spec.shards` (not both at once). Shard
reduction uses a full-replacement strategy: new masters are created, all slots
are migrated, then old nodes are cleaned up.

### Repair

When a managed Pod is deleted, the controller re-plans from the current observed
state to the desired spec, respecting the monotonic naming rule and safety
invariants.

## Safety Model

**Three layers of defense:**

1. **Prompt constraints** — The system prompt defines the DSL schema, whitelisted
   actions, and safety invariants. The LLM cannot generate arbitrary Redis
   commands, K8S patches, or shell commands.

2. **Validator VM** — A deterministic validator simulates every action in the
   plan against an abstract topology model. It rejects plans that reach unsafe
   intermediate states (e.g., replica holding slots, master with zero replicas,
   slot overlap, deleting a slot-owning master).

3. **Executor live-state checks** — Before executing each step, the executor
   re-reads live K8S/Redis state and re-verifies safety preconditions. The plan
   is never trusted; live state is the source of truth.

The LLM is **untrusted**. It only provides a sequence suggestion. Safety is
enforced deterministically.

## Project Structure

```
.
├── api/v1alpha1/          # CRD types (RedisCluster)
├── cmd/manager/           # Controller entry point
├── config/
│   ├── crd/bases/         # Generated CRD manifests
│   └── rbac/              # Generated RBAC manifests
├── docs/                  # Design docs (DESIGN, OPERATIONS, ACTIONS, PLANNER)
├── internal/
│   ├── controller/        # Reconciler + action executors
│   ├── plan/              # Plan DSL types + Validator VM
│   ├── planner/           # LLM planner (interface + OpenAI client + prompts)
│   ├── rag/               # Redis cluster info retrieval
│   ├── redis/             # Redis client helpers
│   └── rediscluster/      # Redis Cluster topology operations
├── Makefile
└── Dockerfile
```

## Build & Test

```bash
make build         # generate + fmt + vet + build
make test          # generate + fmt + vet + go test ./...
make run           # run controller locally
make manifests     # regenerate CRD + RBAC
make generate      # regenerate deepcopy
make tidy          # go mod tidy
```

Tests use `fake.Client` (no envtest). Run a single test:

```bash
go test ./internal/planner/ -run TestLLMPlanner_ValidPlan -v
```

## License

[GNU General Public License v3.0](LICENSE)
