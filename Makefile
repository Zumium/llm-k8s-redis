# Tool binaries
GOPATH_BIN := $(shell go env GOPATH)/bin
CONTROLLER_GEN := $(GOPATH_BIN)/controller-gen

GO := go

# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = latest

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	$(GO) vet ./...

.PHONY: test
test: generate fmt vet ## Run tests.
	$(GO) test ./... -coverprofile cover.out

##@ Build

.PHONY: build
build: generate fmt vet ## Build manager binary.
	$(GO) build -o bin/manager ./cmd/manager

.PHONY: build-image
build-image: ## Build controller Docker image (set IMG to override tag).
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o manager ./cmd/manager
	docker build -t $(IMG) .
	rm -f manager

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	$(GO) run ./cmd/manager

##@ Code Generation

.PHONY: controller-gen
controller-gen: ## Download controller-gen locally if necessary.
	@if [ ! -x "$(CONTROLLER_GEN)" ]; then \
		echo ">> installing controller-gen"; \
		$(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@latest; \
	fi

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, etc.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) crd:crdVersions=v1 paths="./api/..." output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths="./..." output:rbac:artifacts:config=config/rbac

.PHONY: tidy
tidy: ## Run go mod tidy.
	$(GO) mod tidy
