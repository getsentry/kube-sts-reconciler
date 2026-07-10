# Development and test harness. See docs/testing.md for the full story.

envtest_k8s_version := env_var_or_default("ENVTEST_K8S_VERSION", "1.33.0")
setup_envtest := "go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21"
kind_cluster := env_var_or_default("KIND_CLUSTER", "sts-reconciler-e2e")
kind_node_image := env_var_or_default("KIND_NODE_IMAGE", "kindest/node:v1.33.1")

default: build test

build:
    go build ./...

fmt:
    gofmt -w cmd internal test

# Vet all packages under every build tag
vet:
    go vet ./...
    go vet -tags integration ./test/integration/...
    go vet -tags e2e ./test/e2e/...

# Unit tests: pure Go, no cluster, no binaries — runs anywhere
test:
    go test ./internal/...

# Envtest gives a real kube-apiserver + etcd but no kubelet/scheduler: it
# verifies API validation, admission (PVC resize), patch semantics, and the
# live watch loop. setup-envtest downloads the binaries on first run.

# Integration tests against a real kube-apiserver (envtest)
test-integration:
    KUBEBUILDER_ASSETS="$({{setup_envtest}} use {{envtest_k8s_version}} -p path)" \
        go test -tags integration -v -timeout 10m ./test/integration/...

# Create the e2e kind cluster (VolumeAttributesClass gates enabled)
kind-up:
    kind create cluster --name {{kind_cluster}} --image {{kind_node_image}} \
        --config hack/kind-config.yaml --wait 120s

# Tear down the e2e kind cluster
kind-down:
    kind delete cluster --name {{kind_cluster}}

# E2E tests: manager in-process against kind, internal/csisim playing CSI driver
test-e2e:
    kubectl config use-context kind-{{kind_cluster}}
    go test -tags e2e -v -timeout 20m ./test/e2e/...

# Full e2e: create the kind cluster, then run the e2e tests against it
e2e: kind-up test-e2e

# Run the controller locally against the current kubeconfig in dry-run mode
run-dry-run:
    go run ./cmd --dry-run --label-selector= --metrics-bind-address=0 --health-probe-bind-address=0

clean:
    go clean ./...
