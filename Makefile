# Development and test harness. See docs/testing.md for the full story.

ENVTEST_K8S_VERSION ?= 1.33.0
SETUP_ENVTEST       ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21
KIND_CLUSTER        ?= sts-reconciler-e2e
KIND_NODE_IMAGE     ?= kindest/node:v1.33.1

.PHONY: all build fmt vet test test-integration kind-up kind-down test-e2e e2e run-dry-run clean

all: build test

build:
	go build ./...

fmt:
	gofmt -w cmd internal test

vet:
	go vet ./...
	go vet -tags integration ./test/integration/...
	go vet -tags e2e ./test/e2e/...

## Unit tests: pure Go, no cluster, no binaries. Runs anywhere.
test:
	go test ./internal/...

## Integration tests: real kube-apiserver + etcd via envtest (downloads
## binaries on first run), no kubelet/scheduler. Verifies API validation,
## admission (PVC resize), patch semantics, and the live watch loop.
test-integration:
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		go test -tags integration -v -timeout 10m ./test/integration/...

## E2E: a real kind cluster with the manager running in-process against it
## and internal/csisim standing in for the CSI driver.
kind-up:
	kind create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE) \
		--config hack/kind-config.yaml --wait 120s

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

test-e2e:
	kubectl config use-context kind-$(KIND_CLUSTER)
	go test -tags e2e -v -timeout 20m ./test/e2e/...

e2e: kind-up test-e2e

## Run the controller locally against the current kubeconfig in dry-run mode.
run-dry-run:
	go run ./cmd --dry-run --label-selector= --metrics-bind-address=0 --health-probe-bind-address=0

clean:
	go clean ./...
