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

# --- Packaged deployment (image + Helm chart) --------------------------------

image_name := env_var_or_default("IMAGE_NAME", "kube-sts-reconciler")
image_tag := env_var_or_default("IMAGE_TAG", "dev")

# Build the controller container image (distroless)
build-image:
    docker build -t {{image_name}}:{{image_tag}} .

# Load the locally built image into the kind cluster
kind-load: build-image
    kind load docker-image {{image_name}}:{{image_tag}} --name {{kind_cluster}}

# Lint the Helm chart and render both recreate modes
helm-lint:
    helm lint charts/kube-sts-reconciler
    helm template ksr charts/kube-sts-reconciler > /dev/null
    helm template ksr charts/kube-sts-reconciler --set controller.recreateMode=self > /dev/null

# Install the chart into the kind cluster using the locally built image
deploy-kind: kind-load
    kubectl config use-context kind-{{kind_cluster}}
    helm upgrade --install ksr charts/kube-sts-reconciler \
        --namespace sts-reconciler-system --create-namespace \
        --set image.repository={{image_name}} --set image.tag={{image_tag}} \
        --set image.pullPolicy=Never
    kubectl -n sts-reconciler-system rollout status deploy/ksr-kube-sts-reconciler --timeout=120s

# E2E against the helm-deployed in-cluster controller (real image, RBAC, probes)
test-e2e-deployed:
    kubectl config use-context kind-{{kind_cluster}}
    E2E_DEPLOYED=1 go test -tags e2e -v -timeout 20m ./test/e2e/...

# Full deployed e2e: cluster + image + chart + tests
e2e-deployed: kind-up deploy-kind test-e2e-deployed

# Run the controller locally against the current kubeconfig in dry-run mode
run-dry-run:
    go run ./cmd --dry-run --metrics-bind-address=0 --health-probe-bind-address=0

clean:
    go clean ./...

# --- Manual-poking sandbox (docs/testing.md, "Manual poking") ---------------
# Provisions a workload on the kind cluster so the controller can be driven
# by hand: namespace `sandbox`, an expandable StorageClass, a
# VolumeAttributesClass `vac-fast`, and a 1-replica StatefulSet `broker`.

# Create the sandbox namespace, StorageClass, VAC, and broker StatefulSet
sandbox-up storage="1Gi" vac="":
    kubectl apply -f hack/sandbox/base.yaml
    just _sandbox-apply-sts {{storage}} '{{vac}}'
    kubectl -n sandbox rollout status sts/broker --timeout=180s

# Re-apply the broker StatefulSet with a new template (simulates the next deploy after orphan-delete)
sandbox-recreate storage vac="":
    just _sandbox-apply-sts {{storage}} '{{vac}}'
    kubectl -n sandbox rollout status sts/broker --timeout=180s

_sandbox-apply-sts storage vac:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "{{vac}}" ]; then
        sed -e 's/__STORAGE__/{{storage}}/' -e 's/__VAC_LINE__/volumeAttributesClassName: {{vac}}/' hack/sandbox/sts.yaml | kubectl apply -f -
    else
        sed -e 's/__STORAGE__/{{storage}}/' -e '/__VAC_LINE__/d' hack/sandbox/sts.yaml | kubectl apply -f -
    fi

# Stamp the desired-pvc-spec annotation on the broker StatefulSet
sandbox-annotate storage="2Gi" vac="vac-fast":
    kubectl -n sandbox annotate --overwrite sts broker \
        'sts-reconciler.sentry.io/desired-pvc-spec={"version":1,"claims":{"data":{"volumeAttributesClassName":"{{vac}}","storage":"{{storage}}"}}}'

# Show reconcile state: annotations, PVC spec vs status, recent events
sandbox-status:
    #!/usr/bin/env bash
    echo "=== StatefulSet annotations ==="
    kubectl -n sandbox get sts broker -o jsonpath='{.metadata.annotations}' 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(StatefulSet not found — orphan-deleted, awaiting recreate?)"
    echo
    echo "=== PVCs (spec request / VAC -> status capacity / current VAC) ==="
    kubectl -n sandbox get pvc -o custom-columns='NAME:.metadata.name,REQ:.spec.resources.requests.storage,VAC:.spec.volumeAttributesClassName,CAP:.status.capacity.storage,CURVAC:.status.currentVolumeAttributesClassName' 2>/dev/null
    echo
    echo "=== Recent events ==="
    kubectl -n sandbox get events --sort-by=.lastTimestamp 2>/dev/null | tail -12

# Run the stand-in CSI driver for the sandbox (non-dry-run reconciles need it to converge)
sandbox-csisim:
    go run ./cmd/csisim --namespace sandbox

# Delete the sandbox namespace, StorageClass, and VAC
sandbox-down:
    kubectl delete -f hack/sandbox/base.yaml --ignore-not-found
