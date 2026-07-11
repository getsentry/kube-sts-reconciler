# kube-sts-reconciler

[experimental] A lightweight Kubernetes controller that automates orphan-delete and PVC
patching when StatefulSet `volumeClaimTemplates` change.

StatefulSet `volumeClaimTemplates` are immutable, so changing storage size or the
[VolumeAttributesClass](https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/)
of an existing StatefulSet normally requires a manual orphan-delete → patch-PVCs →
re-apply dance. This controller automates it, driven entirely by an opt-in annotation.

## How it works

Stamp the desired PVC spec on a StatefulSet:

```sh
kubectl annotate sts my-broker 'sts-reconciler.sentry.io/desired-pvc-spec=
{"version":1,"claims":{"data":{"volumeAttributesClassName":"vac-fast","storage":"333Gi"}}}'
```

The controller then, in order (safest step first, destructive step last):

1. **Validates** — health gate, VAC exists, StorageClass allows expansion, no shrinks,
   no unknown fields. Anything off ⇒ `Failed` status + warning event, no mutation.
2. **Patches** each `<claim>-<sts>-<ordinal>` PVC's spec (`volumeAttributesClassName`,
   `resources.requests.storage`).
3. **Waits** for the CSI driver to converge PVC status (bounded by
   `--convergence-timeout`, default 10m).
4. **Orphan-deletes** the StatefulSet — pods and PVCs keep running.
5. On the next deploy re-applying the StatefulSet with matching templates, it sees no
   drift and **clears** its annotations. Done.

Progress is tracked in a `sts-reconciler.sentry.io/status` annotation and in Events on
the StatefulSet (`kubectl describe`). `sts-reconciler.sentry.io/skip: "true"` is an
emergency stop for a single StatefulSet. No CRDs; the controller is inert without the
annotation.

See [docs/implementation-plan.md](docs/implementation-plan.md) for the full design
(annotation contract, state machine, recreate modes, guardrails).

## Running

```sh
go run ./cmd \
  --label-selector=service=taskbroker \  # scope; empty watches everything
  --dry-run \                            # log intended actions, mutate nothing
  --convergence-timeout=10m
```

`--recreate-mode` picks who recreates the StatefulSet after the orphan-delete:

- `deploy` (default): the controller waits; the next `kubectl apply`/deploy sync
  recreates it. Fully stateless.
- `self`: the controller snapshots the manifest to a ConfigMap before deleting,
  recreates the StatefulSet itself with the updated `volumeClaimTemplates` (reconciler
  annotations stripped), then removes the snapshot. The ConfigMap makes this
  crash-safe: a controller restarted mid-flow resumes from it. Snapshots are
  anchored by a content hash stamped on the PVCs, so a forged ConfigMap cannot make
  the controller create arbitrary StatefulSets. Requires extra RBAC (`create` on
  statefulsets, read/write on configmaps).

## Deploying

The controller ships as a distroless container image and a Helm chart (both published
to GHCR on version tags):

```sh
helm install ksr oci://ghcr.io/getsentry/charts/kube-sts-reconciler \
  --namespace sts-reconciler-system --create-namespace \
  --set controller.labelSelector="service=taskbroker" \
  --set controller.recreateMode=deploy \
  --set controller.dryRun=true          # recommended for the first rollout
```

The chart encodes the least-privilege RBAC split: `controller.recreateMode=self` is the
only thing that grants `create` on StatefulSets and write access to ConfigMaps. See
[charts/kube-sts-reconciler/values.yaml](charts/kube-sts-reconciler/values.yaml) for
all knobs (timeouts, metrics, resources, scheduling). Environments that don't consume
Helm directly can render plain manifests with `helm template`.

One-time setup after the first release: flip the GHCR package visibility to **public**
for both `kube-sts-reconciler` (image) and `charts/kube-sts-reconciler` (chart) — GHCR
packages are created private by default.

### Consuming from getsentry/ops (sentry-kube)

The public OCI chart is consumed exactly like other third-party operators in
`k8s/helm_services/` (`kube-binpacking-exporter` is the closest precedent — a
controller pulled from a public GHCR OCI chart):

```yaml
# ops: k8s/helm_services/kube-sts-reconciler/_helm.yaml
chart:
  name: kube-sts-reconciler
  repository: oci://ghcr.io/getsentry/charts/kube-sts-reconciler
  version: 0.1.0            # pinned; bump deliberately
  dynamic_app_version: false

releases:
  - name: kube-sts-reconciler
    namespace: sts-reconciler-system
    use:
      - 00.default.yaml
```

with per-cluster policy (`controller.labelSelector`, `controller.recreateMode`,
`controller.dryRun`) in the layered values files (`00.default.yaml`,
`50.<region>.yaml`), the service listed under `helm.services:` in each cluster
config, and a GoCD pipeline applying it via `sentry-kube helm apply`.

## Development & testing

```sh
just test               # unit tests (no cluster needed)
just test-integration   # envtest: real kube-apiserver
just e2e                # kind: full loop against a real cluster
```

See [docs/testing.md](docs/testing.md) — including how the harness simulates the CSI
driver locally, since kind can't modify or expand real volumes.
