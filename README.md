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

`--recreate-mode=deploy` (the default and only implemented mode) means the controller
never recreates the StatefulSet itself — the next `kubectl apply`/deploy sync does.

## Development & testing

```sh
make test               # unit tests (no cluster needed)
make test-integration   # envtest: real kube-apiserver
make e2e                # kind: full loop against a real cluster
```

See [docs/testing.md](docs/testing.md) — including how the harness simulates the CSI
driver locally, since kind can't modify or expand real volumes.
