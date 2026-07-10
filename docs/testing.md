# Test Harness

Three layers, each catching what the previous one can't. All of them validate the same
contract: stamp `sts-reconciler.sentry.io/desired-pvc-spec` on a StatefulSet and the
controller patches PVCs, waits for CSI convergence, orphan-deletes, and clears the
annotations once a recreated StatefulSet shows no drift.

| Layer | Command | Needs | What's real |
|---|---|---|---|
| Unit | `just test` | Go only | Contract parsing, drift/convergence logic, and the full state machine against a fake client (13 scenarios incl. dry-run, timeouts, failure latching) |
| Integration | `just test-integration` | Network (first run downloads envtest binaries) | A real kube-apiserver + etcd: API validation, admission (`PersistentVolumeClaimResize`), merge-patch semantics, live watch-driven manager |
| E2E | `just e2e` | Docker + [kind](https://kind.sigs.k8s.io/) + kubectl | Everything: real StatefulSet controller, scheduler, kubelet, pods with bound local-path PVCs |

## The CSI gap, and the simulator

No local cluster can execute a real `ControllerModifyVolume` (VolumeAttributesClass
change) or grow a local-path volume — those need a cloud CSI driver. The harness fills
the gap with `internal/csisim`: a test-only loop that converges PVC **status** toward
PVC **spec** exactly the way external-resizer and the CSI sidecars would (sets
`status.currentVolumeAttributesClassName`, grows `status.capacity`). Annotating a PVC
with `csisim.sentry.io/infeasible: "true"` makes it report the VAC change as
`Infeasible` instead, for failure-path testing.

This means the harness validates the *controller's* behavior (the point of the
experiment), while real-CSI behavior stays a milestone-2 concern on s4s2.

## Unit tests

```sh
just test
```

`internal/contract`, `internal/drift`, and `internal/controller` (fake client). Fast
enough to run on every save. Covers: happy path (VAC + expansion through delete and
recreate), skip annotation, invalid/unknown-field specs, shrink rejection, health gate,
missing VAC (waits, then proceeds when created), non-expandable StorageClass,
convergence timeout, mid-flight spec change, CSI-infeasible, dry-run mutating nothing,
stale status cleanup, and PVC→StatefulSet watch mapping.

## Integration tests (envtest)

```sh
just test-integration
```

Starts a real kube-apiserver with `VolumeAttributesClass=true` and
`storage.k8s.io/v1beta1` enabled, runs a real manager against it. There's no kubelet or
StatefulSet controller in envtest, so the tests create the per-ordinal PVCs and mark
them Bound themselves, and play the CSI driver inline. First run downloads the envtest
binaries via `setup-envtest`; they're cached afterwards.

The suite skips itself politely when `KUBEBUILDER_ASSETS` is unset — always invoke it
through the justfile recipe.

## E2E (kind)

```sh
just e2e            # kind-up + test-e2e
just kind-down      # tear down
```

`just kind-up` creates a cluster from `hack/kind-config.yaml`, which enables the
`VolumeAttributesClass` feature gate and the `storage.k8s.io/v1beta1` API — without
these, the API server silently drops `spec.volumeAttributesClassName` from PVCs, which
is worth knowing about your real clusters too.

The test then:

1. Starts the manager **in-process** against the kind kubeconfig (no image build —
   fast iteration) and `csisim` alongside it.
2. Creates an expandable StorageClass backed by local-path, a VolumeAttributesClass,
   and a 1-replica StatefulSet (`service=taskbroker`) with a mounted 1Gi claim, then
   waits for the pod to be Running with a Bound PVC.
3. Stamps the desired-spec annotation asking for the VAC plus 2Gi.
4. Watches the controller patch the PVC, csisim converge it, and the controller
   orphan-delete the StatefulSet — then asserts the pod and PVC survived.
5. Recreates the StatefulSet with updated `volumeClaimTemplates` (simulating the next
   deploy) and waits for the controller to clear both annotations.

Safety: the test inspects the current kubeconfig context and refuses to run unless it
starts with `kind-`. It cannot be pointed at a real cluster by accident.

### Manual poking

The e2e test cleans up after itself, so a fresh kind cluster has nothing to poke.
The `sandbox-*` recipes provision a standalone workload: namespace `sandbox` with an
expandable StorageClass, a VolumeAttributesClass `vac-fast`, and a 1-replica
StatefulSet `broker` (labeled `service=taskbroker`, so the controller's default
selector matches) with a mounted 1Gi claim named `data`.

You'll want three terminals — sandbox commands, the CSI simulator, and the controller:

```sh
just kind-up                    # once; kubectl context switches to kind-sts-reconciler-e2e
just sandbox-up                 # broker STS running with a bound 1Gi PVC

# terminal 2 — the stand-in CSI driver. Without it a non-dry-run reconcile
# sits in AwaitingConvergence until the timeout: kind has no CSI driver that
# can actually expand volumes or apply a VAC.
just sandbox-csisim

# terminal 3 — the controller. Start with --dry-run to preview, then run for real.
go run ./cmd --dry-run
go run ./cmd
```

Then drive a reconcile and watch it move through the state machine:

```sh
just sandbox-annotate                        # asks for vac-fast + 2Gi (both overridable:
                                             #   just sandbox-annotate 3Gi vac-fast)
just sandbox-status                          # Patching -> AwaitingConvergence -> STS deleted
                                             # (PVC survives with the patched spec)

# Simulate the next deploy: re-apply the STS with matching templates.
just sandbox-recreate 2Gi vac-fast
just sandbox-annotate                        # re-stamp, as a deploy manifest would carry it;
                                             # drift is empty, controller clears both annotations
just sandbox-status
```

Useful one-offs while poking:

```sh
kubectl -n sandbox get sts broker -o jsonpath='{.metadata.annotations.sts-reconciler\.sentry\.io/status}'
kubectl -n sandbox get events --field-selector involvedObject.name=broker
kubectl -n sandbox annotate sts broker sts-reconciler.sentry.io/skip=true    # emergency stop
just sandbox-down                            # tear the sandbox down, keep the cluster
```

Failure paths are easy to provoke too: annotate with a shrink (`just sandbox-annotate
512Mi`) and watch the `Failed` status latch, or mark the PVC
`csisim.sentry.io/infeasible=true` before a VAC change to see the CSI-infeasible path.
