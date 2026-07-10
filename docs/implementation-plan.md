# Implementation Plan — kube-sts-reconciler

A lightweight Kubernetes controller that automates PVC patching and orphan-delete when
StatefulSet `volumeClaimTemplates` change.

**Status:** Proposed — for review
**Based on:** StatefulSet Volume Reconciler design handoff (2026-07-10), plus review decisions below
**Context:** [getsentry/ops#21850](https://github.com/getsentry/ops/pull/21850) (taskbroker VolumeAttributesClass support)

---

## 1. Design decisions resolved during review

The handoff doc left several points open or internally inconsistent. These were resolved
with the doc owner before this plan was written:

| # | Question | Decision |
|---|---|---|
| 1 | The doc's pseudocode stores reconcile state in an annotation on the StatefulSet, but its first action is to delete the StatefulSet — leaving nowhere to track state and no watch event to drive later phases. | **Reorder: patch PVCs first, orphan-delete last.** PVC fields we care about (`volumeAttributesClassName`, storage expansion) are mutable while the StatefulSet exists; only *recreation* requires the delete. STS **annotations** are always mutable, so the status annotation lives on the STS for the entire flow and the delete is the final act. **Fallback** if this proves infeasible in practice: keep the doc's delete-first order and move state onto the PVCs (which survive orphan-delete). |
| 2 | Who recreates the StatefulSet after orphan-delete? | **Both, behind a flag.** Default mode: recreation happens via the next sentry-kube sync / manual re-apply (controller stays stateless). Optional `--recreate-mode=self`: the controller snapshots the manifest before deleting and re-applies it itself. See §5. |
| 3 | The flow diagram says sentry-kube clears the desired-spec annotation; the pseudocode says the controller does. | **The controller clears it** once drift is empty. sentry-kube's contract is stamp-and-forget. |
| 4 | Annotation domain: `taskbroker.sentry.io` vs. something service-neutral, given the plan to widen scope beyond taskbroker. | **`sts-reconciler.sentry.io`** from day one. Taskbroker scoping lives in the label selector (`service=taskbroker`), not the annotation contract. |

Two smaller corrections to the handoff doc, folded into this plan:

- **PDB guardrail:** orphan-delete does not disturb pods, so it cannot *violate* a PDB at
  delete time. The real risk is the window where the STS is absent and crashed pods are not
  recreated. The guardrail is therefore reframed as a **health gate** (§7) plus keeping the
  STS-less window as short as possible (delete-last ordering already does this).
- **Storage shrink:** `resources.requests.storage` is expand-only in Kubernetes. The
  controller must validate `desired >= current` per PVC and reject shrinks with a warning
  event rather than attempting the patch.

## 2. Goals and non-goals

**Goals**

- Automate the manual three-step dance (orphan-delete → patch PVCs → recreate) for
  StatefulSet volume changes, driven entirely by an opt-in annotation.
- Support patching `volumeAttributesClassName` and storage expansion on bound PVCs.
- Stay stateless: all state in annotations on live objects; no CRDs, no database.
- Be inert by default: no annotation ⇒ no action, ever.

**Non-goals**

- `storageClassName` / `accessModes` changes (immutable after bind; requires PV migration).
- Storage shrinking.
- Replacing the deploy pipeline. The controller reacts to a signal; it does not decide
  when storage changes are needed.
- Multi-cluster management. One controller instance per cluster.

## 3. Annotation contract (v1)

All keys use the `sts-reconciler.sentry.io` domain.

### 3.1 Desired spec — written by sentry-kube (or a human), cleared by the controller

```yaml
metadata:
  annotations:
    sts-reconciler.sentry.io/desired-pvc-spec: |
      {
        "version": 1,
        "claims": {
          "sqlite": {
            "volumeAttributesClassName": "task-process-segments-broker-sqlite-vac-i15000t140",
            "storage": "333Gi"
          }
        }
      }
```

Schema rules:

- `version` (int, required): contract version; the controller rejects versions it doesn't know.
- `claims` (map, required): keys are `volumeClaimTemplate` names. The controller resolves
  actual PVCs as `<claim>-<sts-name>-<ordinal>` for ordinals `0..replicas-1`, cross-checked
  against ownership/labels.
- Per-claim fields (all optional, at least one required):
  - `volumeAttributesClassName` (string): target VAC. Must exist in the cluster before the
    controller will patch (validated at reconcile time; missing VAC ⇒ warning event + requeue).
  - `storage` (quantity string): target `resources.requests.storage`. Must be ≥ every
    current PVC request (expand-only).
- Unknown per-claim fields ⇒ the whole annotation is rejected with a warning event and the
  status set to `Failed` with a reason. Fail closed, never patch a partial interpretation.

### 3.2 Status — written and cleared by the controller only

```yaml
    sts-reconciler.sentry.io/status: |
      {
        "version": 1,
        "state": "Patching",
        "observedSpecHash": "sha256:…",
        "pvcs": {
          "sqlite-task-process-segments-broker-0": "Converged",
          "sqlite-task-process-segments-broker-1": "Modifying"
        },
        "reason": "",
        "lastTransition": "2026-07-10T15:30:00Z"
      }
```

`observedSpecHash` is a hash of the desired-spec annotation content. If the desired spec
changes mid-flight, the controller restarts the state machine from `Pending` instead of
finishing a stale reconcile.

### 3.3 Auxiliary keys

| Key | Written by | Purpose |
|---|---|---|
| `sts-reconciler.sentry.io/manifest-snapshot` (on a ConfigMap, self-recreate mode only) | controller | Stored STS manifest for recreation, see §5 |
| `sts-reconciler.sentry.io/skip: "true"` | human | Emergency opt-out: controller ignores this STS entirely |

## 4. Reconcile state machine

Revised ordering per decision #1 — **patch first, delete last**:

```
        desired-pvc-spec annotation present, drift ≠ ∅
                          │
                          v
   ┌──────── Pending ─ validate (health gate, VAC exists, expand-only,
   │                    field mutability) ── invalid ──> Failed (event + reason)
   │  valid
   v
Patching ─ patch each PVC (VAC and/or storage) ──> AwaitingConvergence
                                                        │
                     PVC status shows VAC applied and/or │ (requeue 30s,
                     capacity ≥ requested                │  timeout 10m)
                                                        v
                                                   Converged
                                                        │
                              ┌─ recreate-mode=deploy ──┴── recreate-mode=self ─┐
                              v                                                 v
                    Deleting (orphan)                          Snapshot manifest → Deleting (orphan)
                              │                                                 │
                              v                                                 v
                    (STS absent — wait for                        controller re-applies manifest
                     next deploy / re-apply)                      with updated volumeClaimTemplates
                              │                                                 │
                              └────────────────┬────────────────────────────────┘
                                               v
                          STS reappears → reconcile runs → drift = ∅
                          → controller clears desired-pvc-spec + status  →  Done
```

Key properties:

- **Every phase before `Deleting` leaves a fully functional system.** A timeout, crash, or
  human abort mid-`Patching` strands nothing: the STS still exists, pods still run, and the
  status annotation says exactly where things stopped.
- **State lives on the STS**, which exists for every state except the brief window after
  `Deleting` — and in that window there is no state left to track: PVCs are already
  converged, and the terminal transition (clear annotations) is triggered by the STS
  *reappearing* with empty drift. No state survives the delete because none needs to.
- **Idempotent transitions.** Each reconcile re-reads the world and recomputes drift; the
  status annotation is a hint for observability and requeue pacing, not a source of truth
  that can go stale. If drift is empty at any point, the controller clears and exits.
- **Fallback (decision #1):** if patch-while-STS-exists hits an unforeseen blocker, the
  fallback is the doc's original delete-first order with per-PVC status annotations
  (`sts-reconciler.sentry.io/status` on each PVC) and reconciliation driven by PVC watch
  events. This is strictly more complex (fan-in across PVCs to decide convergence) and is
  only built if needed.

### Convergence detection

- **VAC:** `pvc.status.currentVolumeAttributesClassName == desired` and
  `pvc.status.modifyVolumeStatus` is empty (no in-progress/failed modification).
  `ModifyVolumeStatus.status == Infeasible` ⇒ transition to `Failed` with the CSI reason.
- **Expansion:** `pvc.status.capacity.storage >= desired` and no
  `FileSystemResizePending` / `Resizing` condition still true. Some CSI drivers require a
  pod restart to finish filesystem resize; if only `FileSystemResizePending` remains, the
  controller treats the PVC as *converged-enough to proceed* (the STS recreation and normal
  pod churn completes it) but records it in the status annotation.

## 5. Recreate modes (decision #2)

Controlled by `--recreate-mode`, default `deploy`:

| Mode | Behavior | Trade-off |
|---|---|---|
| `deploy` (default) | After orphan-delete, do nothing. Next sentry-kube sync (or a human `kubectl apply`) recreates the STS. | Controller fully stateless; STS-less window lasts until the next deploy. |
| `self` | Before deleting, write the live STS manifest (minus server-managed fields, minus the reconciler annotations, with `volumeClaimTemplates` updated to match the desired spec) to a ConfigMap in the STS namespace. After delete, immediately re-apply it, then delete the ConfigMap. | Seconds-long STS-less window, no deploy dependency; controller briefly owns manifest state, and the merged template must be correct. |

The state machine is identical up to `Deleting`; the modes only differ in what follows.
Both modes are implemented. In `self` mode the snapshot ConfigMap doubles as the
crash-recovery signal: a ConfigMap watch re-enqueues the StatefulSet on controller
restart, so a crash between delete and recreate resumes instead of stranding the
workload.

**Snapshot trust boundary:** ConfigMap content is never trusted on its own — otherwise
anyone with ConfigMap write access could have the controller create an arbitrary
StatefulSet with its RBAC. Before the orphan-delete, the controller stamps the
snapshot's content hash onto every claim PVC
(`sts-reconciler.sentry.io/snapshot-hash`); recreation requires at least one PVC — and
every existing claim PVC — to carry the matching anchor, and the manifest's identity
must match the snapshot's key. PVCs survive the delete window by design and forging the
anchor requires PVC patch rights, so ConfigMap access alone cannot cross the boundary.
Rejected snapshots are left in place (as evidence) with a `SnapshotRejected` warning
event on the ConfigMap. A StatefulSet with no PVCs at all cannot be anchored, so self
mode refuses to orphan-delete it (blocked, then `Failed` after the gate timeout) rather
than delete something it could never recreate.

Recreate race (`deploy` mode): if sentry-kube re-applies while the controller is still in
`Patching`, the apply necessarily carries the *old* `volumeClaimTemplates` (the new ones are
rejected while the old STS exists — that's the whole premise), so nothing conflicts; the
controller keeps patching. `observedSpecHash` protects against a re-apply that changes the
desired-spec annotation mid-flight.

## 6. Controller architecture

### 6.1 Stack

| Component | Choice |
|---|---|
| Language | Go (latest stable, currently 1.24.x) |
| Framework | controller-runtime via kubebuilder scaffolding — reconciler only, no CRDs |
| Deployment | Single-replica Deployment in the managed cluster, leader election on (safe rolling updates) |
| Images | Distroless, multi-arch; built in CI (GitHub Actions) and pushed to Sentry's registry |

### 6.2 Repository layout

```
cmd/main.go                     # flags, manager setup, leader election
internal/
  controller/
    statefulset_controller.go   # reconciler + state machine
    statefulset_controller_test.go
  contract/
    annotation.go               # parse/validate/serialize v1 contract (§3)
    annotation_test.go
  drift/
    drift.go                    # desired vs actual PVC comparison
    convergence.go              # VAC + expansion convergence checks (§4)
  recreate/
    deploy.go                   # no-op waiter
    self.go                     # snapshot + re-apply
config/                         # kustomize: RBAC, deployment, metrics service
docs/
  implementation-plan.md        # this file
  runbook.md                    # manual procedure + failure playbook (milestone 0)
```

### 6.3 Watches and scoping

- **Primary watch:** StatefulSets, filtered by a label-selector predicate
  (`--label-selector`, default `service=taskbroker`) *and* presence of either reconciler
  annotation. Widening scope later (experiment plan step 4) is a flag change.
- **Secondary watch:** PVCs, mapped to their owning STS via the
  `<claim>-<sts>-<ordinal>` naming convention + labels, so PVC status changes (VAC applied,
  resize done) trigger reconciles promptly instead of relying on requeue polling.
- **Namespaces:** `--namespaces` flag; empty = all namespaces (default for the POC cluster).

### 6.4 RBAC (least privilege)

| Resource | Verbs | Why |
|---|---|---|
| `statefulsets` | get, list, watch, patch, **delete** | annotations, orphan-delete |
| `statefulsets` | create | **`self` recreate mode only** — omitted from default role |
| `persistentvolumeclaims` | get, list, watch, patch | PVC patching + convergence |
| `volumeattributesclasses` | get, list, watch | validate VAC exists |
| `storageclasses` | get | check `allowVolumeExpansion` before expanding |
| `poddisruptionbudgets` | get, list | health-gate context |
| `configmaps` | get, create, delete | `self` mode manifest snapshot only |
| `events` | create, patch | operator visibility |

Delete is deliberately scoped: the controller only ever calls delete with
`PropagationPolicy=Orphan`, and only on an STS carrying a valid desired-spec annotation
whose PVCs it has just verified as converged.

## 7. Safety guardrails

1. **Annotation-gated:** no `desired-pvc-spec` annotation ⇒ no action. Ever.
2. **Label-scoped:** `service=taskbroker` only, until deliberately widened by flag.
3. **Health gate (in `Pending`):** refuse to start if `status.readyReplicas == 0` while
   `spec.replicas > 0`, or if the STS has an in-progress rolling update
   (`currentRevision != updateRevision`). Emit a warning event and requeue with backoff —
   don't silently drop the request.
4. **Validation-before-action (in `Pending`):** VAC exists; StorageClass allows expansion;
   desired storage ≥ current on every PVC; no immutable fields (e.g. `storageClassName`)
   in the annotation. Any failure ⇒ `Failed` state + warning event, no mutation.
5. **Delete-last ordering:** the only destructive step happens after PVCs are verified
   converged, minimizing the STS-less window (seconds in `self` mode, one deploy cycle in
   `deploy` mode).
6. **Timeouts:** stuck in `AwaitingConvergence` > 10 min (`--convergence-timeout`), or
   blocked by the health gate > 10 min (`--gate-timeout`), ⇒ `Failed` state, warning
   event, stop requeueing. Because nothing has been deleted yet, the system is
   degraded-but-running and a human can take over via the runbook. The timeout anchors
   live in the status annotation with an in-memory fallback, so the bound holds even if
   annotation writes fail.
7. **Dry-run:** `--dry-run` flag logs every mutation it *would* make (including the delete)
   and writes status state `DryRun`. First deployment on s4s2 runs this way.
8. **Emergency opt-out:** `sts-reconciler.sentry.io/skip=true` annotation halts the
   controller for that STS regardless of other state.
9. **Spec-change detection:** `observedSpecHash` restarts the state machine cleanly if the
   desired spec changes mid-flight.

## 8. Observability

- **Metrics** (controller-runtime defaults plus):
  - `sts_reconciler_state{namespace,statefulset,state}` gauge — current state machine position
  - `sts_reconciler_transitions_total{from,to}` counter
  - `sts_reconciler_pvcs_patched_total{field}` counter (`vac` | `storage`)
  - `sts_reconciler_failures_total{reason}` counter
  - `sts_reconciler_convergence_duration_seconds` histogram
- **Events** on both the STS and each PVC for every transition, validation failure, patch,
  and delete — so `kubectl describe` tells the whole story without dashboard access.
- **Structured logs** keyed by namespace/name/state for log-search correlation.
- Alert suggestion (wired later in ops): page on `state="Failed"` gauge > 0 for > 15 min.

## 9. Testing strategy

| Layer | Tooling | Coverage |
|---|---|---|
| Unit | plain Go tests | contract parsing/validation, drift computation, convergence logic, state transitions (table-driven, every state × event) |
| Integration | envtest (real API server, no kubelet) | full reconcile loop: annotation → validate → patch → converge (status stubbed) → delete → recreate → clear. Both recreate modes. Failure paths: shrink attempt, missing VAC, unhealthy STS, mid-flight spec change, `skip` annotation. |
| E2E | kind + a CSI driver supporting expansion (e.g. csi-driver-host-path) | happy path with a real StatefulSet; VAC path exercised on a real cluster later since kind's CSI VAC support is limited |
| Cluster validation | s4s2, dry-run mode | milestone 2 exit criterion: dry-run output matches the manual runbook steps for a real taskbroker VAC change |

## 10. Milestones

Aligned with the handoff doc's experiment plan; estimates match its 1–2 week prototype +
1 week integration framing.

**M0 — Runbook (now, ~1 day).** Document the exact manual sequence (patch PVCs → verify
convergence → orphan-delete → re-apply) in `docs/runbook.md`, using the delete-last
ordering from this plan. This is both today's operational procedure and the failure
playbook once the controller exists.

**M1 — Controller core (week 1).** Kubebuilder scaffold; annotation contract package with
full validation; drift + convergence packages; state machine through `AwaitingConvergence`;
dry-run mode; unit + envtest suites; CI (lint, test, image build).
*Exit: envtest green on happy path + all validation failure paths.*

**M2 — Delete, recreate & cluster validation (week 2).** `Deleting` phase with orphan
propagation; `deploy` recreate mode; terminal cleanup (clear annotations); metrics + events;
kind e2e; deploy to s4s2 in dry-run; then first supervised live run with a manually stamped
annotation on a taskbroker STS.
*Exit: one real taskbroker VAC change reconciled end-to-end with a human watching.*

**M3 — Self-recreate mode + sentry-kube integration (week 3).** `--recreate-mode=self`
(snapshot ConfigMap, re-apply, cleanup). sentry-kube change (separate repo/PR): on a
volumeClaimTemplate diff, stamp `sts-reconciler.sentry.io/desired-pvc-spec`, apply the STS
*without* the template change, and surface the reconcile status in deploy output.
*Exit: a deploy with a template change completes without operator intervention.*

**M4 — Widen scope.** Relax the label selector via flag; write onboarding docs for other
services; decide alerting ownership. Driven by demand, no fixed timeline.

## 11. Open questions and accepted risks

- **STS-less window in `deploy` mode.** Between orphan-delete and the next sync, crashed
  pods are not replaced and scaling is impossible. Accepted for the POC; mitigated by
  delete-last ordering and eliminated by `self` mode in M3. Worth measuring in M2: how long
  is the window in practice on s4s2?
- **sentry-kube apply semantics.** Does sentry-kube's apply strip unknown live annotations
  (which would erase the controller's status annotation on every deploy)? Must be checked
  during M3; if so, the status annotation moves to the field-manager-safe path (server-side
  apply with a distinct field manager) or onto PVCs.
- **`FileSystemResizePending` semantics per CSI driver.** The "converged-enough" rule in §4
  assumes pod churn finishes filesystem resizes. Validate against the actual drivers used by
  taskbroker volumes during M2.
- **Multiple controller instances (handoff doc: "possibly multiple instances eventually").**
  Out of scope; the label-selector flag already allows sharding by service if ever needed.
