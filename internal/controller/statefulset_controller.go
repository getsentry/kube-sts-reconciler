// Package controller implements the annotation-driven StatefulSet volume
// reconciler. The state machine is level-triggered: every reconcile recomputes
// the world from scratch and acts on the first thing that is out of place.
// The status annotation is an observability record and timeout anchor, never
// a source of truth.
//
// Order of operations (see docs/implementation-plan.md §4):
//
//	patch PVC specs -> wait for CSI convergence -> orphan-delete the
//	StatefulSet -> (recreated externally) -> drift empty -> clear annotations
package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
	"github.com/getsentry/kube-sts-reconciler/internal/drift"
	"github.com/getsentry/kube-sts-reconciler/internal/recreate"
)

// Event reasons emitted on the StatefulSet.
const (
	ReasonInvalidDesiredSpec   = "InvalidDesiredSpec"
	ReasonUnhealthy            = "UnhealthyStatefulSet"
	ReasonMissingVAC           = "MissingVolumeAttributesClass"
	ReasonExpansionUnsupported = "ExpansionUnsupported"
	ReasonPVCPatched           = "PVCPatched"
	ReasonAwaitingConvergence  = "AwaitingConvergence"
	ReasonConvergenceTimeout   = "ConvergenceTimeout"
	ReasonHealthGateTimeout    = "HealthGateTimeout"
	ReasonModifyInfeasible     = "ModifyVolumeInfeasible"
	ReasonOrphanDeleted        = "OrphanDeleted"
	ReasonRecreated            = "Recreated"
	ReasonSnapshotSkipped      = "SnapshotSkipped"
	ReasonSnapshotRejected     = "SnapshotRejected"
	ReasonReconcileComplete    = "ReconcileComplete"
	ReasonDryRun               = "DryRun"
)

const (
	patchRequeue       = 5 * time.Second
	convergenceRequeue = 15 * time.Second
	gateRequeue        = 30 * time.Second
)

// Reconciler reconciles StatefulSets carrying the desired-pvc-spec annotation.
type Reconciler struct {
	client.Client
	Recorder record.EventRecorder

	// Selector scopes the controller: StatefulSets not matching it are never
	// reconciled, regardless of annotations. The informer predicate applies
	// the same filter; this field enforces it on every other path too (PVC
	// watch mapping, direct requeues). Nil selects everything.
	Selector labels.Selector

	// DryRun disables every mutation. Intended actions are logged and emitted
	// as events, but neither PVCs, nor the StatefulSet, nor its annotations
	// are touched.
	DryRun bool

	// ConvergenceTimeout bounds how long PVC status may lag the patched spec
	// before the reconcile is marked Failed. Zero means 10 minutes.
	ConvergenceTimeout time.Duration

	// GateTimeout bounds how long the health gate may block a reconcile
	// before it is marked Failed instead of retrying forever. Zero means 10
	// minutes.
	GateTimeout time.Duration

	// SelfRecreate enables --recreate-mode=self: the controller snapshots the
	// StatefulSet manifest to a ConfigMap before the orphan-delete and
	// recreates it (with merged volumeClaimTemplates) itself, instead of
	// waiting for the next deploy to re-apply it.
	SelfRecreate bool

	// waitAnchors is an in-memory fallback for the timeout anchors normally
	// persisted in the status annotation, so a timeout still fires even when
	// annotation writes keep failing. failedLatches is the same fallback for
	// the Failed latch, so a terminal failure alerts once even when the
	// status write fails. Losing either on restart only extends a timeout or
	// re-emits one event, never loses safety.
	mu            sync.Mutex
	waitAnchors   map[string]waitAnchor
	failedLatches map[string]failedLatch
}

type failedLatch struct {
	specHash string
	status   *contract.Status
}

type waitAnchor struct {
	specHash string
	state    contract.State
	at       time.Time
}

// anchorTime returns when this StatefulSet first entered (specHash, state),
// as far as this process has observed. The entry is replaced whenever the
// spec hash or state changes.
func (r *Reconciler) anchorTime(sts *appsv1.StatefulSet, specHash string, state contract.State) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.waitAnchors == nil {
		r.waitAnchors = map[string]waitAnchor{}
	}
	key := sts.Namespace + "/" + sts.Name
	if a, ok := r.waitAnchors[key]; ok && a.specHash == specHash && a.state == state {
		return a.at
	}
	now := time.Now().UTC()
	r.waitAnchors[key] = waitAnchor{specHash: specHash, state: state, at: now}
	return now
}

func (r *Reconciler) clearAnchor(namespace, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waitAnchors, namespace+"/"+name)
}

// latchFailed records a terminal failure in memory before the status write is
// even attempted, so the warning event cannot be re-emitted on write retries.
func (r *Reconciler) latchFailed(sts *appsv1.StatefulSet, st *contract.Status) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failedLatches == nil {
		r.failedLatches = map[string]failedLatch{}
	}
	r.failedLatches[sts.Namespace+"/"+sts.Name] = failedLatch{specHash: st.ObservedSpecHash, status: st}
}

func (r *Reconciler) failedLatchFor(sts *appsv1.StatefulSet, specHash string) *contract.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.failedLatches[sts.Namespace+"/"+sts.Name]; ok && l.specHash == specHash {
		return l.status
	}
	return nil
}

func (r *Reconciler) clearFailedLatch(namespace, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failedLatches, namespace+"/"+name)
}

func (r *Reconciler) convergenceTimeout() time.Duration {
	if r.ConvergenceTimeout <= 0 {
		return 10 * time.Minute
	}
	return r.ConvergenceTimeout
}

func (r *Reconciler) gateTimeout() time.Duration {
	if r.GateTimeout <= 0 {
		return 10 * time.Minute
	}
	return r.GateTimeout
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattributesclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//
// Self-recreate mode only — omit from the role when running --recreate-mode=deploy:
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=create
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;delete

// Reconcile implements the level-triggered state machine.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, sts); err != nil {
		if apierrors.IsNotFound(err) {
			r.clearAnchor(req.Namespace, req.Name)
			r.clearFailedLatch(req.Namespace, req.Name)
			if r.SelfRecreate {
				// In self mode the delete's watch event lands here: recreate
				// from the snapshot taken before the orphan-delete. In deploy
				// mode a deleted StatefulSet needs nothing from us — the next
				// deploy re-applies it.
				return r.recreateFromSnapshot(ctx, req.NamespacedName)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if sts.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	if r.Selector != nil && !r.Selector.Matches(labels.Set(sts.Labels)) {
		return ctrl.Result{}, nil
	}
	if sts.Annotations[contract.SkipAnnotation] == "true" {
		log.Info("skip annotation set, ignoring StatefulSet")
		return ctrl.Result{}, nil
	}

	raw, ok := sts.Annotations[contract.DesiredSpecAnnotation]
	if !ok {
		// No desired spec. Clear any stale status annotation left behind
		// (e.g. an operator removed the desired spec to abort).
		r.clearFailedLatch(sts.Namespace, sts.Name)
		if _, has := sts.Annotations[contract.StatusAnnotation]; has {
			return ctrl.Result{}, r.removeAnnotations(ctx, sts, contract.StatusAnnotation)
		}
		return ctrl.Result{}, nil
	}

	specHash := contract.HashValue(raw)
	status := r.currentStatus(sts)
	if status != nil && status.State == contract.StateFailed && status.ObservedSpecHash == specHash {
		// Failed is latched per desired-spec content: no retries (and no
		// repeated warning events) until the operator changes or removes the
		// desired spec.
		return ctrl.Result{}, nil
	}
	if latched := r.failedLatchFor(sts, specHash); latched != nil {
		// The failure already alerted but its status write failed; keep
		// retrying the write quietly instead of re-evaluating (and
		// re-alerting) the whole reconcile.
		return ctrl.Result{}, r.writeStatus(ctx, sts, latched)
	}

	desired, err := contract.ParseDesiredSpec(raw)
	if err != nil {
		return r.fail(ctx, sts, specHash, ReasonInvalidDesiredSpec, err.Error(), nil)
	}
	if status != nil && status.ObservedSpecHash != "" && status.ObservedSpecHash != specHash {
		// The desired spec changed mid-flight; restart the state machine from
		// a clean assessment. Nothing to undo: all completed patches were
		// toward a spec the operator has since replaced, and the fresh
		// assessment below recomputes everything against the new spec.
		log.Info("desired spec changed mid-flight, reassessing", "oldHash", status.ObservedSpecHash, "newHash", specHash)
		status = nil
	}

	pvcs, err := r.listClaimPVCs(ctx, sts, desired)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := drift.Validate(desired, sts, pvcs); err != nil {
		return r.fail(ctx, sts, specHash, ReasonInvalidDesiredSpec, err.Error(), nil)
	}

	a := drift.Assess(desired, sts, pvcs)
	if a.Failed() {
		return r.fail(ctx, sts, specHash, ReasonModifyInfeasible, a.FailureReason(), a.PVCStates)
	}

	switch {
	case a.Done():
		return r.complete(ctx, sts)
	case !a.SpecsMatch():
		return r.patchPhase(ctx, sts, desired, a, status, specHash)
	case !a.Converged():
		return r.convergencePhase(ctx, sts, a, status, specHash)
	default:
		// PVCs converged; only the StatefulSet's own volumeClaimTemplates
		// still disagree. Orphan-delete so it can be recreated with matching
		// templates.
		return r.deletePhase(ctx, sts, desired, pvcs, a, status, specHash)
	}
}

// gateBlocked handles a closed health gate: warn, record the Blocked state,
// and retry — but only for so long. A gate that stays closed past the gate
// timeout latches Failed so the reconcile cannot stall silently forever.
func (r *Reconciler) gateBlocked(ctx context.Context, sts *appsv1.StatefulSet, status *contract.Status, specHash, reason string, pvcStates map[string]string) (ctrl.Result, error) {
	r.Recorder.Event(sts, corev1.EventTypeWarning, ReasonUnhealthy, reason)
	if r.DryRun {
		return ctrl.Result{RequeueAfter: gateRequeue}, nil
	}
	transition := r.anchorTime(sts, specHash, contract.StateBlocked)
	if status != nil && status.State == contract.StateBlocked && status.ObservedSpecHash == specHash && status.LastTransition.Before(transition) {
		transition = status.LastTransition
	}
	if time.Since(transition) > r.gateTimeout() {
		return r.fail(ctx, sts, specHash, ReasonHealthGateTimeout,
			fmt.Sprintf("health gate blocked reconciliation for more than %s: %s", r.gateTimeout(), reason), pvcStates)
	}
	if err := r.writeStatus(ctx, sts, &contract.Status{
		Version:          contract.SupportedVersion,
		State:            contract.StateBlocked,
		ObservedSpecHash: specHash,
		PVCs:             pvcStates,
		Reason:           reason,
		LastTransition:   transition,
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: gateRequeue}, nil
}

// patchPhase validates prerequisites and patches every drifted PVC spec.
func (r *Reconciler) patchPhase(ctx context.Context, sts *appsv1.StatefulSet, desired *contract.DesiredSpec, a *drift.Assessment, status *contract.Status, specHash string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if reason := healthGate(sts); reason != "" {
		return r.gateBlocked(ctx, sts, status, specHash, "refusing to reconcile volumes: "+reason, a.PVCStates)
	}

	retryReason, err := r.checkPrerequisites(ctx, desired, a)
	if err != nil {
		return r.fail(ctx, sts, specHash, ReasonExpansionUnsupported, err.Error(), a.PVCStates)
	}
	if retryReason != "" {
		r.Recorder.Event(sts, corev1.EventTypeWarning, ReasonMissingVAC, retryReason)
		return ctrl.Result{RequeueAfter: gateRequeue}, nil
	}

	for _, p := range a.Patches {
		if r.DryRun {
			msg := fmt.Sprintf("dry-run: would patch PVC %s (%s)", p.PVC.Name, describePatch(p))
			log.Info(msg)
			r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonDryRun, msg)
			continue
		}
		if err := r.applyPVCPatch(ctx, p); err != nil {
			return ctrl.Result{}, fmt.Errorf("patching PVC %s: %w", p.PVC.Name, err)
		}
		msg := fmt.Sprintf("patched PVC %s (%s)", p.PVC.Name, describePatch(p))
		log.Info(msg)
		r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonPVCPatched, msg)
	}

	if r.DryRun {
		// Nothing was mutated, so re-assessing would do the same again;
		// preview the rest of the flow, then stop for this StatefulSet.
		msg := "dry-run: would then wait for the CSI driver to converge PVC status"
		if a.TemplateDrift {
			msg += fmt.Sprintf(", then orphan-delete StatefulSet %s/%s for recreation with updated volumeClaimTemplates", sts.Namespace, sts.Name)
		}
		log.Info(msg)
		r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonDryRun, msg)
		return ctrl.Result{}, nil
	}
	if err := r.writeStatus(ctx, sts, &contract.Status{
		Version:          contract.SupportedVersion,
		State:            contract.StatePatching,
		ObservedSpecHash: specHash,
		PVCs:             a.PVCStates,
		LastTransition:   time.Now().UTC(),
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: patchRequeue}, nil
}

// convergencePhase waits for PVC status to catch up with the patched specs,
// bounded by the convergence timeout.
func (r *Reconciler) convergencePhase(ctx context.Context, sts *appsv1.StatefulSet, a *drift.Assessment, status *contract.Status, specHash string) (ctrl.Result, error) {
	if r.DryRun {
		return ctrl.Result{}, nil
	}

	// Anchor the timeout at the first entry into AwaitingConvergence for this
	// spec hash. The in-memory anchor backs up the persisted one so the
	// timeout still fires even if the status write below keeps failing (in
	// which case a fresh anchor would otherwise reset the clock on every
	// reconcile).
	transition := r.anchorTime(sts, specHash, contract.StateAwaitingConvergence)
	if status != nil && status.State == contract.StateAwaitingConvergence && status.ObservedSpecHash == specHash && status.LastTransition.Before(transition) {
		transition = status.LastTransition
	}
	if time.Since(transition) > r.convergenceTimeout() {
		reason := fmt.Sprintf("PVCs did not converge within %s: %s", r.convergenceTimeout(), waitingSummary(a))
		return r.fail(ctx, sts, specHash, ReasonConvergenceTimeout, reason, a.PVCStates)
	}
	if status == nil || status.State != contract.StateAwaitingConvergence {
		r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonAwaitingConvergence,
			"PVC specs match desired spec; waiting for the CSI driver to converge PVC status")
	}

	if err := r.writeStatus(ctx, sts, &contract.Status{
		Version:          contract.SupportedVersion,
		State:            contract.StateAwaitingConvergence,
		ObservedSpecHash: specHash,
		PVCs:             a.PVCStates,
		LastTransition:   transition,
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: convergenceRequeue}, nil
}

// deletePhase orphan-deletes the StatefulSet. This is the only destructive
// step and it runs last, once every PVC is verified converged. In self mode
// the manifest is snapshotted to a ConfigMap first, so the delete's watch
// event (or a restarted controller finding the snapshot) can recreate the
// StatefulSet without waiting for a deploy.
func (r *Reconciler) deletePhase(ctx context.Context, sts *appsv1.StatefulSet, desired *contract.DesiredSpec, pvcs []drift.ClaimPVC, a *drift.Assessment, status *contract.Status, specHash string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if reason := healthGate(sts); reason != "" {
		return r.gateBlocked(ctx, sts, status, specHash, "refusing to orphan-delete: "+reason, a.PVCStates)
	}

	if r.DryRun {
		msg := fmt.Sprintf("dry-run: would orphan-delete StatefulSet %s/%s (PVCs converged, volumeClaimTemplates still drifted)", sts.Namespace, sts.Name)
		if r.SelfRecreate {
			msg = fmt.Sprintf("dry-run: would snapshot the manifest, orphan-delete StatefulSet %s/%s, and recreate it with updated volumeClaimTemplates", sts.Namespace, sts.Name)
		}
		log.Info(msg)
		r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonDryRun, msg)
		return ctrl.Result{}, nil
	}

	if r.SelfRecreate {
		if len(pvcs) == 0 {
			// With no PVCs there is nothing to anchor a snapshot to (see
			// AnchorAnnotation) — and nothing was patched either, so the next
			// deploy re-applying the manifest loses nothing. Fall back to
			// deploy-mode semantics for this StatefulSet.
			r.Recorder.Event(sts, corev1.EventTypeWarning, ReasonSnapshotSkipped,
				"no PVCs exist to anchor a recreation snapshot; recreation is left to the next deploy")
		} else if err := r.writeSnapshot(ctx, sts, desired, pvcs); err != nil {
			return ctrl.Result{}, fmt.Errorf("writing recreation snapshot: %w", err)
		}
	}

	// Record the Deleting state first: if the delete fails or conflicts, the
	// annotation shows where the flow stopped. On success it simply vanishes
	// with the object.
	if err := r.writeStatus(ctx, sts, &contract.Status{
		Version:          contract.SupportedVersion,
		State:            contract.StateDeleting,
		ObservedSpecHash: specHash,
		PVCs:             a.PVCStates,
		LastTransition:   time.Now().UTC(),
	}); err != nil {
		return ctrl.Result{}, err
	}

	msg := fmt.Sprintf("orphan-deleting StatefulSet %s/%s so it can be recreated with updated volumeClaimTemplates", sts.Namespace, sts.Name)
	log.Info(msg)

	err := r.Delete(ctx, sts,
		client.PropagationPolicy(metav1.DeletePropagationOrphan),
		client.Preconditions{UID: &sts.UID, ResourceVersion: &sts.ResourceVersion},
	)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if apierrors.IsConflict(err) {
		// The object changed between our read and the delete. The update's
		// watch event usually re-runs us anyway; requeue explicitly so the
		// delete cannot stall if that event was already processed.
		return ctrl.Result{RequeueAfter: patchRequeue}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	r.clearAnchor(sts.Namespace, sts.Name)
	// Recorded only after the delete succeeded, so `kubectl describe` never
	// shows an orphan-delete that did not actually happen.
	r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonOrphanDeleted, msg)
	return ctrl.Result{}, nil
}

// writeSnapshot persists the recreation manifest before the orphan-delete and
// anchors its content hash on every existing claim PVC (see
// recreate.AnchorAnnotation). Idempotent: an existing snapshot is
// overwritten and the anchors re-stamped, so a retried deletePhase always
// captures the manifest as of the latest reconcile.
func (r *Reconciler) writeSnapshot(ctx context.Context, sts *appsv1.StatefulSet, desired *contract.DesiredSpec, pvcs []drift.ClaimPVC) error {
	cm, err := recreate.NewSnapshot(sts, desired)
	if err != nil {
		return err
	}
	if err := r.Create(ctx, cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, existing); err != nil {
			return err
		}
		existing.Labels = cm.Labels
		existing.Annotations = cm.Annotations
		existing.Data = cm.Data
		if err := r.Update(ctx, existing); err != nil {
			return err
		}
	}
	hash := recreate.SnapshotHash(cm)
	for _, cp := range pvcs {
		if cp.PVC.Annotations[recreate.AnchorAnnotation] == hash {
			continue
		}
		orig := cp.PVC.DeepCopy()
		if cp.PVC.Annotations == nil {
			cp.PVC.Annotations = map[string]string{}
		}
		cp.PVC.Annotations[recreate.AnchorAnnotation] = hash
		if err := r.Patch(ctx, cp.PVC, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("anchoring snapshot on PVC %s: %w", cp.PVC.Name, err)
		}
	}
	logf.FromContext(ctx).Info("wrote recreation snapshot", "configmap", cm.Name, "anchoredPVCs", len(pvcs))
	return nil
}

// recreateFromSnapshot recreates a deleted StatefulSet from its snapshot
// ConfigMap, then removes the snapshot. Reached via the StatefulSet delete
// watch event, or via the ConfigMap watch after a controller restart.
func (r *Reconciler) recreateFromSnapshot(ctx context.Context, key types.NamespacedName) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: recreate.SnapshotName(key.Name)}, cm)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil // nothing pending for this StatefulSet
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	sts, err := recreate.FromSnapshot(cm)
	if err != nil {
		// A corrupt snapshot cannot be acted on and retrying will not help;
		// leave the ConfigMap in place for a human to inspect.
		log.Error(err, "snapshot is unreadable; leaving it for manual inspection", "configmap", cm.Name)
		return ctrl.Result{}, nil
	}
	if sts.Name != key.Name || sts.Namespace != key.Namespace {
		r.rejectSnapshot(ctx, cm, fmt.Sprintf("manifest identity %s/%s does not match snapshot key %s", sts.Namespace, sts.Name, key))
		return ctrl.Result{}, nil
	}
	anchored, reason, err := r.snapshotAnchored(ctx, cm, sts)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !anchored {
		r.rejectSnapshot(ctx, cm, reason)
		return ctrl.Result{}, nil
	}

	if err := r.Create(ctx, sts); err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing := &appsv1.StatefulSet{}
			if getErr := r.Get(ctx, key, existing); getErr == nil && existing.DeletionTimestamp == nil {
				// Someone else recreated it (e.g. a deploy raced us). The
				// normal reconcile path owns it now; drop the stale snapshot.
				log.Info("StatefulSet already recreated externally; dropping snapshot", "configmap", cm.Name)
				return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, cm))
			}
			// The old object is still terminating (orphan finalizer); the
			// next delete watch event retries, with a requeue as backstop.
			return ctrl.Result{RequeueAfter: patchRequeue}, nil
		}
		return ctrl.Result{}, err
	}

	msg := fmt.Sprintf("recreated StatefulSet %s/%s from snapshot with updated volumeClaimTemplates", sts.Namespace, sts.Name)
	log.Info(msg)
	r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonRecreated, msg)
	if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	r.clearAnchorsFromPVCs(ctx, sts)
	return ctrl.Result{}, nil
}

// snapshotAnchored verifies that the snapshot's content hash was stamped on
// the StatefulSet's PVCs by the controller before the orphan-delete. This is
// the trust boundary of self-recreate: PVCs survive the delete window and
// writing them requires PVC patch rights, so a principal with only ConfigMap
// access cannot have the controller create an arbitrary StatefulSet. At least
// one anchored PVC is required, and every existing claim PVC must agree.
func (r *Reconciler) snapshotAnchored(ctx context.Context, cm *corev1.ConfigMap, sts *appsv1.StatefulSet) (bool, string, error) {
	want := recreate.SnapshotHash(cm)
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	anchored := 0
	for _, tmpl := range sts.Spec.VolumeClaimTemplates {
		for ordinal := int32(0); ordinal < replicas; ordinal++ {
			pvc := &corev1.PersistentVolumeClaim{}
			name := drift.PVCName(tmpl.Name, sts.Name, ordinal)
			err := r.Get(ctx, types.NamespacedName{Namespace: sts.Namespace, Name: name}, pvc)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return false, "", err
			}
			if got := pvc.Annotations[recreate.AnchorAnnotation]; got != want {
				return false, fmt.Sprintf("PVC %s does not anchor this snapshot (anchor %q, want %q)", name, got, want), nil
			}
			anchored++
		}
	}
	if anchored == 0 {
		return false, "no PVC anchors this snapshot; refusing to create a StatefulSet from unverified ConfigMap content", nil
	}
	return true, "", nil
}

// rejectSnapshot records why a snapshot was refused. The ConfigMap is left in
// place: it is evidence, and deleting it would let a forger retry silently.
func (r *Reconciler) rejectSnapshot(ctx context.Context, cm *corev1.ConfigMap, reason string) {
	logf.FromContext(ctx).Info("refusing to recreate from snapshot", "configmap", cm.Name, "reason", reason)
	r.Recorder.Event(cm, corev1.EventTypeWarning, ReasonSnapshotRejected, reason)
}

// clearAnchorsFromPVCs removes the anchor annotations once recreation
// succeeded. Best effort: a leftover anchor is harmless (it never matches a
// future snapshot's hash).
func (r *Reconciler) clearAnchorsFromPVCs(ctx context.Context, sts *appsv1.StatefulSet) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	for _, tmpl := range sts.Spec.VolumeClaimTemplates {
		for ordinal := int32(0); ordinal < replicas; ordinal++ {
			pvc := &corev1.PersistentVolumeClaim{}
			name := drift.PVCName(tmpl.Name, sts.Name, ordinal)
			if err := r.Get(ctx, types.NamespacedName{Namespace: sts.Namespace, Name: name}, pvc); err != nil {
				continue
			}
			if _, has := pvc.Annotations[recreate.AnchorAnnotation]; !has {
				continue
			}
			orig := pvc.DeepCopy()
			delete(pvc.Annotations, recreate.AnchorAnnotation)
			if err := r.Patch(ctx, pvc, client.MergeFrom(orig)); err != nil {
				logf.FromContext(ctx).Info("failed to clear snapshot anchor (harmless, will never match again)", "pvc", name, "error", err.Error())
			}
		}
	}
}

// complete clears both reconciler annotations: the terminal transition.
func (r *Reconciler) complete(ctx context.Context, sts *appsv1.StatefulSet) (ctrl.Result, error) {
	if r.DryRun {
		msg := "dry-run: PVCs and volumeClaimTemplates match desired spec; would clear reconciler annotations"
		logf.FromContext(ctx).Info(msg)
		r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonDryRun, msg)
		return ctrl.Result{}, nil
	}
	if err := r.removeAnnotations(ctx, sts, contract.DesiredSpecAnnotation, contract.StatusAnnotation); err != nil {
		return ctrl.Result{}, err
	}
	r.clearAnchor(sts.Namespace, sts.Name)
	r.clearFailedLatch(sts.Namespace, sts.Name)
	r.Recorder.Event(sts, corev1.EventTypeNormal, ReasonReconcileComplete, "PVCs match desired spec; reconciliation complete")
	logf.FromContext(ctx).Info("reconciliation complete", "statefulset", sts.Name)
	return ctrl.Result{}, nil
}

// fail latches a terminal failure into the status annotation and emits a
// warning event. No requeue: the latch holds until the desired spec changes.
func (r *Reconciler) fail(ctx context.Context, sts *appsv1.StatefulSet, specHash, reason, message string, pvcStates map[string]string) (ctrl.Result, error) {
	logf.FromContext(ctx).Info("reconcile failed", "reason", reason, "message", message)
	r.Recorder.Event(sts, corev1.EventTypeWarning, reason, message)
	if r.DryRun {
		return ctrl.Result{}, nil
	}
	r.clearAnchor(sts.Namespace, sts.Name)
	st := &contract.Status{
		Version:          contract.SupportedVersion,
		State:            contract.StateFailed,
		ObservedSpecHash: specHash,
		PVCs:             pvcStates,
		Reason:           fmt.Sprintf("%s: %s", reason, message),
		LastTransition:   time.Now().UTC(),
	}
	// Latch in memory before the write: if the write fails and is retried,
	// the retry persists this status quietly instead of re-alerting.
	r.latchFailed(sts, st)
	return ctrl.Result{}, r.writeStatus(ctx, sts, st)
}

// healthGate returns a non-empty reason when the StatefulSet is in no state
// to have its storage reconciled.
func healthGate(sts *appsv1.StatefulSet) string {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas > 0 && sts.Status.ReadyReplicas == 0 {
		return fmt.Sprintf("StatefulSet has 0 ready replicas (want %d)", replicas)
	}
	if sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return "StatefulSet has a rolling update in progress"
	}
	return ""
}

// checkPrerequisites verifies cluster-level preconditions for the pending
// patches. It returns (retryReason, nil) for conditions that may resolve on
// their own (a VAC that has not been created yet) and a non-nil error for
// terminal ones (StorageClass forbids expansion).
func (r *Reconciler) checkPrerequisites(ctx context.Context, desired *contract.DesiredSpec, a *drift.Assessment) (string, error) {
	checkedVACs := map[string]bool{}
	checkedSCs := map[string]bool{}
	for _, p := range a.Patches {
		if p.NewVAC != nil && !checkedVACs[*p.NewVAC] {
			checkedVACs[*p.NewVAC] = true
			vac := &storagev1beta1.VolumeAttributesClass{}
			if err := r.Get(ctx, types.NamespacedName{Name: *p.NewVAC}, vac); err != nil {
				if apierrors.IsNotFound(err) {
					return fmt.Sprintf("VolumeAttributesClass %q does not exist yet; waiting for it to be created", *p.NewVAC), nil
				}
				return "", err
			}
		}
		if p.NewStorage != "" {
			scName := ""
			if p.PVC.Spec.StorageClassName != nil {
				scName = *p.PVC.Spec.StorageClassName
			} else {
				// A nil storageClassName means the PVC follows the cluster
				// default StorageClass; resolve it so a non-expandable
				// default is rejected as cleanly as an explicitly named one.
				name, err := r.defaultStorageClassName(ctx)
				if err != nil {
					return "", err
				}
				scName = name
			}
			if scName == "" {
				// Explicitly classless (statically bound) or no default
				// exists: the resize admission would reject the patch anyway.
				return "", fmt.Errorf("PVC %s has no StorageClass (and no cluster default); volume expansion is impossible", p.PVC.Name)
			}
			if checkedSCs[scName] {
				continue
			}
			checkedSCs[scName] = true
			sc := &storagev1.StorageClass{}
			if err := r.Get(ctx, types.NamespacedName{Name: scName}, sc); err != nil {
				if apierrors.IsNotFound(err) {
					// Unusual, but not ours to fix; the expansion patch would
					// fail server-side anyway. Treat as terminal.
					return "", fmt.Errorf("StorageClass %q of PVC %s not found", scName, p.PVC.Name)
				}
				return "", err
			}
			if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
				return "", fmt.Errorf("StorageClass %q does not allow volume expansion (required to grow PVC %s)", scName, p.PVC.Name)
			}
		}
	}
	return "", nil
}

// defaultStorageClassName resolves the cluster's default StorageClass,
// mirroring the API server's choice when several are marked default (the most
// recently created wins). Returns "" when none is marked default.
func (r *Reconciler) defaultStorageClassName(ctx context.Context) (string, error) {
	list := &storagev1.StorageClassList{}
	if err := r.List(ctx, list); err != nil {
		return "", err
	}
	name := ""
	var newest time.Time
	for i := range list.Items {
		sc := &list.Items[i]
		if sc.Annotations["storageclass.kubernetes.io/is-default-class"] != "true" {
			continue
		}
		if name == "" || sc.CreationTimestamp.Time.After(newest) {
			name = sc.Name
			newest = sc.CreationTimestamp.Time
		}
	}
	return name, nil
}

// listClaimPVCs finds every existing PVC the StatefulSet controller created
// for the claims named in the desired spec, matching the
// <claim>-<name>-<ordinal> naming convention for ANY ordinal — not just
// 0..replicas-1. PVCs retained from a scale-down must be patched too, or a
// later scale-up would resurrect them with a stale spec. Missing PVCs need no
// handling: the recreated StatefulSet creates them from the updated template.
// (PVCs are matched by name, not label selector, because the StatefulSet
// controller does not propagate pod labels onto the PVCs it creates.)
func (r *Reconciler) listClaimPVCs(ctx context.Context, sts *appsv1.StatefulSet, desired *contract.DesiredSpec) ([]drift.ClaimPVC, error) {
	list := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, list, client.InNamespace(sts.Namespace)); err != nil {
		return nil, err
	}
	var out []drift.ClaimPVC
	for _, claim := range desired.ClaimNames() {
		prefix := claim + "-" + sts.Name + "-"
		for i := range list.Items {
			pvc := &list.Items[i]
			rest, found := strings.CutPrefix(pvc.Name, prefix)
			if !found {
				continue
			}
			if _, err := strconv.Atoi(rest); err != nil {
				continue
			}
			out = append(out, drift.ClaimPVC{Claim: claim, PVC: pvc})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PVC.Name < out[j].PVC.Name })
	return out, nil
}

// applyPVCPatch patches one PVC's spec toward the desired state.
func (r *Reconciler) applyPVCPatch(ctx context.Context, p drift.Patch) error {
	orig := p.PVC.DeepCopy()
	if p.NewVAC != nil {
		p.PVC.Spec.VolumeAttributesClassName = p.NewVAC
	}
	if p.NewStorage != "" {
		qty, err := resource.ParseQuantity(p.NewStorage)
		if err != nil {
			return err
		}
		if p.PVC.Spec.Resources.Requests == nil {
			p.PVC.Spec.Resources.Requests = corev1.ResourceList{}
		}
		p.PVC.Spec.Resources.Requests[corev1.ResourceStorage] = qty
	}
	return r.Patch(ctx, p.PVC, client.MergeFrom(orig))
}

// writeStatus persists the status annotation, skipping the write when nothing
// meaningful changed (so status writes cannot hot-loop the watch).
func (r *Reconciler) writeStatus(ctx context.Context, sts *appsv1.StatefulSet, next *contract.Status) error {
	if cur := r.currentStatus(sts); cur != nil &&
		cur.State == next.State &&
		cur.ObservedSpecHash == next.ObservedSpecHash &&
		cur.Reason == next.Reason &&
		mapsEqual(cur.PVCs, next.PVCs) {
		return nil
	}
	encoded, err := next.Encode()
	if err != nil {
		return err
	}
	orig := sts.DeepCopy()
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[contract.StatusAnnotation] = encoded
	return r.Patch(ctx, sts, client.MergeFrom(orig))
}

func (r *Reconciler) removeAnnotations(ctx context.Context, sts *appsv1.StatefulSet, keys ...string) error {
	orig := sts.DeepCopy()
	for _, k := range keys {
		delete(sts.Annotations, k)
	}
	return r.Patch(ctx, sts, client.MergeFrom(orig))
}

func (r *Reconciler) currentStatus(sts *appsv1.StatefulSet) *contract.Status {
	raw, ok := sts.Annotations[contract.StatusAnnotation]
	if !ok {
		return nil
	}
	status, err := contract.ParseStatus(raw)
	if err != nil {
		// A corrupt status is recoverable: the state machine recomputes
		// everything anyway.
		return nil
	}
	return status
}

func describePatch(p drift.Patch) string {
	var parts []string
	if p.NewVAC != nil {
		parts = append(parts, "volumeAttributesClassName="+*p.NewVAC)
	}
	if p.NewStorage != "" {
		parts = append(parts, "storage="+p.NewStorage)
	}
	return strings.Join(parts, ", ")
}

func waitingSummary(a *drift.Assessment) string {
	names := make([]string, 0, len(a.Waiting))
	for name := range a.Waiting {
		names = append(names, name)
	}
	sort.Strings(names)
	var parts []string
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s: %s", name, a.Waiting[name]))
	}
	return strings.Join(parts, "; ")
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// SetupWithManager wires the reconciler: a primary watch on StatefulSets
// (filtered by labelSelector when non-empty) and a secondary watch on PVCs
// mapped back to the StatefulSet they belong to, so CSI status updates
// trigger prompt reconciles instead of relying on requeue polling.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, labelSelector string) error {
	stsPredicates := []predicate.Predicate{}
	if labelSelector != "" {
		sel, err := metav1.ParseToLabelSelector(labelSelector)
		if err != nil {
			return fmt.Errorf("invalid label selector %q: %w", labelSelector, err)
		}
		p, err := predicate.LabelSelectorPredicate(*sel)
		if err != nil {
			return err
		}
		stsPredicates = append(stsPredicates, p)
		// Also enforce the selector inside Reconcile and the PVC mapper: the
		// informer predicate alone does not cover reconciles enqueued via the
		// PVC watch.
		compiled, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return err
		}
		r.Selector = compiled
	}

	name := "sts-volume-reconciler"
	if r.SelfRecreate {
		name += "-self"
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&appsv1.StatefulSet{}, builder.WithPredicates(stsPredicates...)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.mapPVCToStatefulSet))
	if r.SelfRecreate {
		// Snapshot ConfigMaps re-enqueue their StatefulSet, so a controller
		// restarted between orphan-delete and recreation resumes on cache
		// sync instead of leaving the StatefulSet gone until a deploy.
		b = b.Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(mapSnapshotToStatefulSet))
	}
	return b.Complete(r)
}

// mapSnapshotToStatefulSet maps a snapshot ConfigMap event to the StatefulSet
// it belongs to.
func mapSnapshotToStatefulSet(_ context.Context, obj client.Object) []reconcile.Request {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm.Labels[recreate.SnapshotLabel] != "true" {
		return nil
	}
	name := cm.Labels[recreate.StatefulSetLabel]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: cm.Namespace, Name: name}}}
}

// mapPVCToStatefulSet maps a PVC event to the annotated StatefulSet that owns
// it, using the <claim>-<name>-<ordinal> naming convention. Only StatefulSets
// currently carrying the desired-spec annotation are considered, keeping the
// fan-out tiny.
func (r *Reconciler) mapPVCToStatefulSet(ctx context.Context, obj client.Object) []reconcile.Request {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil
	}
	stsList := &appsv1.StatefulSetList{}
	if err := r.List(ctx, stsList, client.InNamespace(pvc.Namespace)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range stsList.Items {
		sts := &stsList.Items[i]
		if _, has := sts.Annotations[contract.DesiredSpecAnnotation]; !has {
			continue
		}
		if r.Selector != nil && !r.Selector.Matches(labels.Set(sts.Labels)) {
			continue
		}
		for _, tmpl := range sts.Spec.VolumeClaimTemplates {
			prefix := tmpl.Name + "-" + sts.Name + "-"
			rest, found := strings.CutPrefix(pvc.Name, prefix)
			if !found {
				continue
			}
			if _, err := strconv.Atoi(rest); err != nil {
				continue
			}
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: sts.Namespace, Name: sts.Name}})
			break
		}
	}
	return reqs
}
