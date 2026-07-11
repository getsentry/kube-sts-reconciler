package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
	"github.com/getsentry/kube-sts-reconciler/internal/drift"
	"github.com/getsentry/kube-sts-reconciler/internal/recreate"
)

const (
	testNS  = "default"
	testSTS = "broker"
)

func strp(s string) *string { return &s }

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := storagev1beta1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

type fixture struct {
	t        *testing.T
	client   client.Client
	recorder *record.FakeRecorder
	r        *Reconciler
}

func newFixture(t *testing.T, objs ...client.Object) *fixture {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
	rec := record.NewFakeRecorder(100)
	return &fixture{
		t:        t,
		client:   c,
		recorder: rec,
		r:        &Reconciler{Client: c, Recorder: rec},
	}
}

func (f *fixture) reconcile() ctrl.Result {
	f.t.Helper()
	res, err := f.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: testSTS},
	})
	if err != nil {
		f.t.Fatalf("reconcile error: %v", err)
	}
	return res
}

func (f *fixture) getSTS() *appsv1.StatefulSet {
	f.t.Helper()
	sts := &appsv1.StatefulSet{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: testSTS}, sts); err != nil {
		f.t.Fatalf("get sts: %v", err)
	}
	return sts
}

func (f *fixture) stsGone() bool {
	f.t.Helper()
	err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: testSTS}, &appsv1.StatefulSet{})
	return apierrors.IsNotFound(err)
}

func (f *fixture) getPVC(name string) *corev1.PersistentVolumeClaim {
	f.t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, pvc); err != nil {
		f.t.Fatalf("get pvc %s: %v", name, err)
	}
	return pvc
}

func (f *fixture) status() *contract.Status {
	f.t.Helper()
	raw, ok := f.getSTS().Annotations[contract.StatusAnnotation]
	if !ok {
		return nil
	}
	st, err := contract.ParseStatus(raw)
	if err != nil {
		f.t.Fatalf("parse status: %v", err)
	}
	return st
}

// drainEvents empties the fake recorder and returns everything seen.
func (f *fixture) drainEvents() []string {
	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func (f *fixture) expectEvent(substr string) {
	f.t.Helper()
	for _, e := range f.drainEvents() {
		if strings.Contains(e, substr) {
			return
		}
	}
	f.t.Fatalf("no event containing %q", substr)
}

// simulateCSI makes PVC status reflect its spec, playing the CSI driver's role.
func (f *fixture) simulateCSI(pvcName string) {
	f.t.Helper()
	pvc := f.getPVC(pvcName)
	if pvc.Spec.VolumeAttributesClassName != nil {
		pvc.Status.CurrentVolumeAttributesClassName = pvc.Spec.VolumeAttributesClassName
		pvc.Status.ModifyVolumeStatus = nil
	}
	if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		if pvc.Status.Capacity == nil {
			pvc.Status.Capacity = corev1.ResourceList{}
		}
		pvc.Status.Capacity[corev1.ResourceStorage] = req
	}
	if err := f.client.Status().Update(context.Background(), pvc); err != nil {
		f.t.Fatalf("simulate csi on %s: %v", pvcName, err)
	}
}

func desiredJSON(t *testing.T, claims map[string]map[string]string) string {
	t.Helper()
	doc := map[string]any{"version": 1, "claims": claims}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// healthySTS returns a 2-replica StatefulSet with one volumeClaimTemplate
// ("sqlite", 100Gi) and a ready status.
func healthySTS(annotations map[string]string) *appsv1.StatefulSet {
	replicas := int32(2)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testSTS,
			Namespace:   testNS,
			Labels:      map[string]string{"service": "broker"},
			Annotations: annotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": testSTS}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "sqlite"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")},
					},
				},
			}},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        2,
			ReadyReplicas:   2,
			CurrentRevision: "rev-1",
			UpdateRevision:  "rev-1",
		},
	}
}

func boundPVC(name, storageClass, requested, capacity string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: strp(storageClass),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(requested)},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimBound,
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
		},
	}
	return pvc
}

func expandableSC(name string) *storagev1.StorageClass {
	allow := true
	return &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          "test.csi.sentry.io",
		AllowVolumeExpansion: &allow,
	}
}

func vac(name string) *storagev1beta1.VolumeAttributesClass {
	return &storagev1beta1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		DriverName: "test.csi.sentry.io",
	}
}

func TestHappyPathVACAndExpansion(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{
		"sqlite": {"volumeAttributesClassName": "vac-new", "storage": "200Gi"},
	})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		boundPVC("sqlite-broker-1", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
		vac("vac-new"),
	)

	// Phase 1: PVC specs get patched.
	res := f.reconcile()
	if res.RequeueAfter == 0 {
		t.Fatal("expected requeue after patching")
	}
	for _, name := range []string{"sqlite-broker-0", "sqlite-broker-1"} {
		pvc := f.getPVC(name)
		if pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName != "vac-new" {
			t.Fatalf("%s VAC not patched: %v", name, pvc.Spec.VolumeAttributesClassName)
		}
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
			t.Fatalf("%s storage not patched: %s", name, got.String())
		}
	}
	if st := f.status(); st == nil || st.State != contract.StatePatching {
		t.Fatalf("status = %+v, want Patching", st)
	}
	f.expectEvent(ReasonPVCPatched)

	// Phase 2: specs match, CSI has not converged yet.
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateAwaitingConvergence {
		t.Fatalf("status = %+v, want AwaitingConvergence", st)
	}

	// CSI does its job.
	f.simulateCSI("sqlite-broker-0")
	f.simulateCSI("sqlite-broker-1")

	// Phase 3: converged; template still drifted -> orphan-delete.
	f.reconcile()
	if !f.stsGone() {
		t.Fatal("StatefulSet should have been orphan-deleted")
	}
	f.expectEvent(ReasonOrphanDeleted)
	// PVCs survive the delete (fake client has no GC, but assert they are intact).
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatal("PVC lost its patch")
	}

	// Phase 4: "next deploy" recreates the StatefulSet with updated templates
	// and the same annotation.
	recreated := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	recreated.Spec.VolumeClaimTemplates[0].Spec.VolumeAttributesClassName = strp("vac-new")
	recreated.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("200Gi")
	if err := f.client.Create(context.Background(), recreated); err != nil {
		t.Fatal(err)
	}

	f.reconcile()
	sts := f.getSTS()
	if _, has := sts.Annotations[contract.DesiredSpecAnnotation]; has {
		t.Fatal("desired-spec annotation should be cleared on completion")
	}
	if _, has := sts.Annotations[contract.StatusAnnotation]; has {
		t.Fatal("status annotation should be cleared on completion")
	}
	f.expectEvent(ReasonReconcileComplete)
}

func TestNoAnnotationIsNoOp(t *testing.T) {
	f := newFixture(t, healthySTS(nil), boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"))
	f.reconcile()
	if f.stsGone() {
		t.Fatal("sts must not be touched")
	}
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("unexpected events: %v", events)
	}
}

func TestSkipAnnotationWins(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	f := newFixture(t,
		healthySTS(map[string]string{
			contract.DesiredSpecAnnotation: desired,
			contract.SkipAnnotation:        "true",
		}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
	)
	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("PVC must not be patched while skip annotation is set")
	}
}

func TestInvalidSpecFailsAndLatches(t *testing.T) {
	f := newFixture(t, healthySTS(map[string]string{contract.DesiredSpecAnnotation: "{not json"}))
	f.reconcile()
	st := f.status()
	if st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	f.expectEvent(ReasonInvalidDesiredSpec)

	// Latched: a second reconcile emits nothing new.
	f.reconcile()
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("failed state should be latched, got events: %v", events)
	}
}

func TestShrinkIsRejected(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "50Gi"}})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
	)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("PVC must not be patched on a rejected spec")
	}
}

func TestUnhealthyGateBlocksPatching(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Status.ReadyReplicas = 0
	f := newFixture(t, sts, boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"), expandableSC("fast"))

	res := f.reconcile()
	if res.RequeueAfter == 0 {
		t.Fatal("expected a requeue while unhealthy")
	}
	f.expectEvent(ReasonUnhealthy)
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("PVC must not be patched while gate is closed")
	}
}

func TestMissingVACWaitsThenProceeds(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"volumeAttributesClassName": "vac-later"}})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
	)

	res := f.reconcile()
	if res.RequeueAfter == 0 {
		t.Fatal("expected requeue while VAC is missing")
	}
	f.expectEvent(ReasonMissingVAC)
	if f.getPVC("sqlite-broker-0").Spec.VolumeAttributesClassName != nil {
		t.Fatal("PVC must not be patched before the VAC exists")
	}

	if err := f.client.Create(context.Background(), vac("vac-later")); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.VolumeAttributesClassName; got == nil || *got != "vac-later" {
		t.Fatalf("PVC VAC = %v, want vac-later", got)
	}
}

func TestDefaultStorageClassExpansionGate(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	defaultPVC := func() *corev1.PersistentVolumeClaim {
		pvc := boundPVC("sqlite-broker-0", "ignored", "100Gi", "100Gi")
		pvc.Spec.StorageClassName = nil // relies on the cluster default
		return pvc
	}
	markDefault := func(sc *storagev1.StorageClass) *storagev1.StorageClass {
		sc.Annotations = map[string]string{"storageclass.kubernetes.io/is-default-class": "true"}
		return sc
	}

	// Expandable default: patch proceeds.
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		defaultPVC(),
		markDefault(expandableSC("cluster-default")),
	)
	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatalf("expandable default SC should allow the patch, got %s", got.String())
	}

	// Non-expandable default: latched Failed, same as a named rigid class.
	rigid := markDefault(expandableSC("cluster-default"))
	rigid.AllowVolumeExpansion = nil
	f = newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		defaultPVC(),
		rigid,
	)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed for non-expandable default SC", st)
	}
	f.expectEvent(ReasonExpansionUnsupported)

	// No default at all: expansion is impossible, latched Failed.
	f = newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		defaultPVC(),
	)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed when no default SC exists", st)
	}
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("PVC must not be patched when expansion is impossible")
	}
}

func TestExpansionForbiddenBySC(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sc := expandableSC("rigid")
	sc.AllowVolumeExpansion = nil
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "rigid", "100Gi", "100Gi"),
		sc,
	)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	f.expectEvent(ReasonExpansionUnsupported)
}

func TestConvergenceTimeout(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	f := newFixture(t, sts, boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"), expandableSC("fast"))
	f.r.ConvergenceTimeout = 50 * time.Millisecond

	f.reconcile() // patches
	f.reconcile() // enters AwaitingConvergence
	if st := f.status(); st == nil || st.State != contract.StateAwaitingConvergence {
		t.Fatalf("status = %+v, want AwaitingConvergence", st)
	}
	time.Sleep(60 * time.Millisecond)
	f.reconcile() // times out
	st := f.status()
	if st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	if !strings.Contains(st.Reason, ReasonConvergenceTimeout) {
		t.Fatalf("reason = %q", st.Reason)
	}
}

func TestSpecChangeMidFlightRestartsStateMachine(t *testing.T) {
	desiredA := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desiredA}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
	)
	f.reconcile() // patch to 200Gi
	f.reconcile() // AwaitingConvergence

	// Operator ups the ask to 300Gi mid-flight.
	sts := f.getSTS()
	desiredB := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "300Gi"}})
	sts.Annotations[contract.DesiredSpecAnnotation] = desiredB
	if err := f.client.Update(context.Background(), sts); err != nil {
		t.Fatal(err)
	}

	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "300Gi" {
		t.Fatalf("PVC should be re-patched to 300Gi, got %s", got.String())
	}
	if st := f.status(); st == nil || st.State != contract.StatePatching {
		t.Fatalf("status = %+v, want Patching against the new spec", st)
	}
}

func TestModifyVolumeInfeasibleFails(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"volumeAttributesClassName": "vac-new"}})
	pvc := boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi")
	pvc.Spec.VolumeAttributesClassName = strp("vac-new") // already patched
	pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
		TargetVolumeAttributesClassName: "vac-new",
		Status:                          corev1.PersistentVolumeClaimModifyVolumeInfeasible,
	}
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		pvc, vac("vac-new"),
	)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	f.expectEvent(ReasonModifyInfeasible)
}

func TestDryRunMutatesNothing(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{
		"sqlite": {"volumeAttributesClassName": "vac-new", "storage": "200Gi"},
	})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
		vac("vac-new"),
	)
	f.r.DryRun = true

	f.reconcile()
	pvc := f.getPVC("sqlite-broker-0")
	if pvc.Spec.VolumeAttributesClassName != nil {
		t.Fatal("dry-run must not patch PVCs")
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("dry-run must not patch PVC storage")
	}
	if f.status() != nil {
		t.Fatal("dry-run must not write status annotations")
	}
	if f.stsGone() {
		t.Fatal("dry-run must not delete")
	}
	f.expectEvent(ReasonDryRun)
}

func TestDryRunFailureAlertsOnce(t *testing.T) {
	// An invalid spec in dry-run cannot persist a Failed status, so the
	// in-memory latch must prevent every subsequent reconcile from
	// re-emitting the same warning.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "50Gi"}}) // shrink
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
	)
	f.r.DryRun = true

	f.reconcile()
	f.expectEvent(ReasonInvalidDesiredSpec)
	if f.status() != nil {
		t.Fatal("dry-run must not write status annotations")
	}

	f.reconcile()
	f.reconcile()
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("dry-run failure must alert once, got %v", events)
	}
}

func TestRejectedSnapshotAlertsOnce(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := recreate.NewSnapshot(healthySTS(nil), spec)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, forged) // no anchoring PVCs: rejected
	f.r.SelfRecreate = true

	f.reconcile()
	f.expectEvent(ReasonSnapshotRejected)

	// The ConfigMap watch keeps enqueueing; the refusal must not re-alert.
	f.reconcile()
	f.reconcile()
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("unchanged rejected snapshot must alert once, got %v", events)
	}
}

func TestStaleStatusIsCleared(t *testing.T) {
	st := &contract.Status{Version: 1, State: contract.StatePatching, LastTransition: time.Now()}
	encoded, _ := st.Encode()
	f := newFixture(t, healthySTS(map[string]string{contract.StatusAnnotation: encoded}))
	f.reconcile()
	if _, has := f.getSTS().Annotations[contract.StatusAnnotation]; has {
		t.Fatal("stale status should be removed when no desired spec exists")
	}
}

func TestScaledDownPVCsArePatchedToo(t *testing.T) {
	// Replicas is 2, but a PVC from a previous scale-up to 3 still exists.
	// It must be patched as well, or a later scale-up would resurrect it
	// with a stale spec.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		boundPVC("sqlite-broker-1", "fast", "100Gi", "100Gi"),
		boundPVC("sqlite-broker-2", "fast", "100Gi", "100Gi"), // retained from scale-down
		expandableSC("fast"),
	)
	f.reconcile()
	for _, name := range []string{"sqlite-broker-0", "sqlite-broker-1", "sqlite-broker-2"} {
		if got := f.getPVC(name).Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
			t.Fatalf("%s not patched: %s", name, got.String())
		}
	}
}

func TestSelectorEnforcedOutsideInformer(t *testing.T) {
	// The informer predicate filters watch events, but reconciles can also be
	// enqueued via the PVC mapper; the reconciler itself must enforce the
	// selector on those paths.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Labels = map[string]string{"service": "other"}
	f := newFixture(t, sts, boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"), expandableSC("fast"))
	sel, err := metav1.ParseToLabelSelector("service=broker")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		t.Fatal(err)
	}
	f.r.Selector = compiled

	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("out-of-scope StatefulSet must not be reconciled")
	}
	// The PVC mapper must not enqueue out-of-scope StatefulSets either.
	if reqs := f.r.mapPVCToStatefulSet(context.Background(), f.getPVC("sqlite-broker-0")); len(reqs) != 0 {
		t.Fatalf("mapper enqueued out-of-scope STS: %v", reqs)
	}
}

func TestShrinkRejectedEvenWithoutPVCs(t *testing.T) {
	// No PVCs exist at all; the shrink must still be caught (against the
	// volumeClaimTemplate) instead of sailing through to completion.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "50Gi"}})
	f := newFixture(t, healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}))
	f.reconcile()
	st := f.status()
	if st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	sts := f.getSTS()
	if _, has := sts.Annotations[contract.DesiredSpecAnnotation]; !has {
		t.Fatal("desired annotation must not be cleared on a rejected shrink")
	}
}

func TestGateTimeoutLatchesFailed(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Status.ReadyReplicas = 0
	f := newFixture(t, sts, boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"), expandableSC("fast"))
	f.r.GateTimeout = 50 * time.Millisecond

	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateBlocked {
		t.Fatalf("status = %+v, want Blocked", st)
	}
	time.Sleep(60 * time.Millisecond)
	f.reconcile()
	st := f.status()
	if st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed after gate timeout", st)
	}
	if !strings.Contains(st.Reason, ReasonHealthGateTimeout) {
		t.Fatalf("reason = %q", st.Reason)
	}
	// Nothing was mutated and the StatefulSet survives.
	if f.stsGone() {
		t.Fatal("gate timeout must not delete anything")
	}
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "100Gi" {
		t.Fatal("gate timeout must not patch anything")
	}
}

func TestGateRecoveryProceedsWithPatch(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Status.ReadyReplicas = 0
	f := newFixture(t, sts, boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"), expandableSC("fast"))

	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateBlocked {
		t.Fatalf("status = %+v, want Blocked", st)
	}

	// Pods recover before the gate timeout.
	cur := f.getSTS()
	cur.Status.ReadyReplicas = 2
	if err := f.client.Status().Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatal("patching should proceed once the gate clears")
	}
	if st := f.status(); st == nil || st.State != contract.StatePatching {
		t.Fatalf("status = %+v, want Patching", st)
	}
}

func TestDeleteGateTimeoutLatchesFailed(t *testing.T) {
	// PVCs already converged; only the template drifts. Pods go unhealthy
	// before the orphan-delete: the gate must block, then latch Failed.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Status.ReadyReplicas = 0
	pvc := boundPVC("sqlite-broker-0", "fast", "200Gi", "200Gi")
	f := newFixture(t, sts, pvc, expandableSC("fast"))
	f.r.GateTimeout = 50 * time.Millisecond

	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateBlocked {
		t.Fatalf("status = %+v, want Blocked", st)
	}
	time.Sleep(60 * time.Millisecond)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed", st)
	}
	if f.stsGone() {
		t.Fatal("StatefulSet must survive a blocked delete")
	}
}

func TestSelfRecreateFullLoop(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{
		"sqlite": {"volumeAttributesClassName": "vac-new", "storage": "200Gi"},
	})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		boundPVC("sqlite-broker-1", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
		vac("vac-new"),
	)
	f.r.SelfRecreate = true

	f.reconcile() // patch PVCs
	f.reconcile() // AwaitingConvergence
	f.simulateCSI("sqlite-broker-0")
	f.simulateCSI("sqlite-broker-1")
	f.reconcile() // snapshot + orphan-delete
	if !f.stsGone() {
		t.Fatal("StatefulSet should be deleted")
	}
	cm := &corev1.ConfigMap{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: recreate.SnapshotName(testSTS)}, cm); err != nil {
		t.Fatalf("snapshot ConfigMap should exist while STS is gone: %v", err)
	}

	f.reconcile() // NotFound path: recreate from snapshot
	sts := f.getSTS()
	tmpl := sts.Spec.VolumeClaimTemplates[0].Spec
	if tmpl.VolumeAttributesClassName == nil || *tmpl.VolumeAttributesClassName != "vac-new" {
		t.Fatalf("recreated template VAC = %v", tmpl.VolumeAttributesClassName)
	}
	if got := tmpl.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatalf("recreated template storage = %s", got.String())
	}
	if _, has := sts.Annotations[contract.DesiredSpecAnnotation]; has {
		t.Fatal("recreated StatefulSet must not carry reconciler annotations")
	}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: recreate.SnapshotName(testSTS)}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("snapshot ConfigMap should be deleted after recreation, got %v", err)
	}
	f.expectEvent(ReasonRecreated)

	// PVCs kept their patched specs throughout.
	if got := f.getPVC("sqlite-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatal("PVC lost its patch across the recreate")
	}
}

// anchoredPVC returns a PVC carrying the snapshot's anchor hash, as
// writeSnapshot would have stamped it before the orphan-delete.
func anchoredPVC(name string, snapshot *corev1.ConfigMap) *corev1.PersistentVolumeClaim {
	pvc := boundPVC(name, "fast", "200Gi", "200Gi")
	pvc.Annotations = map[string]string{recreate.AnchorAnnotation: recreate.SnapshotHash(snapshot)}
	return pvc
}

func TestSelfRecreateRecoversFromOrphanedSnapshot(t *testing.T) {
	// Simulates a controller crash between orphan-delete and recreation: no
	// StatefulSet, only the snapshot ConfigMap and the anchor-stamped PVCs.
	// The reconcile (triggered by the ConfigMap watch on cache sync) must
	// recreate the StatefulSet.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := recreate.NewSnapshot(healthySTS(nil), spec)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, snapshot,
		anchoredPVC("sqlite-broker-0", snapshot),
		anchoredPVC("sqlite-broker-1", snapshot),
	)
	f.r.SelfRecreate = true

	// The ConfigMap watch maps the snapshot back to the StatefulSet name.
	reqs := mapSnapshotToStatefulSet(context.Background(), snapshot)
	if len(reqs) != 1 || reqs[0].Name != testSTS {
		t.Fatalf("mapper reqs = %v", reqs)
	}

	f.reconcile()
	sts := f.getSTS()
	if got := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatalf("recovered template storage = %s", got.String())
	}
	// Anchors are cleaned up after a successful recreation.
	if _, has := f.getPVC("sqlite-broker-0").Annotations[recreate.AnchorAnnotation]; has {
		t.Fatal("anchor annotation should be cleared after recreation")
	}
}

func TestSelfRecreateRefusesDeleteWithoutAnchorablePVCs(t *testing.T) {
	// Scaled-to-zero StatefulSet: template drift, no PVCs at all. In self
	// mode the delete must be refused — an unanchorable snapshot means the
	// StatefulSet could never be recreated. The gate timeout bounds the wait.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	zero := int32(0)
	sts.Spec.Replicas = &zero
	sts.Status.Replicas = 0
	sts.Status.ReadyReplicas = 0
	f := newFixture(t, sts) // no PVCs
	f.r.SelfRecreate = true
	f.r.GateTimeout = 50 * time.Millisecond

	res := f.reconcile()
	if f.stsGone() {
		t.Fatal("self mode must not delete a StatefulSet it cannot recreate")
	}
	if res.RequeueAfter == 0 {
		t.Fatal("expected a requeue while blocked")
	}
	if st := f.status(); st == nil || st.State != contract.StateBlocked {
		t.Fatalf("status = %+v, want Blocked", st)
	}

	time.Sleep(60 * time.Millisecond)
	f.reconcile()
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed after gate timeout", st)
	}
	if f.stsGone() {
		t.Fatal("StatefulSet must survive")
	}
}

func TestSelfRecreateWithPartialClaimSpec(t *testing.T) {
	// Two volumeClaimTemplates, but the desired spec touches only "sqlite".
	// The snapshot must anchor the "logs" PVCs too, or its own verification
	// would reject it after the orphan-delete and strand the workload.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	sts := healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired})
	sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "logs"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	})
	f := newFixture(t, sts,
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		boundPVC("sqlite-broker-1", "fast", "100Gi", "100Gi"),
		boundPVC("logs-broker-0", "fast", "10Gi", "10Gi"),
		boundPVC("logs-broker-1", "fast", "10Gi", "10Gi"),
		expandableSC("fast"),
	)
	f.r.SelfRecreate = true

	f.reconcile() // patch sqlite PVCs
	f.reconcile() // AwaitingConvergence
	f.simulateCSI("sqlite-broker-0")
	f.simulateCSI("sqlite-broker-1")
	f.reconcile() // snapshot (anchoring ALL claim PVCs) + orphan-delete
	if !f.stsGone() {
		t.Fatal("StatefulSet should be deleted")
	}
	for _, name := range []string{"sqlite-broker-0", "logs-broker-0", "logs-broker-1"} {
		if _, has := f.getPVC(name).Annotations[recreate.AnchorAnnotation]; !has {
			t.Fatalf("%s must carry the snapshot anchor", name)
		}
	}

	f.reconcile() // recreate from snapshot; must not be rejected
	sts = f.getSTS()
	if got := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatalf("recreated sqlite template = %s", got.String())
	}
	if got := sts.Spec.VolumeClaimTemplates[1].Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "10Gi" {
		t.Fatalf("untouched logs template = %s", got.String())
	}
	for _, name := range []string{"sqlite-broker-0", "logs-broker-0"} {
		if _, has := f.getPVC(name).Annotations[recreate.AnchorAnnotation]; has {
			t.Fatalf("%s anchor should be cleared after recreation", name)
		}
	}
}

func TestSnapshotAnchoredByHighOrdinalPVCOnly(t *testing.T) {
	// Only a PVC beyond replicas-1 (retained from a scale-down) survives and
	// anchors the snapshot. Recovery must still work: verification matches
	// the same PVC set writeSnapshot anchored, at any ordinal.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := recreate.NewSnapshot(healthySTS(nil), spec)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, snapshot, anchoredPVC("sqlite-broker-5", snapshot))
	f.r.SelfRecreate = true

	f.reconcile()
	if f.stsGone() {
		t.Fatal("snapshot anchored by a high-ordinal PVC must still recreate the StatefulSet")
	}
	if _, has := f.getPVC("sqlite-broker-5").Annotations[recreate.AnchorAnnotation]; has {
		t.Fatal("anchor on the high-ordinal PVC should be cleared after recreation")
	}
}

func TestForgedSnapshotIsRejected(t *testing.T) {
	// An attacker with only ConfigMap write access forges a snapshot for a
	// StatefulSet that never went through the controller's delete flow. No
	// PVC carries the anchor hash, so recreation must be refused.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := recreate.NewSnapshot(healthySTS(nil), spec)
	if err != nil {
		t.Fatal(err)
	}

	// Case 1: no PVCs at all — nothing anchors the snapshot.
	f := newFixture(t, forged)
	f.r.SelfRecreate = true
	f.reconcile()
	if !f.stsGone() {
		t.Fatal("unanchored snapshot must not create a StatefulSet")
	}
	f.expectEvent(ReasonSnapshotRejected)
	// The evidence is preserved.
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: recreate.SnapshotName(testSTS)}, &corev1.ConfigMap{}); err != nil {
		t.Fatalf("rejected snapshot must be left in place: %v", err)
	}

	// Case 2: PVCs exist but anchor a different (legitimate) snapshot hash —
	// e.g. the attacker overwrote the controller's ConfigMap content.
	pvc := boundPVC("sqlite-broker-0", "fast", "200Gi", "200Gi")
	pvc.Annotations = map[string]string{recreate.AnchorAnnotation: "sha256:somethingelse"}
	f = newFixture(t, forged, pvc)
	f.r.SelfRecreate = true
	f.reconcile()
	if !f.stsGone() {
		t.Fatal("snapshot with mismatched anchor must not create a StatefulSet")
	}
	f.expectEvent(ReasonSnapshotRejected)
}

func TestSnapshotIdentityMismatchIsRejected(t *testing.T) {
	// The ConfigMap is named/labeled for one StatefulSet but its manifest
	// declares another: refuse rather than create the impostor.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	impostor := healthySTS(nil)
	impostor.Name = "evil"
	snapshot, err := recreate.NewSnapshot(impostor, spec)
	if err != nil {
		t.Fatal(err)
	}
	// Rename the ConfigMap so it claims to be broker's snapshot.
	snapshot.Name = recreate.SnapshotName(testSTS)
	snapshot.Labels[recreate.StatefulSetLabel] = testSTS

	f := newFixture(t, snapshot, anchoredPVC("sqlite-broker-0", snapshot))
	f.r.SelfRecreate = true
	f.reconcile()
	if !f.stsGone() {
		t.Fatal("identity-mismatched snapshot must not create broker")
	}
	err = f.client.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "evil"}, &appsv1.StatefulSet{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("impostor StatefulSet must not be created either, got %v", err)
	}
	f.expectEvent(ReasonSnapshotRejected)
}

func TestDeployModeDoesNotRecreate(t *testing.T) {
	// A snapshot ConfigMap exists (e.g. left over from a self-mode run), but
	// the controller runs in deploy mode: a deleted StatefulSet must stay
	// deleted.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	spec, err := contract.ParseDesiredSpec(desired)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := recreate.NewSnapshot(healthySTS(nil), spec)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, snapshot)

	f.reconcile()
	if !f.stsGone() {
		t.Fatal("deploy mode must never create StatefulSets")
	}
}

func TestFailedLatchSurvivesStatusWriteFailure(t *testing.T) {
	// A shrink is terminal. Even if the Failed status annotation could not be
	// written (simulated by clearing it), a second reconcile must not
	// re-emit the warning event thanks to the in-memory latch.
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "50Gi"}})
	f := newFixture(t,
		healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}),
		boundPVC("sqlite-broker-0", "fast", "100Gi", "100Gi"),
		expandableSC("fast"),
	)
	f.reconcile()
	f.drainEvents()

	// Simulate the status write having been lost.
	sts := f.getSTS()
	delete(sts.Annotations, contract.StatusAnnotation)
	if err := f.client.Update(context.Background(), sts); err != nil {
		t.Fatal(err)
	}

	f.reconcile()
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("in-memory latch should suppress re-alerts, got %v", events)
	}
	// And the retried write restored the persisted latch.
	if st := f.status(); st == nil || st.State != contract.StateFailed {
		t.Fatalf("status = %+v, want Failed restored from memory latch", st)
	}
}

func TestMapPVCToStatefulSet(t *testing.T) {
	desired := desiredJSON(t, map[string]map[string]string{"sqlite": {"storage": "200Gi"}})
	f := newFixture(t, healthySTS(map[string]string{contract.DesiredSpecAnnotation: desired}))

	reqs := f.r.mapPVCToStatefulSet(context.Background(), boundPVC("sqlite-broker-1", "fast", "100Gi", "100Gi"))
	if len(reqs) != 1 || reqs[0].Name != testSTS {
		t.Fatalf("reqs = %v", reqs)
	}
	// Unrelated PVC maps to nothing.
	reqs = f.r.mapPVCToStatefulSet(context.Background(), boundPVC("data-other-0", "fast", "1Gi", "1Gi"))
	if len(reqs) != 0 {
		t.Fatalf("unrelated PVC mapped: %v", reqs)
	}
	// Name that shares the prefix but has no ordinal suffix maps to nothing.
	reqs = f.r.mapPVCToStatefulSet(context.Background(), boundPVC("sqlite-broker-backup", "fast", "1Gi", "1Gi"))
	if len(reqs) != 0 {
		t.Fatalf("non-ordinal PVC mapped: %v", reqs)
	}
}

var _ = drift.PVCName // keep import if helpers change
