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
			Labels:      map[string]string{"service": "taskbroker"},
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

func TestStaleStatusIsCleared(t *testing.T) {
	st := &contract.Status{Version: 1, State: contract.StatePatching, LastTransition: time.Now()}
	encoded, _ := st.Encode()
	f := newFixture(t, healthySTS(map[string]string{contract.StatusAnnotation: encoded}))
	f.reconcile()
	if _, has := f.getSTS().Annotations[contract.StatusAnnotation]; has {
		t.Fatal("stale status should be removed when no desired spec exists")
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
