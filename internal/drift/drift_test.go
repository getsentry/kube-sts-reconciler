package drift

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
)

func mustSpec(t *testing.T, raw string) *contract.DesiredSpec {
	t.Helper()
	spec, err := contract.ParseDesiredSpec(raw)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func sts(claims ...corev1.PersistentVolumeClaim) *appsv1.StatefulSet {
	replicas := int32(2)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "broker", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &replicas,
			VolumeClaimTemplates: claims,
		},
	}
}

func template(name, storage string, vac *string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeAttributesClassName: vac,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(storage)},
			},
		},
	}
}

func pvc(name, requested, capacity string, specVAC, currentVAC *string) *corev1.PersistentVolumeClaim {
	p := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeAttributesClassName: specVAC,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(requested)},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:                            corev1.ClaimBound,
			CurrentVolumeAttributesClassName: currentVAC,
		},
	}
	if capacity != "" {
		p.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)}
	}
	return p
}

func strp(s string) *string { return &s }

func TestPVCName(t *testing.T) {
	if got := PVCName("sqlite", "task-broker", 3); got != "sqlite-task-broker-3" {
		t.Fatalf("PVCName = %q", got)
	}
}

func TestValidateRejectsUnknownClaim(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"missing":{"storage":"10Gi"}}}`)
	err := Validate(desired, sts(template("sqlite", "100Gi", nil)), nil)
	if err == nil || !strings.Contains(err.Error(), "not a volumeClaimTemplate") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateRejectsShrink(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"10Gi"}}}`)
	pvcs := []ClaimPVC{{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "100Gi", "100Gi", nil, nil)}}
	err := Validate(desired, sts(template("sqlite", "100Gi", nil)), pvcs)
	if err == nil || !strings.Contains(err.Error(), "only supports expansion") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateRejectsShrinkAgainstTemplate(t *testing.T) {
	// No PVCs at all: the shrink must still be caught against the template.
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"10Gi"}}}`)
	err := Validate(desired, sts(template("sqlite", "100Gi", nil)), nil)
	if err == nil || !strings.Contains(err.Error(), "volumeClaimTemplate request") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateAcceptsExpansion(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"200Gi"}}}`)
	pvcs := []ClaimPVC{{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "100Gi", "100Gi", nil, nil)}}
	if err := Validate(desired, sts(template("sqlite", "100Gi", nil)), pvcs); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestAssessNeedsPatch(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"volumeAttributesClassName":"vac-new","storage":"200Gi"}}}`)
	s := sts(template("sqlite", "100Gi", nil))
	pvcs := []ClaimPVC{
		{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "100Gi", "100Gi", nil, nil)},
		{Claim: "sqlite", PVC: pvc("sqlite-broker-1", "100Gi", "100Gi", strp("vac-old"), strp("vac-old"))},
	}
	a := Assess(desired, s, pvcs)
	if len(a.Patches) != 2 {
		t.Fatalf("patches = %d, want 2", len(a.Patches))
	}
	for _, p := range a.Patches {
		if p.NewVAC == nil || *p.NewVAC != "vac-new" || p.NewStorage != "200Gi" {
			t.Errorf("patch %s: vac=%v storage=%q", p.PVC.Name, p.NewVAC, p.NewStorage)
		}
	}
	if a.SpecsMatch() || a.Converged() || a.Done() {
		t.Error("assessment should not be converged")
	}
	if !a.TemplateDrift {
		t.Error("template drift expected")
	}
	if a.PVCStates["sqlite-broker-0"] != "NeedsPatch" {
		t.Errorf("state = %q", a.PVCStates["sqlite-broker-0"])
	}
}

func TestAssessAwaitingConvergence(t *testing.T) {
	// Specs already match desired; CSI has not caught up.
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"volumeAttributesClassName":"vac-new","storage":"200Gi"}}}`)
	s := sts(template("sqlite", "100Gi", nil))
	pvcs := []ClaimPVC{
		// VAC applied per spec but status still points at old VAC; capacity lagging.
		{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "200Gi", "100Gi", strp("vac-new"), strp("vac-old"))},
	}
	a := Assess(desired, s, pvcs)
	if !a.SpecsMatch() {
		t.Fatalf("expected specs to match, patches: %+v", a.Patches)
	}
	if a.Converged() {
		t.Fatal("should be waiting for convergence")
	}
	if a.PVCStates["sqlite-broker-0"] != "AwaitingConvergence" {
		t.Errorf("state = %q", a.PVCStates["sqlite-broker-0"])
	}
}

func TestAssessModifyVolumeInProgressAndInfeasible(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"volumeAttributesClassName":"vac-new"}}}`)
	s := sts(template("sqlite", "100Gi", nil))

	inProgress := pvc("sqlite-broker-0", "100Gi", "100Gi", strp("vac-new"), strp("vac-old"))
	inProgress.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
		TargetVolumeAttributesClassName: "vac-new",
		Status:                          corev1.PersistentVolumeClaimModifyVolumeInProgress,
	}
	a := Assess(desired, s, []ClaimPVC{{Claim: "sqlite", PVC: inProgress}})
	if a.Failed() || a.Converged() {
		t.Fatalf("in-progress modify should be waiting: %+v", a)
	}

	infeasible := pvc("sqlite-broker-0", "100Gi", "100Gi", strp("vac-new"), strp("vac-old"))
	infeasible.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
		TargetVolumeAttributesClassName: "vac-new",
		Status:                          corev1.PersistentVolumeClaimModifyVolumeInfeasible,
	}
	a = Assess(desired, s, []ClaimPVC{{Claim: "sqlite", PVC: infeasible}})
	if !a.Failed() {
		t.Fatal("infeasible modify should fail the assessment")
	}
	if !strings.Contains(a.FailureReason(), "infeasible") {
		t.Errorf("reason = %q", a.FailureReason())
	}
	if a.PVCStates["sqlite-broker-0"] != "Infeasible" {
		t.Errorf("state = %q", a.PVCStates["sqlite-broker-0"])
	}
}

func TestAssessFileSystemResizePendingIsConvergedEnough(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"200Gi"}}}`)
	s := sts(template("sqlite", "200Gi", nil)) // template already updated
	p := pvc("sqlite-broker-0", "200Gi", "100Gi", nil, nil)
	p.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
		Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
		Status: corev1.ConditionTrue,
	}}
	a := Assess(desired, s, []ClaimPVC{{Claim: "sqlite", PVC: p}})
	if !a.Converged() {
		t.Fatalf("FileSystemResizePending should be converged-enough: waiting=%v", a.Waiting)
	}
	if a.PVCStates["sqlite-broker-0"] != "FileSystemResizePending" {
		t.Errorf("state = %q", a.PVCStates["sqlite-broker-0"])
	}
	if !a.Done() {
		t.Error("template matches and PVCs converged: should be done")
	}
}

func TestAssessDoneClearsEverything(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"volumeAttributesClassName":"vac-new","storage":"200Gi"}}}`)
	s := sts(template("sqlite", "200Gi", strp("vac-new")))
	pvcs := []ClaimPVC{
		{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "200Gi", "200Gi", strp("vac-new"), strp("vac-new"))},
		{Claim: "sqlite", PVC: pvc("sqlite-broker-1", "200Gi", "200Gi", strp("vac-new"), strp("vac-new"))},
	}
	a := Assess(desired, s, pvcs)
	if !a.Done() {
		t.Fatalf("expected done: patches=%v waiting=%v templateDrift=%v", a.Patches, a.Waiting, a.TemplateDrift)
	}
	for name, state := range a.PVCStates {
		if state != "Converged" {
			t.Errorf("%s state = %q", name, state)
		}
	}
}

func TestAssessTemplateDriftOnly(t *testing.T) {
	// PVCs converged but the template still carries the old spec: the
	// controller must proceed to orphan-delete, not declare success.
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"200Gi"}}}`)
	s := sts(template("sqlite", "100Gi", nil))
	pvcs := []ClaimPVC{{Claim: "sqlite", PVC: pvc("sqlite-broker-0", "200Gi", "200Gi", nil, nil)}}
	a := Assess(desired, s, pvcs)
	if !a.Converged() {
		t.Fatalf("PVCs should be converged: %+v", a.Waiting)
	}
	if !a.TemplateDrift || a.Done() {
		t.Fatal("template drift must keep the assessment not-done")
	}
}

func TestAssessMissingPVCsAreSkipped(t *testing.T) {
	desired := mustSpec(t, `{"version":1,"claims":{"sqlite":{"storage":"200Gi"}}}`)
	s := sts(template("sqlite", "200Gi", nil))
	a := Assess(desired, s, nil) // no PVCs exist at all
	if !a.Done() {
		t.Fatal("no PVCs and matching template means nothing to do")
	}
}
