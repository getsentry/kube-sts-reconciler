// Package drift compares the desired PVC spec from the annotation contract
// against the actual state of a StatefulSet's PVCs and volumeClaimTemplates,
// and decides what (if anything) the controller must do next.
package drift

import (
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
)

// PVCName returns the PVC created by the StatefulSet controller for a given
// volumeClaimTemplate and ordinal.
func PVCName(claimName, stsName string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-%d", claimName, stsName, ordinal)
}

// ClaimPVC pairs an existing PVC with the volumeClaimTemplate it came from.
type ClaimPVC struct {
	Claim string
	PVC   *corev1.PersistentVolumeClaim
}

// Patch describes the spec changes one PVC needs.
type Patch struct {
	Claim string
	PVC   *corev1.PersistentVolumeClaim
	// NewVAC, when non-nil, is the value for spec.volumeAttributesClassName.
	NewVAC *string
	// NewStorage, when non-empty, is the value for
	// spec.resources.requests.storage.
	NewStorage string
}

// Assessment classifies every PVC and the StatefulSet template against the
// desired spec. It is recomputed from scratch on every reconcile; nothing in
// it is cached state.
type Assessment struct {
	// Patches lists PVCs whose spec does not yet match the desired spec.
	Patches []Patch
	// Waiting maps PVC name -> human-readable reason for PVCs whose spec
	// matches but whose status has not yet converged.
	Waiting map[string]string
	// Infeasible maps PVC name -> reason for terminal, non-retryable
	// failures reported by the CSI driver (e.g. ModifyVolume infeasible).
	Infeasible map[string]string
	// PVCStates maps PVC name -> short state label for the status annotation.
	PVCStates map[string]string
	// TemplateDrift is true when the StatefulSet's volumeClaimTemplates do
	// not match the desired spec, meaning an orphan-delete + recreate is
	// still required even once all PVCs converge.
	TemplateDrift bool
}

// SpecsMatch reports whether every existing PVC's spec matches the desired spec.
func (a *Assessment) SpecsMatch() bool { return len(a.Patches) == 0 }

// Converged reports whether every existing PVC has both spec and status
// matching the desired spec.
func (a *Assessment) Converged() bool { return a.SpecsMatch() && len(a.Waiting) == 0 }

// Done reports whether there is nothing left for the controller to do.
func (a *Assessment) Done() bool { return a.Converged() && !a.TemplateDrift }

// Failed reports whether any PVC hit a terminal failure.
func (a *Assessment) Failed() bool { return len(a.Infeasible) > 0 }

// FailureReason flattens Infeasible into one deterministic message.
func (a *Assessment) FailureReason() string {
	names := make([]string, 0, len(a.Infeasible))
	for name := range a.Infeasible {
		names = append(names, name)
	}
	sort.Strings(names)
	msg := ""
	for _, name := range names {
		if msg != "" {
			msg += "; "
		}
		msg += fmt.Sprintf("%s: %s", name, a.Infeasible[name])
	}
	return msg
}

// Validate rejects desired specs the controller must never act on: claims
// that are not part of the StatefulSet, and storage shrinks (Kubernetes only
// supports expansion). Shrinks are checked against both the live PVCs and the
// StatefulSet's own template, so a shrink is rejected even when no PVCs exist
// yet. Returns a terminal error; the caller marks the reconcile Failed
// without mutating anything.
func Validate(desired *contract.DesiredSpec, sts *appsv1.StatefulSet, pvcs []ClaimPVC) error {
	templates := map[string]*corev1.PersistentVolumeClaim{}
	for i := range sts.Spec.VolumeClaimTemplates {
		t := &sts.Spec.VolumeClaimTemplates[i]
		templates[t.Name] = t
	}
	for _, name := range desired.ClaimNames() {
		t, ok := templates[name]
		if !ok {
			return fmt.Errorf("claim %q is not a volumeClaimTemplate of StatefulSet %s", name, sts.Name)
		}
		want := desired.Claims[name]
		if want.Storage == nil {
			continue
		}
		if cur, ok := t.Spec.Resources.Requests[corev1.ResourceStorage]; ok && want.Storage.Cmp(cur) < 0 {
			return fmt.Errorf("claim %q: desired storage %s is smaller than the volumeClaimTemplate request %s (Kubernetes only supports expansion)",
				name, want.Storage.String(), cur.String())
		}
	}
	for _, cp := range pvcs {
		want, ok := desired.Claims[cp.Claim]
		if !ok || want.Storage == nil {
			continue
		}
		if cur, ok := cp.PVC.Spec.Resources.Requests[corev1.ResourceStorage]; ok && want.Storage.Cmp(cur) < 0 {
			return fmt.Errorf("claim %q: desired storage %s is smaller than current request %s on PVC %s (Kubernetes only supports expansion)",
				cp.Claim, want.Storage.String(), cur.String(), cp.PVC.Name)
		}
	}
	return nil
}

// Assess computes the full drift picture. pvcs contains only PVCs that exist;
// missing PVCs are fine — the recreated StatefulSet will create them from the
// updated template.
func Assess(desired *contract.DesiredSpec, sts *appsv1.StatefulSet, pvcs []ClaimPVC) *Assessment {
	a := &Assessment{
		Waiting:    map[string]string{},
		Infeasible: map[string]string{},
		PVCStates:  map[string]string{},
	}
	for _, cp := range pvcs {
		want, ok := desired.Claims[cp.Claim]
		if !ok {
			continue
		}
		assessPVC(a, cp, want)
	}
	a.TemplateDrift = templateDrift(desired, sts)
	return a
}

func assessPVC(a *Assessment, cp ClaimPVC, want contract.ClaimDesired) {
	pvc := cp.PVC
	patch := Patch{Claim: cp.Claim, PVC: pvc}
	fsResizePending := false

	if want.VolumeAttributesClassName != nil {
		if pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName != *want.VolumeAttributesClassName {
			patch.NewVAC = want.VolumeAttributesClassName
		} else if mvs := pvc.Status.ModifyVolumeStatus; mvs != nil {
			if mvs.Status == corev1.PersistentVolumeClaimModifyVolumeInfeasible {
				a.Infeasible[pvc.Name] = fmt.Sprintf("CSI driver reports modifying volume to VolumeAttributesClass %q is infeasible", mvs.TargetVolumeAttributesClassName)
			} else {
				a.Waiting[pvc.Name] = fmt.Sprintf("volume modification to VolumeAttributesClass %q is %s", mvs.TargetVolumeAttributesClassName, mvs.Status)
			}
		} else if pvc.Status.CurrentVolumeAttributesClassName == nil || *pvc.Status.CurrentVolumeAttributesClassName != *want.VolumeAttributesClassName {
			a.Waiting[pvc.Name] = fmt.Sprintf("waiting for CSI driver to apply VolumeAttributesClass %q", *want.VolumeAttributesClassName)
		}
	}

	if want.Storage != nil {
		cur, hasReq := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if !hasReq || cur.Cmp(*want.Storage) < 0 {
			patch.NewStorage = want.Storage.String()
		} else if cap, ok := pvc.Status.Capacity[corev1.ResourceStorage]; !ok || cap.Cmp(*want.Storage) < 0 {
			// The volume may be resized while the filesystem resize waits for
			// pod churn; that finishes on its own once the StatefulSet is
			// recreated, so it is converged-enough and does not block progress.
			if hasCondition(pvc, corev1.PersistentVolumeClaimFileSystemResizePending) {
				fsResizePending = true
			} else {
				capStr := "unknown"
				if ok {
					capStr = cap.String()
				}
				a.Waiting[pvc.Name] = fmt.Sprintf("waiting for volume expansion to %s (current capacity %s)", want.Storage.String(), capStr)
			}
		} else if hasCondition(pvc, corev1.PersistentVolumeClaimResizing) {
			// Capacity is satisfied but a resize is still marked in flight.
			// Conforming resizers update capacity and conditions atomically,
			// so this is defensive — but the next step is a destructive
			// delete, so wait for the condition to clear.
			a.Waiting[pvc.Name] = "volume resize still marked in progress despite sufficient capacity"
		}
	}

	if patch.NewVAC != nil || patch.NewStorage != "" {
		a.Patches = append(a.Patches, patch)
	}

	// Precedence: a terminal failure trumps everything, then pending spec
	// patches, then in-flight convergence, then converged-enough, then done.
	switch {
	case a.Infeasible[pvc.Name] != "":
		a.PVCStates[pvc.Name] = "Infeasible"
	case patch.NewVAC != nil || patch.NewStorage != "":
		a.PVCStates[pvc.Name] = "NeedsPatch"
	case a.Waiting[pvc.Name] != "":
		a.PVCStates[pvc.Name] = "AwaitingConvergence"
	case fsResizePending:
		a.PVCStates[pvc.Name] = "FileSystemResizePending"
	default:
		a.PVCStates[pvc.Name] = "Converged"
	}
}

// templateDrift reports whether the StatefulSet's volumeClaimTemplates still
// differ from the desired spec. While they differ, the StatefulSet must be
// orphan-deleted and recreated before the flow is complete.
func templateDrift(desired *contract.DesiredSpec, sts *appsv1.StatefulSet) bool {
	templates := map[string]*corev1.PersistentVolumeClaim{}
	for i := range sts.Spec.VolumeClaimTemplates {
		t := &sts.Spec.VolumeClaimTemplates[i]
		templates[t.Name] = t
	}
	for name, want := range desired.Claims {
		t, ok := templates[name]
		if !ok {
			continue // Validate already rejects this; be defensive here.
		}
		if want.VolumeAttributesClassName != nil {
			if t.Spec.VolumeAttributesClassName == nil || *t.Spec.VolumeAttributesClassName != *want.VolumeAttributesClassName {
				return true
			}
		}
		if want.Storage != nil {
			cur, ok := t.Spec.Resources.Requests[corev1.ResourceStorage]
			if !ok || cur.Cmp(*want.Storage) < 0 {
				return true
			}
		}
	}
	return false
}

func hasCondition(pvc *corev1.PersistentVolumeClaim, condType corev1.PersistentVolumeClaimConditionType) bool {
	for _, c := range pvc.Status.Conditions {
		if c.Type == condType && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
