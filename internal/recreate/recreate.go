// Package recreate implements the manifest snapshot used by
// --recreate-mode=self: before orphan-deleting a StatefulSet the controller
// stores a sanitized copy of its manifest in a ConfigMap, and recreates the
// StatefulSet from it (with volumeClaimTemplates merged to the desired spec)
// once the delete completes. The ConfigMap is the crash-safety net — if the
// controller dies between delete and recreate, the snapshot is still there to
// resume from.
package recreate

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
)

const (
	// SnapshotLabel marks a ConfigMap as a recreation snapshot.
	SnapshotLabel = contract.Domain + "/snapshot"
	// StatefulSetLabel holds the name of the StatefulSet the snapshot is for,
	// so ConfigMap watch events can be mapped back to a reconcile request.
	StatefulSetLabel = contract.Domain + "/statefulset"
	// SpecHashAnnotation records which desired-spec content produced the
	// snapshot, for traceability.
	SpecHashAnnotation = contract.Domain + "/spec-hash"
	// DataKey is the ConfigMap data key holding the manifest.
	DataKey = "statefulset.json"

	// lastAppliedAnnotation is kubectl's client-side apply bookkeeping; it
	// embeds the pre-change manifest and would only confuse later applies.
	lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"
)

// SnapshotName returns the ConfigMap name used for a StatefulSet's snapshot.
func SnapshotName(stsName string) string {
	return "sts-snapshot-" + stsName
}

// NewSnapshot builds the snapshot ConfigMap for a StatefulSet: the manifest
// is stripped of server-managed metadata and reconciler annotations, and its
// volumeClaimTemplates are merged with the desired spec so the recreated
// StatefulSet matches the already-patched PVCs.
func NewSnapshot(sts *appsv1.StatefulSet, desired *contract.DesiredSpec) (*corev1.ConfigMap, error) {
	manifest := sanitized(sts)
	mergeTemplates(manifest, desired)

	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshaling StatefulSet snapshot: %w", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SnapshotName(sts.Name),
			Namespace: sts.Namespace,
			Labels: map[string]string{
				SnapshotLabel:    "true",
				StatefulSetLabel: sts.Name,
			},
			Annotations: map[string]string{
				SpecHashAnnotation: desired.Hash(),
			},
		},
		Data: map[string]string{DataKey: string(raw)},
	}, nil
}

// FromSnapshot decodes the StatefulSet manifest stored in a snapshot ConfigMap.
func FromSnapshot(cm *corev1.ConfigMap) (*appsv1.StatefulSet, error) {
	raw, ok := cm.Data[DataKey]
	if !ok {
		return nil, fmt.Errorf("snapshot ConfigMap %s/%s has no %q key", cm.Namespace, cm.Name, DataKey)
	}
	sts := &appsv1.StatefulSet{}
	if err := json.Unmarshal([]byte(raw), sts); err != nil {
		return nil, fmt.Errorf("decoding snapshot ConfigMap %s/%s: %w", cm.Namespace, cm.Name, err)
	}
	return sts, nil
}

// sanitized copies only the parts of the StatefulSet that belong in a fresh
// create: name, namespace, labels, non-reconciler annotations, and spec.
// Everything server-managed (uid, resourceVersion, status, managedFields,
// ownerReferences, ...) is dropped by construction.
func sanitized(sts *appsv1.StatefulSet) *appsv1.StatefulSet {
	annotations := map[string]string{}
	for k, v := range sts.Annotations {
		switch k {
		case contract.DesiredSpecAnnotation, contract.StatusAnnotation, lastAppliedAnnotation:
			continue
		}
		annotations[k] = v
	}
	if len(annotations) == 0 {
		annotations = nil
	}
	var labels map[string]string
	if sts.Labels != nil {
		labels = map[string]string{}
		for k, v := range sts.Labels {
			labels[k] = v
		}
	}
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        sts.Name,
			Namespace:   sts.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *sts.Spec.DeepCopy(),
	}
}

// mergeTemplates applies the desired per-claim changes onto the manifest's
// volumeClaimTemplates. Validate has already rejected shrinks and unknown
// claims by the time a snapshot is taken.
func mergeTemplates(sts *appsv1.StatefulSet, desired *contract.DesiredSpec) {
	for i := range sts.Spec.VolumeClaimTemplates {
		t := &sts.Spec.VolumeClaimTemplates[i]
		want, ok := desired.Claims[t.Name]
		if !ok {
			continue
		}
		if want.VolumeAttributesClassName != nil {
			t.Spec.VolumeAttributesClassName = want.VolumeAttributesClassName
		}
		if want.Storage != nil {
			if t.Spec.Resources.Requests == nil {
				t.Spec.Resources.Requests = corev1.ResourceList{}
			}
			t.Spec.Resources.Requests[corev1.ResourceStorage] = *want.Storage
		}
	}
}
