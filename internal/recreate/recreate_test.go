package recreate

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
)

func strp(s string) *string { return &s }

func snapshotInput(t *testing.T) (*appsv1.StatefulSet, *contract.DesiredSpec) {
	t.Helper()
	replicas := int32(2)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "broker",
			Namespace: "default",
			Labels:    map[string]string{"service": "taskbroker"},
			Annotations: map[string]string{
				contract.DesiredSpecAnnotation:                     `{"version":1}`,
				contract.StatusAnnotation:                          `{"state":"Deleting"}`,
				"kubectl.kubernetes.io/last-applied-configuration": `{"stale":true}`,
				"team": "processing",
			},
			UID:             types.UID("uid-1"),
			ResourceVersion: "42",
			Generation:      7,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "broker"}},
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
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 2},
	}
	desired, err := contract.ParseDesiredSpec(`{"version":1,"claims":{"sqlite":{"volumeAttributesClassName":"vac-new","storage":"200Gi"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	return sts, desired
}

func TestSnapshotRoundTrip(t *testing.T) {
	sts, desired := snapshotInput(t)
	cm, err := NewSnapshot(sts, desired)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Name != "sts-snapshot-broker" || cm.Namespace != "default" {
		t.Fatalf("cm identity: %s/%s", cm.Namespace, cm.Name)
	}
	if cm.Labels[SnapshotLabel] != "true" || cm.Labels[StatefulSetLabel] != "broker" {
		t.Fatalf("cm labels: %v", cm.Labels)
	}
	if cm.Annotations[SpecHashAnnotation] != desired.Hash() {
		t.Fatal("spec hash annotation missing")
	}

	back, err := FromSnapshot(cm)
	if err != nil {
		t.Fatal(err)
	}

	// Server-managed fields and reconciler annotations are gone.
	if back.UID != "" || back.ResourceVersion != "" || back.Generation != 0 {
		t.Fatal("server-managed metadata must be stripped")
	}
	if back.Status.ReadyReplicas != 0 {
		t.Fatal("status must be stripped")
	}
	for _, k := range []string{contract.DesiredSpecAnnotation, contract.StatusAnnotation, "kubectl.kubernetes.io/last-applied-configuration"} {
		if _, has := back.Annotations[k]; has {
			t.Fatalf("annotation %q must be stripped", k)
		}
	}
	// User content is preserved.
	if back.Annotations["team"] != "processing" || back.Labels["service"] != "taskbroker" {
		t.Fatal("user labels/annotations must be preserved")
	}
	if back.Spec.Replicas == nil || *back.Spec.Replicas != 2 {
		t.Fatal("spec must be preserved")
	}

	// Templates carry the desired spec.
	tmpl := back.Spec.VolumeClaimTemplates[0].Spec
	if tmpl.VolumeAttributesClassName == nil || *tmpl.VolumeAttributesClassName != "vac-new" {
		t.Fatalf("template VAC = %v", tmpl.VolumeAttributesClassName)
	}
	if got := tmpl.Resources.Requests[corev1.ResourceStorage]; got.String() != "200Gi" {
		t.Fatalf("template storage = %s", got.String())
	}
}

func TestFromSnapshotRejectsGarbage(t *testing.T) {
	cm := &corev1.ConfigMap{Data: map[string]string{DataKey: "{not json"}}
	if _, err := FromSnapshot(cm); err == nil {
		t.Fatal("expected decode error")
	}
	cm = &corev1.ConfigMap{Data: map[string]string{}}
	if _, err := FromSnapshot(cm); err == nil {
		t.Fatal("expected missing-key error")
	}
}
