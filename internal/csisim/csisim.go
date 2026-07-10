// Package csisim simulates the parts of a CSI driver the reconciler waits on.
//
// Local clusters (kind + local-path provisioner) can bind PVCs and run pods,
// but they cannot execute ControllerModifyVolume (VolumeAttributesClass
// changes) or ControllerExpandVolume. This simulator closes that gap for the
// e2e harness: it watches PVCs in a namespace and converges their status
// toward their spec the way external-resizer and the CSI sidecar would —
// applying the requested VolumeAttributesClass and growing status capacity.
//
// It is a test double, never deployed to a real cluster.
package csisim

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// InfeasibleAnnotation marks a PVC whose VAC modification the simulator should
// report as infeasible (terminal CSI failure), for failure-path tests.
const InfeasibleAnnotation = "csisim.sentry.io/infeasible"

// Simulator converges PVC status toward spec on a fixed interval.
type Simulator struct {
	Client    client.Client
	Namespace string
	// Interval between convergence sweeps. Zero means 500ms.
	Interval time.Duration
	// Latency is how long a change must be observed before it is applied,
	// approximating a slow CSI driver. Zero applies changes on first sight.
	Latency time.Duration

	firstSeen map[string]time.Time
}

// Run sweeps until the context is cancelled.
func (s *Simulator) Run(ctx context.Context) {
	log := logf.Log.WithName("csisim")
	interval := s.Interval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	s.firstSeen = map[string]time.Time{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.sweep(ctx); err != nil && ctx.Err() == nil {
				log.Error(err, "sweep failed")
			}
		}
	}
}

func (s *Simulator) sweep(ctx context.Context) error {
	list := &corev1.PersistentVolumeClaimList{}
	if err := s.Client.List(ctx, list, client.InNamespace(s.Namespace)); err != nil {
		return err
	}
	for i := range list.Items {
		pvc := &list.Items[i]
		if !s.ripe(pvc) {
			continue
		}
		changed := false

		if want := pvc.Spec.VolumeAttributesClassName; want != nil {
			cur := pvc.Status.CurrentVolumeAttributesClassName
			if cur == nil || *cur != *want {
				if pvc.Annotations[InfeasibleAnnotation] == "true" {
					if pvc.Status.ModifyVolumeStatus == nil || pvc.Status.ModifyVolumeStatus.Status != corev1.PersistentVolumeClaimModifyVolumeInfeasible {
						pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
							TargetVolumeAttributesClassName: *want,
							Status:                          corev1.PersistentVolumeClaimModifyVolumeInfeasible,
						}
						changed = true
					}
				} else {
					pvc.Status.CurrentVolumeAttributesClassName = want
					pvc.Status.ModifyVolumeStatus = nil
					changed = true
				}
			}
		}

		if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && pvc.Status.Phase == corev1.ClaimBound {
			if cur, ok := pvc.Status.Capacity[corev1.ResourceStorage]; !ok || cur.Cmp(req) < 0 {
				if pvc.Status.Capacity == nil {
					pvc.Status.Capacity = corev1.ResourceList{}
				}
				pvc.Status.Capacity[corev1.ResourceStorage] = req
				changed = true
			}
		}

		if changed {
			if err := s.Client.Status().Update(ctx, pvc); err != nil {
				return err
			}
			logf.Log.WithName("csisim").Info("converged PVC status", "pvc", pvc.Name)
		}
	}
	return nil
}

// ripe implements the artificial latency: a PVC must have been seen once
// before changes to it are applied on a later sweep.
func (s *Simulator) ripe(pvc *corev1.PersistentVolumeClaim) bool {
	if s.Latency <= 0 {
		return true
	}
	first, ok := s.firstSeen[pvc.Name]
	if !ok {
		s.firstSeen[pvc.Name] = time.Now()
		return false
	}
	return time.Since(first) >= s.Latency
}
