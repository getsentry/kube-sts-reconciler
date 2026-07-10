//go:build e2e

// Package e2e exercises the reconciler against a real kind cluster: real
// StatefulSet controller creating pods and PVCs, real scheduler and kubelet,
// real API server admission — with internal/csisim standing in for the CSI
// driver (kind's local-path provisioner cannot modify or expand volumes).
//
// The manager runs in-process against the current kubeconfig, which must
// point at a kind cluster (the test refuses anything else). Set up with:
//
//	make kind-up && make test-e2e     # or just: make e2e
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
	"github.com/getsentry/kube-sts-reconciler/internal/controller"
	"github.com/getsentry/kube-sts-reconciler/internal/csisim"
)

const (
	ns       = "sts-reconciler-e2e"
	stsName  = "broker"
	scName   = "e2e-expandable"
	vacName  = "e2e-vac-fast"
	claim    = "data"
	pvc0     = claim + "-" + stsName + "-0"
	driver   = "sim.csi.sentry.io"
	waitLong = 3 * time.Minute
)

// requireKind refuses to run against anything that is not a kind cluster, so
// a stray KUBECONFIG can never point this destructive test at a real cluster.
func requireKind(t *testing.T) {
	t.Helper()
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := rules.Load()
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	if !strings.HasPrefix(cfg.CurrentContext, "kind-") {
		t.Fatalf("current kubeconfig context %q is not a kind cluster; refusing to run e2e (use `make kind-up`)", cfg.CurrentContext)
	}
}

func newClient(t *testing.T) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := storagev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c, scheme
}

func startManager(t *testing.T, ctx context.Context, scheme *runtime.Scheme) {
	t.Helper()
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &controller.Reconciler{
		Client:             mgr.GetClient(),
		Recorder:           mgr.GetEventRecorderFor("sts-volume-reconciler"),
		ConvergenceTimeout: 5 * time.Minute,
	}
	if err := r.SetupWithManager(mgr, "service=taskbroker"); err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := mgr.Start(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("manager exited: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if ok {
			t.Logf("done: %s", what)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out (%s) waiting for %s", timeout, what)
		}
		time.Sleep(time.Second)
	}
}

func strp(s string) *string { return &s }

func newSTS(storage string, vac *string) *appsv1.StatefulSet {
	replicas := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: ns,
			Labels:    map[string]string{"app": stsName, "service": "taskbroker"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: stsName,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": stsName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": stsName}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: func() *int64 { v := int64(1); return &v }(),
					Containers: []corev1.Container{{
						Name:  "app",
						Image: "registry.k8s.io/pause:3.10",
						VolumeMounts: []corev1.VolumeMount{{
							Name:      claim,
							MountPath: "/data",
						}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: claim},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:               []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName:          strp(scName),
					VolumeAttributesClassName: vac,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(storage)},
					},
				},
			}},
		},
	}
}

func desiredAnnotation(t *testing.T) string {
	t.Helper()
	doc := map[string]any{
		"version": 1,
		"claims": map[string]any{
			claim: map[string]string{
				"volumeAttributesClassName": vacName,
				"storage":                   "2Gi",
			},
		},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestEndToEnd(t *testing.T) {
	requireKind(t)
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, scheme := newClient(t)

	// --- environment setup ---
	for _, obj := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName},
			// local-path provisions the volume; csisim handles what
			// local-path cannot (expansion + VAC status).
			Provisioner:          "rancher.io/local-path",
			VolumeBindingMode:    func() *storagev1.VolumeBindingMode { m := storagev1.VolumeBindingWaitForFirstConsumer; return &m }(),
			AllowVolumeExpansion: func() *bool { b := true; return &b }(),
		},
		&storagev1beta1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: vacName},
			DriverName: driver,
			Parameters: map[string]string{"iops": "15000", "throughput": "140"},
		},
	} {
		if err := c.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating %T: %v", obj, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_ = c.Delete(cleanupCtx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})

	startManager(t, ctx, scheme)
	sim := &csisim.Simulator{Client: c, Namespace: ns, Latency: 2 * time.Second}
	go sim.Run(ctx)

	// --- deploy the workload and wait for it to be genuinely running ---
	if err := c.Create(ctx, newSTS("1Gi", nil)); err != nil {
		t.Fatal(err)
	}
	eventually(t, waitLong, "StatefulSet ready with bound PVC", func() (bool, error) {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, sts); err != nil {
			return false, err
		}
		if sts.Status.ReadyReplicas != 1 {
			return false, nil
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvc0}, pvc); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})

	// --- stamp the annotation, exactly as sentry-kube (or kubectl) would ---
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, sts); err != nil {
		t.Fatal(err)
	}
	orig := sts.DeepCopy()
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[contract.DesiredSpecAnnotation] = desiredAnnotation(t)
	if err := c.Patch(ctx, sts, client.MergeFrom(orig)); err != nil {
		t.Fatal(err)
	}

	// --- controller patches the PVC spec ---
	eventually(t, time.Minute, "PVC spec patched (VAC + 2Gi)", func() (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvc0}, pvc); err != nil {
			return false, err
		}
		vacOK := pvc.Spec.VolumeAttributesClassName != nil && *pvc.Spec.VolumeAttributesClassName == vacName
		req := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		return vacOK && req.String() == "2Gi", nil
	})

	// --- csisim converges status; controller orphan-deletes the STS ---
	eventually(t, waitLong, "StatefulSet orphan-deleted after convergence", func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, &appsv1.StatefulSet{})
		return apierrors.IsNotFound(err), nil
	})

	// Orphaned: the pod and PVC must have survived the delete.
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName + "-0"}, pod); err != nil {
		t.Fatalf("pod should survive orphan-delete: %v", err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvc0}, pvc); err != nil {
		t.Fatalf("PVC should survive orphan-delete: %v", err)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "2Gi" {
		t.Fatalf("PVC spec regressed: %s", got.String())
	}
	if pvc.Status.CurrentVolumeAttributesClassName == nil || *pvc.Status.CurrentVolumeAttributesClassName != vacName {
		t.Fatal("PVC status should show the applied VAC")
	}

	// --- "next deploy" recreates the STS with matching templates ---
	recreated := newSTS("2Gi", strp(vacName))
	recreated.Annotations = map[string]string{contract.DesiredSpecAnnotation: desiredAnnotation(t)}
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}

	// --- terminal cleanup: annotations cleared, workload adopted and ready ---
	eventually(t, waitLong, "annotations cleared and StatefulSet ready", func() (bool, error) {
		got := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, got); err != nil {
			return false, err
		}
		_, hasDesired := got.Annotations[contract.DesiredSpecAnnotation]
		_, hasStatus := got.Annotations[contract.StatusAnnotation]
		return !hasDesired && !hasStatus && got.Status.ReadyReplicas == 1, nil
	})

	fmt.Println("e2e: full annotate -> patch -> converge -> orphan-delete -> recreate -> clear loop verified")
}
