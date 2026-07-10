//go:build integration

// Package integration runs the reconciler against a real kube-apiserver via
// envtest. There is no kubelet, scheduler, or controller-manager: the tests
// create PVCs themselves (normally the StatefulSet controller's job) and play
// the CSI driver's role by updating PVC status. What IS real here — and not
// covered by the fake-client unit tests — is API server validation and
// admission (e.g. the PersistentVolumeClaimResize admission plugin), merge
// patch semantics, and the watch-driven reconcile loop of a live manager.
//
// Run with: just test-integration
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/getsentry/kube-sts-reconciler/internal/contract"
	"github.com/getsentry/kube-sts-reconciler/internal/controller"
)

var (
	k8sClient client.Client
	testEnv   *envtest.Environment
)

const stsName = "broker"

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("KUBEBUILDER_ASSETS not set; skipping integration suite (run via `just test-integration`)")
		os.Exit(0)
	}
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	testEnv = &envtest.Environment{}
	apiServer := testEnv.ControlPlane.GetAPIServer()
	apiServer.Configure().
		Set("feature-gates", "VolumeAttributesClass=true").
		Set("runtime-config", "storage.k8s.io/v1beta1=true")

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Printf("failed to start envtest: %v\n", err)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := storagev1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Printf("failed to build client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		fmt.Printf("failed to build manager: %v\n", err)
		os.Exit(1)
	}
	r := &controller.Reconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorderFor("sts-volume-reconciler"),
	}
	if err := r.SetupWithManager(mgr, "service=taskbroker"); err != nil {
		fmt.Printf("failed to set up reconciler: %v\n", err)
		os.Exit(1)
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Printf("manager exited: %v\n", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		fmt.Println("cache never synced")
		os.Exit(1)
	}

	code := m.Run()
	cancel()
	_ = testEnv.Stop()
	os.Exit(code)
}

// --- helpers ---

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func newNamespace(t *testing.T) string {
	name := "it-" + strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "-", "_", "-").Replace(name)
	if len(name) > 60 {
		name = name[:60]
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Create(ctxT(t), ns); err != nil {
		t.Fatal(err)
	}
	return name
}

func createSC(t *testing.T, name string, allowExpansion bool) {
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          "sim.csi.sentry.io",
		AllowVolumeExpansion: &allowExpansion,
	}
	if err := k8sClient.Create(ctxT(t), sc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func createVAC(t *testing.T, name string) {
	vac := &storagev1beta1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		DriverName: "sim.csi.sentry.io",
		Parameters: map[string]string{"iops": "15000"},
	}
	if err := k8sClient.Create(ctxT(t), vac); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func strp(s string) *string { return &s }

func newSTS(ns, scName string, replicas int32) *appsv1.StatefulSet {
	labels := map[string]string{"app": stsName, "service": "taskbroker"}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: ns, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: stsName,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": stsName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": stsName}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: strp(scName),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}},
		},
	}
}

// createBoundPVCs does the StatefulSet controller's job (absent in envtest):
// create the per-ordinal PVCs and mark them Bound with capacity.
func createBoundPVCs(t *testing.T, ns, scName string, replicas int32) {
	ctx := ctxT(t)
	for i := int32(0); i < replicas; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("data-%s-%d", stsName, i), Namespace: ns},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: strp(scName),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		if err := k8sClient.Create(ctx, pvc); err != nil {
			t.Fatal(err)
		}
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.AccessModes = pvc.Spec.AccessModes
		pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
		if err := k8sClient.Status().Update(ctx, pvc); err != nil {
			t.Fatal(err)
		}
	}
}

// markReady fakes the StatefulSet controller's status so the health gate opens.
func markReady(t *testing.T, sts *appsv1.StatefulSet) {
	replicas := *sts.Spec.Replicas
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.Replicas = replicas
	sts.Status.ReadyReplicas = replicas
	sts.Status.CurrentReplicas = replicas
	sts.Status.UpdatedReplicas = replicas
	sts.Status.CurrentRevision = "rev-1"
	sts.Status.UpdateRevision = "rev-1"
	if err := k8sClient.Status().Update(ctxT(t), sts); err != nil {
		t.Fatal(err)
	}
}

func annotate(t *testing.T, ns string, desired map[string]map[string]string) {
	ctx := ctxT(t)
	sts := &appsv1.StatefulSet{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, sts); err != nil {
		t.Fatal(err)
	}
	doc := map[string]any{"version": 1, "claims": desired}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	orig := sts.DeepCopy()
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[contract.DesiredSpecAnnotation] = string(b)
	if err := k8sClient.Patch(ctx, sts, client.MergeFrom(orig)); err != nil {
		t.Fatal(err)
	}
}

func getPVC(t *testing.T, ns, name string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctxT(t), types.NamespacedName{Namespace: ns, Name: name}, pvc); err != nil {
		t.Fatal(err)
	}
	return pvc
}

// simulateCSI converges one PVC's status toward its spec.
func simulateCSI(t *testing.T, ns, name string) {
	ctx := ctxT(t)
	pvc := getPVC(t, ns, name)
	changed := false
	if pvc.Spec.VolumeAttributesClassName != nil {
		pvc.Status.CurrentVolumeAttributesClassName = pvc.Spec.VolumeAttributesClassName
		pvc.Status.ModifyVolumeStatus = nil
		changed = true
	}
	if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		if pvc.Status.Capacity == nil {
			pvc.Status.Capacity = corev1.ResourceList{}
		}
		if cur := pvc.Status.Capacity[corev1.ResourceStorage]; cur.Cmp(req) < 0 {
			pvc.Status.Capacity[corev1.ResourceStorage] = req
			changed = true
		}
	}
	if changed {
		if err := k8sClient.Status().Update(ctx, pvc); err != nil {
			t.Fatal(err)
		}
	}
}

// finalizeOrphanDelete plays the garbage collector's role (absent in
// envtest): PropagationPolicy=Orphan parks the deleted object behind the
// "orphan" finalizer, and the GC controller removes it after orphaning
// dependents. Nothing in these tests is owned by the StatefulSet, so the
// finalizer can be cleared as soon as it appears. Returns true once the
// StatefulSet is fully gone.
func finalizeOrphanDelete(t *testing.T, ns string) bool {
	t.Helper()
	ctx := ctxT(t)
	sts := &appsv1.StatefulSet{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, sts)
	if apierrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		t.Fatal(err)
	}
	if sts.DeletionTimestamp != nil && len(sts.Finalizers) > 0 {
		orig := sts.DeepCopy()
		sts.Finalizers = nil
		if err := k8sClient.Patch(ctx, sts, client.MergeFrom(orig)); err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	return false
}

func getStatus(t *testing.T, ns string) *contract.Status {
	sts := &appsv1.StatefulSet{}
	if err := k8sClient.Get(ctxT(t), types.NamespacedName{Namespace: ns, Name: stsName}, sts); err != nil {
		t.Fatal(err)
	}
	raw, ok := sts.Annotations[contract.StatusAnnotation]
	if !ok {
		return nil
	}
	st, err := contract.ParseStatus(raw)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// --- tests ---

func TestFullReconcileLoop(t *testing.T) {
	ns := newNamespace(t)
	createSC(t, "expandable", true)
	createVAC(t, "vac-fast")

	sts := newSTS(ns, "expandable", 2)
	if err := k8sClient.Create(ctxT(t), sts); err != nil {
		t.Fatal(err)
	}
	createBoundPVCs(t, ns, "expandable", 2)
	markReady(t, sts)

	annotate(t, ns, map[string]map[string]string{
		"data": {"volumeAttributesClassName": "vac-fast", "storage": "2Gi"},
	})

	// The manager patches both PVC specs.
	for _, name := range []string{"data-broker-0", "data-broker-1"} {
		name := name
		eventually(t, name+" spec patched", func() (bool, error) {
			pvc := getPVC(t, ns, name)
			vacOK := pvc.Spec.VolumeAttributesClassName != nil && *pvc.Spec.VolumeAttributesClassName == "vac-fast"
			req := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			return vacOK && req.String() == "2Gi", nil
		})
	}

	// Play the CSI driver until the controller orphan-deletes the StatefulSet.
	eventually(t, "StatefulSet orphan-deleted", func() (bool, error) {
		for _, name := range []string{"data-broker-0", "data-broker-1"} {
			simulateCSI(t, ns, name)
		}
		return finalizeOrphanDelete(t, ns), nil
	})

	// PVCs survived and keep their patched spec.
	for _, name := range []string{"data-broker-0", "data-broker-1"} {
		pvc := getPVC(t, ns, name)
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "2Gi" {
			t.Fatalf("%s lost its patch: %s", name, got.String())
		}
	}

	// "Next deploy": recreate with updated volumeClaimTemplates and the same
	// annotation still in the manifest.
	recreated := newSTS(ns, "expandable", 2)
	recreated.Spec.VolumeClaimTemplates[0].Spec.VolumeAttributesClassName = strp("vac-fast")
	recreated.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
	recreated.Annotations = map[string]string{}
	if err := k8sClient.Create(ctxT(t), recreated); err != nil {
		t.Fatal(err)
	}
	markReady(t, recreated)
	annotate(t, ns, map[string]map[string]string{
		"data": {"volumeAttributesClassName": "vac-fast", "storage": "2Gi"},
	})

	// Terminal cleanup: both annotations cleared.
	eventually(t, "annotations cleared", func() (bool, error) {
		got := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctxT(t), types.NamespacedName{Namespace: ns, Name: stsName}, got); err != nil {
			return false, err
		}
		_, hasDesired := got.Annotations[contract.DesiredSpecAnnotation]
		_, hasStatus := got.Annotations[contract.StatusAnnotation]
		return !hasDesired && !hasStatus, nil
	})
}

func TestShrinkIsRejectedByController(t *testing.T) {
	ns := newNamespace(t)
	createSC(t, "expandable", true)

	sts := newSTS(ns, "expandable", 1)
	if err := k8sClient.Create(ctxT(t), sts); err != nil {
		t.Fatal(err)
	}
	createBoundPVCs(t, ns, "expandable", 1)
	markReady(t, sts)

	annotate(t, ns, map[string]map[string]string{"data": {"storage": "512Mi"}})

	eventually(t, "Failed status", func() (bool, error) {
		st := getStatus(t, ns)
		return st != nil && st.State == contract.StateFailed, nil
	})
	if got := getPVC(t, ns, "data-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
		t.Fatalf("PVC was mutated on a rejected spec: %s", got.String())
	}
	// The StatefulSet must still exist.
	if err := k8sClient.Get(ctxT(t), types.NamespacedName{Namespace: ns, Name: stsName}, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("StatefulSet should be untouched: %v", err)
	}
}

func TestExpansionRejectedByAdmissionSurfacesAsError(t *testing.T) {
	// A StorageClass without allowVolumeExpansion: the controller's own
	// prerequisite check must mark the reconcile Failed before the API
	// server's PersistentVolumeClaimResize admission ever sees a patch.
	ns := newNamespace(t)
	createSC(t, "rigid", false)

	sts := newSTS(ns, "rigid", 1)
	if err := k8sClient.Create(ctxT(t), sts); err != nil {
		t.Fatal(err)
	}
	createBoundPVCs(t, ns, "rigid", 1)
	markReady(t, sts)

	annotate(t, ns, map[string]map[string]string{"data": {"storage": "2Gi"}})

	eventually(t, "Failed status", func() (bool, error) {
		st := getStatus(t, ns)
		return st != nil && st.State == contract.StateFailed &&
			strings.Contains(st.Reason, "does not allow volume expansion"), nil
	})
	if got := getPVC(t, ns, "data-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
		t.Fatal("PVC must not be patched when the StorageClass forbids expansion")
	}
}

func TestSkipAnnotationPreventsAction(t *testing.T) {
	ns := newNamespace(t)
	createSC(t, "expandable", true)

	sts := newSTS(ns, "expandable", 1)
	sts.Annotations = map[string]string{contract.SkipAnnotation: "true"}
	if err := k8sClient.Create(ctxT(t), sts); err != nil {
		t.Fatal(err)
	}
	createBoundPVCs(t, ns, "expandable", 1)
	markReady(t, sts)

	annotate(t, ns, map[string]map[string]string{"data": {"storage": "2Gi"}})

	// Give the controller a moment, then verify nothing moved.
	time.Sleep(2 * time.Second)
	if got := getPVC(t, ns, "data-broker-0").Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
		t.Fatal("PVC must not be patched while skip annotation is set")
	}
}
