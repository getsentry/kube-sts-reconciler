// The sts-volume-reconciler manager. See docs/implementation-plan.md.
package main

import (
	"flag"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/getsentry/kube-sts-reconciler/internal/controller"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(storagev1.AddToScheme(scheme))
	utilruntime.Must(storagev1beta1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr        string
		probeAddr          string
		leaderElect        bool
		labelSelector      string
		dryRun             bool
		convergenceTimeout time.Duration
		gateTimeout        time.Duration
		recreateMode       string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the Prometheus metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address for liveness/readiness probes.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election (required when running more than one replica).")
	flag.StringVar(&labelSelector, "label-selector", "service=taskbroker", "Only reconcile StatefulSets matching this label selector. Empty selects everything.")
	flag.BoolVar(&dryRun, "dry-run", false, "Log and emit events for intended actions without mutating anything.")
	flag.DurationVar(&convergenceTimeout, "convergence-timeout", 10*time.Minute, "How long PVC status may lag the patched spec before the reconcile is marked Failed.")
	flag.DurationVar(&gateTimeout, "gate-timeout", 10*time.Minute, "How long the health gate may block a reconcile before it is marked Failed.")
	flag.StringVar(&recreateMode, "recreate-mode", "deploy", "Who recreates the StatefulSet after orphan-delete: 'deploy' (external re-apply; only supported mode today) or 'self' (not yet implemented).")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	if recreateMode != "deploy" {
		setupLog.Error(nil, "unsupported --recreate-mode; only 'deploy' is implemented", "recreateMode", recreateMode)
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "sts-volume-reconciler.sts-reconciler.sentry.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	r := &controller.Reconciler{
		Client:             mgr.GetClient(),
		Recorder:           mgr.GetEventRecorderFor("sts-volume-reconciler"),
		DryRun:             dryRun,
		ConvergenceTimeout: convergenceTimeout,
		GateTimeout:        gateTimeout,
	}
	if err := r.SetupWithManager(mgr, labelSelector); err != nil {
		setupLog.Error(err, "unable to set up reconciler")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "dryRun", dryRun, "labelSelector", labelSelector, "convergenceTimeout", convergenceTimeout)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
