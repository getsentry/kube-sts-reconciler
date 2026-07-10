// Standalone runner for internal/csisim, for driving the controller by hand
// on a kind cluster (see docs/testing.md, "Manual poking"). It converges PVC
// status toward spec the way a real CSI driver would — kind's local-path
// provisioner cannot modify or expand volumes, so without this a non-dry-run
// reconcile stalls in AwaitingConvergence.
//
// It mutates PVC status, so it refuses to run against anything that is not a
// kind cluster.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/getsentry/kube-sts-reconciler/internal/csisim"
)

func main() {
	var (
		namespace string
		interval  time.Duration
		latency   time.Duration
	)
	flag.StringVar(&namespace, "namespace", "sandbox", "Namespace whose PVCs to converge.")
	flag.DurationVar(&interval, "interval", 500*time.Millisecond, "Time between convergence sweeps.")
	flag.DurationVar(&latency, "latency", 2*time.Second, "How long a PVC change is observed before it is applied, approximating a slow CSI driver.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig, err := rules.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading kubeconfig: %v\n", err)
		os.Exit(1)
	}
	if !strings.HasPrefix(kubeconfig.CurrentContext, "kind-") {
		fmt.Fprintf(os.Stderr, "current kubeconfig context %q is not a kind cluster; csisim mutates PVC status and refuses to run anywhere else\n", kubeconfig.CurrentContext)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "building scheme: %v\n", err)
		os.Exit(1)
	}
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "building client: %v\n", err)
		os.Exit(1)
	}

	ctrl.Log.Info("csisim running", "context", kubeconfig.CurrentContext, "namespace", namespace, "latency", latency)
	sim := &csisim.Simulator{Client: c, Namespace: namespace, Interval: interval, Latency: latency}
	sim.Run(ctrl.SetupSignalHandler())
}
