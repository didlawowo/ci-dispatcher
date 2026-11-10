// Command ci-dispatcher serves every self-hosted GitHub Actions runner
// scale set of an account from one process.
//
//	ci-dispatcher run            serve the catalog (default)
//	ci-dispatcher validate       check a catalog and print its queues
//	ci-dispatcher render <queue> print the cold pod of a queue
//	ci-dispatcher delete-queues  delete the scale sets of a catalog (test queues)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/actions/scaleset"

	"github.com/actions/scaleset/dispatcher/catalog"
	"github.com/actions/scaleset/dispatcher/claim"
	"github.com/actions/scaleset/dispatcher/controller"
	"github.com/actions/scaleset/dispatcher/gh"
	"github.com/actions/scaleset/dispatcher/metrics"
	"github.com/actions/scaleset/dispatcher/pods"
	"github.com/actions/scaleset/dispatcher/sessions"
)

type options struct {
	catalog        string
	kubeconfig     string
	claimAddr      string
	metricsAddr    string
	tokenFile      string
	appKeyFile     string
	agentImage     string
	leaderElect    bool
	leaseNamespace string
	leaseName      string
	logLevel       string
	restRate       float64
}

func main() {
	cmd := "run"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var o options
	fs.StringVar(&o.catalog, "catalog", "/etc/ci-dispatcher/catalog.yaml", "catalog file")
	fs.StringVar(&o.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig (empty: in-cluster)")
	fs.StringVar(&o.claimAddr, "claim-addr", ":8081", "claim service listen address")
	fs.StringVar(&o.metricsAddr, "metrics-addr", ":8080", "metrics and health listen address")
	fs.StringVar(&o.tokenFile, "github-token-file", envOr("CI_DISPATCHER_GITHUB_TOKEN_FILE", ""), "file holding the GitHub PAT (auth type pat)")
	fs.StringVar(&o.appKeyFile, "github-app-key-file", envOr("CI_DISPATCHER_GITHUB_APP_KEY_FILE", ""), "file holding the GitHub App private key (auth type app)")
	fs.StringVar(&o.agentImage, "agent-image", os.Getenv("CI_DISPATCHER_IMAGE"), "image carrying ci-runner-agent (default: catalog runtime.agentImage)")
	fs.BoolVar(&o.leaderElect, "leader-elect", true, "hold a Lease before serving")
	fs.StringVar(&o.leaseNamespace, "lease-namespace", os.Getenv("POD_NAMESPACE"), "namespace of the Lease (default: catalog namespace)")
	fs.StringVar(&o.leaseName, "lease-name", "ci-dispatcher", "name of the Lease")
	fs.StringVar(&o.logLevel, "log-level", "info", "debug | info | warn | error")
	fs.Float64Var(&o.restRate, "github-rest-rate", 5, "REST API requests per second, shared by every queue")
	_ = fs.Parse(args)

	log := newLogger(o.logLevel)
	var err error
	switch cmd {
	case "run":
		err = run(o, log)
	case "validate":
		err = validate(o)
	case "render":
		err = render(o, fs.Args())
	case "delete-queues":
		err = deleteQueues(o, log)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Error("ci-dispatcher failed", "error", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func version() (string, string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev", "unknown"
	}
	rev := "unknown"
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			rev = s.Value
		}
	}
	return info.Main.Version, rev
}

func labelsOf(c *catalog.Catalog) pods.Labels { return pods.Labels{Prefix: c.Runtime.LabelPrefix} }

func agentImage(o options, c *catalog.Catalog) (string, error) {
	img := o.agentImage
	if img == "" {
		img = c.Runtime.AgentImage
	}
	if img == "" {
		return "", errors.New("no agent image: set CI_DISPATCHER_IMAGE or runtime.agentImage")
	}
	return img, nil
}

func newGitHub(o options, c *catalog.Catalog, log *slog.Logger, m *metrics.Metrics) (*gh.Client, error) {
	v, rev := version()
	cfg := gh.Config{
		Owner:      c.GitHub.Owner,
		GitHubURL:  c.GitHub.URL,
		APIURL:     c.GitHub.APIURL,
		SystemInfo: scaleset.SystemInfo{System: "ci-dispatcher", Subsystem: "ci-dispatcher", Version: v, CommitSHA: rev},
		RESTRate:   o.restRate,
		Logger:     log.With("component", "github"),
	}
	if m != nil {
		cfg.OnRateLimit = func(n int) { m.RateRemaining.Set(float64(n)) }
	}
	switch c.GitHub.Auth.Type {
	case "app":
		cfg.Credentials = gh.Credentials{AppClientID: c.GitHub.Auth.AppClientID, InstallationID: c.GitHub.Auth.InstallationID, PrivateKeyFile: o.appKeyFile}
	default:
		cfg.Credentials = gh.Credentials{TokenFile: o.tokenFile}
	}
	return gh.New(cfg)
}

func kubeClient(o options) (kubernetes.Interface, error) {
	var (
		cfg *rest.Config
		err error
	)
	if o.kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", o.kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.QPS, cfg.Burst = 20, 40
	return kubernetes.NewForConfig(cfg)
}

// resolve returns the queues, asking GitHub for the visibility of every
// repository whose visibility the catalog does not force.
func resolve(ctx context.Context, c *catalog.Catalog, api gh.API) ([]catalog.Queue, error) {
	var vis catalog.Visibility
	if api != nil {
		vis = func(repo string) (bool, error) { return api.IsPublic(ctx, repo) }
	}
	return c.Resolve(vis)
}

func validate(o options) error {
	c, err := catalog.Load(o.catalog)
	if err != nil {
		return err
	}
	queues, err := c.Resolve(nil)
	if err != nil {
		return err
	}
	r, err := pods.NewRenderer(c.Templates)
	if err != nil {
		return err
	}
	for _, q := range queues {
		if _, err := r.Render(coldRequest(c, q, "check-only", "registry.example/agent@sha256:"+strings.Repeat("0", 64))); err != nil {
			return fmt.Errorf("queue %s: %w", q.Name, err)
		}
		fmt.Printf("%s\trepo=%s\tprofile=%s\ttrust=%s\tmax=%d\twarmCompatible=%v\n", q.Name, q.Repo, q.Profile, q.Trust, q.MaxRunners, c.WarmCompatible(q))
	}
	return nil
}

func coldRequest(c *catalog.Catalog, q catalog.Queue, name, agent string) pods.Request {
	return pods.Request{
		Name: name, Namespace: c.Runtime.Namespace, Queue: &q, Profile: q.Profile, Spec: q.Spec,
		ScaleSetID: 1, ClaimURL: c.Runtime.ClaimURL, AgentImage: agent, Labels: labelsOf(c),
	}
}

func render(o options, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: ci-dispatcher render --catalog <file> <queue>")
	}
	c, err := catalog.Load(o.catalog)
	if err != nil {
		return err
	}
	queues, err := c.Resolve(nil)
	if err != nil {
		return err
	}
	img, err := agentImage(o, c)
	if err != nil {
		img = "ci-dispatcher:dev"
	}
	r, err := pods.NewRenderer(c.Templates)
	if err != nil {
		return err
	}
	for _, q := range queues {
		if q.Name == args[0] {
			out, err := r.RenderYAML(coldRequest(c, q, "ci-"+q.Profile+"-xxxxx", img))
			if err != nil {
				return err
			}
			_, err = os.Stdout.Write(out)
			return err
		}
	}
	return fmt.Errorf("queue %q is not in the catalog", args[0])
}

// deleteQueues deletes the scale sets of every catalog queue. It is meant
// for test catalogs; it never runs as part of the service.
func deleteQueues(o options, log *slog.Logger) error {
	c, err := catalog.Load(o.catalog)
	if err != nil {
		return err
	}
	api, err := newGitHub(o, c, log, nil)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	queues, err := c.Resolve(nil)
	if err != nil {
		return err
	}
	var errs []error
	for _, q := range queues {
		id, created, err := api.EnsureScaleSet(ctx, q.Repo, q.Name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := api.DeleteScaleSet(ctx, q.Repo, id); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", q.Name, err))
			continue
		}
		log.Info("scale set deleted", "queue", q.Name, "scaleSetID", id, "wasMissing", created)
	}
	return errors.Join(errs...)
}

func run(o options, log *slog.Logger) error {
	c, err := catalog.Load(o.catalog)
	if err != nil {
		return err
	}
	img, err := agentImage(o, c)
	if err != nil {
		return err
	}
	m := metrics.New()
	api, err := newGitHub(o, c, log, m)
	if err != nil {
		return err
	}
	kube, err := kubeClient(o)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var leading atomic.Bool
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	// Only the leader holds the configurations: the Service routes claims to
	// ready pods, and only the leader is ready.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !leading.Load() {
			http.Error(w, "not leading", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	metricsSrv := &http.Server{Addr: o.metricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "error", err)
			cancel()
		}
	}()
	defer shutdown(metricsSrv)

	lead := func(ctx context.Context) {
		if err := serve(ctx, o, c, api, kube, m, img, log, &leading); err != nil && ctx.Err() == nil {
			log.Error("dispatcher stopped", "error", err)
			cancel()
		}
	}
	if !o.leaderElect {
		lead(ctx)
		return nil
	}
	identity := envOr("POD_NAME", hostname())
	ns := o.leaseNamespace
	if ns == "" {
		ns = c.Runtime.Namespace
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metaOf(o.leaseName, ns),
		Client:     kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   30 * time.Second,
		RenewDeadline:   20 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: lead,
			OnStoppedLeading: func() {
				leading.Store(false)
				log.Info("leadership lost")
				cancel()
			},
		},
	})
	return nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "ci-dispatcher"
	}
	return h
}

func shutdown(s *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}

// controlPlaneNodes lists the nodes labelled as control plane or etcd.
func controlPlaneNodes(nodes []*corev1.Node) []string {
	var out []string
	for _, n := range nodes {
		for _, k := range []string{"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master", "node-role.kubernetes.io/etcd"} {
			if _, ok := n.Labels[k]; ok {
				out = append(out, n.Name)
				break
			}
		}
	}
	return out
}

// serve runs everything that needs the leadership.
func serve(ctx context.Context, o options, c *catalog.Catalog, api *gh.Client, kube kubernetes.Interface, m *metrics.Metrics, img string, log *slog.Logger, leading *atomic.Bool) error {
	if c.Discovery.Enabled {
		names, err := api.InstallationRepos(ctx)
		if err != nil {
			return fmt.Errorf("discovery: %w", err)
		}
		c.AddDiscovered(names)
		log.Info("repositories discovered", "count", len(names))
	}
	queues, err := resolve(ctx, c, api)
	if err != nil {
		return err
	}
	labels := labelsOf(c)
	mgr := pods.NewManager(kube, c.Runtime.Namespace, labels)
	renderer, err := pods.NewRenderer(c.Templates)
	if err != nil {
		return err
	}
	for _, q := range queues {
		if _, err := renderer.Render(coldRequest(c, q, "check-only", img)); err != nil {
			return fmt.Errorf("queue %s: %w", q.Name, err)
		}
	}

	var ctrl *controller.Controller
	claims := claim.New(claim.Config{
		Namespace: c.Runtime.Namespace,
		Reviewer:  claim.TokenReviewer{Client: kube, Audience: pods.TokenAudience},
		Lookup:    mgr.Get,
		Managed:   func(p *corev1.Pod) bool { return p.Labels[pods.LabelManagedBy] == pods.ManagedByValue },
		Logger:    log.With("component", "claim"),
		OnClaimed: func(pod, uid string, d time.Duration) { ctrl.OnClaimed(pod, uid, d) },
	})
	ctrl = controller.New(controller.Config{
		Catalog: c, Queues: queues, API: api, Pods: mgr, Mailbox: claims, Renderer: renderer,
		Metrics: m, Labels: labels, AgentImage: img, Logger: log.With("component", "controller"),
	})
	mgr.OnPod(ctrl.OnPod)
	if err := mgr.Start(ctx); err != nil {
		return err
	}
	cp := controlPlaneNodes(mgr.Nodes())
	for _, q := range queues {
		if q.Trust != catalog.TrustPublic {
			continue
		}
		for _, n := range q.Spec.Nodes {
			if slices.Contains(cp, n) {
				return fmt.Errorf("queue %s: public pods cannot run on control-plane node %s", q.Name, n)
			}
		}
	}

	claimSrv := &http.Server{Addr: o.claimAddr, Handler: claims.Handler(), ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 90 * time.Second, IdleTimeout: 2 * time.Minute}
	go func() {
		if err := claimSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("claim server stopped", "error", err)
		}
	}()
	defer shutdown(claimSrv)

	pool := sessions.NewPool(sessions.Config{
		API: api, Handler: ctrl, Owner: envOr("POD_NAME", hostname()),
		Logger:  log.With("component", "sessions"),
		OnState: func(string, string) {},
		OnError: func(reason string) { m.SessionErrors.WithLabelValues(reason).Inc() },
	})
	specs := make([]sessions.Spec, 0, len(queues))
	for _, q := range queues {
		specs = append(specs, sessions.Spec{Name: q.Name, Repo: q.Repo, MaxRunners: q.MaxRunners})
	}
	pool.Start(ctx, specs)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); ctrl.Run(ctx) }()
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			counts := map[string]int{}
			for _, s := range pool.States() {
				counts[s]++
			}
			for _, s := range sessions.States {
				m.Sessions.WithLabelValues(s).Set(float64(counts[s]))
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	leading.Store(true)
	log.Info("dispatcher serving", "queues", len(queues), "namespace", c.Runtime.Namespace)

	<-ctx.Done()
	leading.Store(false)
	log.Info("closing sessions")
	done := make(chan struct{})
	go func() { pool.Wait(); wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(25 * time.Second):
		log.Warn("sessions not all closed before the deadline")
	}
	return nil
}

func metaOf(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace}
}
