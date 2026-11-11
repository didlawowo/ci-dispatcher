// Package controller turns the session statistics into runner pods: it
// counts the runners of every queue, asks the budget where new ones may
// start, hands out warm pods or creates cold ones, deletes finished pods and
// collects what is left behind.
package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/actions/scaleset"

	"github.com/actions/scaleset/dispatcher/budget"
	"github.com/actions/scaleset/dispatcher/catalog"
	"github.com/actions/scaleset/dispatcher/claim"
	"github.com/actions/scaleset/dispatcher/gh"
	"github.com/actions/scaleset/dispatcher/metrics"
	"github.com/actions/scaleset/dispatcher/pods"
	"github.com/actions/scaleset/dispatcher/sessions"
	"github.com/actions/scaleset/dispatcher/warmpool"
)

// Timing of the controller.
const (
	reconcileEvery     = 15 * time.Second
	gcEvery            = 5 * time.Minute
	runnerGCEvery      = 15 * time.Minute
	completedGrace     = 60 * time.Second
	unclaimedMaxAge    = 10 * time.Minute
	lostMailboxMinAge  = 30 * time.Second
	warmNotReadyMaxAge = 10 * time.Minute
	scaleDownMinAge    = 2 * time.Minute
	maxParallelStarts  = 8
	runnerNamePrefix   = "ci-"
	warmCacheRoot      = "/mnt/language-cache"
)

// PodAPI is what the controller needs from the pod manager.
type PodAPI interface {
	Managed() []*corev1.Pod
	RunnerPods() []*corev1.Pod
	Get(name string) (*corev1.Pod, bool)
	Create(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error)
	Delete(ctx context.Context, name string, uid types.UID) error
	Patch(ctx context.Context, name string, uid types.UID, labels, annotations map[string]string) error
}

// Mailbox is what the controller needs from the claim server.
type Mailbox interface {
	Deposit(pod, uid string, p claim.Payload) error
	Pending(pod string) bool
	Ready(pod string) (time.Time, bool)
	Forget(pod string)
}

// Config builds a Controller.
type Config struct {
	Catalog    *catalog.Catalog
	Queues     []catalog.Queue
	API        gh.API
	Pods       PodAPI
	Mailbox    Mailbox
	Renderer   *pods.Renderer
	Metrics    *metrics.Metrics
	Labels     pods.Labels
	AgentImage string
	Logger     *slog.Logger
	// Now replaces the clock (tests).
	Now func() time.Time
}

type queueState struct {
	q          catalog.Queue
	scaleSetID int
	desired    int
	inflight   int
	warmOK     bool
}

// Controller implements sessions.Handler.
type Controller struct {
	cfg     Config
	log     *slog.Logger
	planner budget.Planner
	warm    *warmpool.Pool
	notify  chan struct{}
	sem     chan struct{}

	mu       sync.Mutex
	queues   map[string]*queueState
	byID     map[int]*queueState
	assigned map[string]time.Time // pod name → assignment time
	sources  map[string]string    // pod name → warm | cold
	warmInfl map[string]int       // profile → warm pods being created
}

// New builds a Controller.
func New(cfg Config) *Controller {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	c := &Controller{
		cfg:      cfg,
		log:      cfg.Logger,
		notify:   make(chan struct{}, 1),
		sem:      make(chan struct{}, maxParallelStarts),
		queues:   map[string]*queueState{},
		byID:     map[int]*queueState{},
		assigned: map[string]time.Time{},
		sources:  map[string]string{},
		warmInfl: map[string]int{},
	}
	c.warm = warmpool.New(cfg.Labels, cfg.Mailbox.Ready)
	for _, q := range cfg.Queues {
		c.queues[q.Name] = &queueState{q: q, desired: q.MinRunners, warmOK: cfg.Catalog.WarmCompatible(q)}
	}
	return c
}

// Warm exposes the warm pool (claim callbacks).
func (c *Controller) Warm() *warmpool.Pool { return c.warm }

func (c *Controller) kick() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *Controller) metricsQueue(q catalog.Queue) metrics.Queue {
	return metrics.Queue{Name: q.Name, Namespace: c.cfg.Catalog.Runtime.Namespace, Repository: q.Repo}
}

// OnScaleSet implements sessions.Handler.
func (c *Controller) OnScaleSet(queue string, id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	qs, ok := c.queues[queue]
	if !ok {
		return
	}
	if qs.scaleSetID != 0 && qs.scaleSetID != id {
		delete(c.byID, qs.scaleSetID)
	}
	qs.scaleSetID = id
	c.byID[id] = qs
}

// OnStatistics implements sessions.Handler: the wanted runner count is the
// assigned jobs, bounded by maxRunners, with minRunners as a floor.
func (c *Controller) OnStatistics(queue string, st *scaleset.RunnerScaleSetStatistic) {
	c.mu.Lock()
	qs, ok := c.queues[queue]
	if !ok {
		c.mu.Unlock()
		return
	}
	desired := min(max(st.TotalAssignedJobs, qs.q.MinRunners), qs.q.MaxRunners)
	changed := desired != qs.desired
	qs.desired = desired
	q := qs.q
	c.mu.Unlock()
	c.cfg.Metrics.SetStatistics(c.metricsQueue(q), st, desired, q.MinRunners, q.MaxRunners)
	if changed {
		c.kick()
	}
}

// OnJobStarted implements sessions.Handler.
func (c *Controller) OnJobStarted(queue string, j *scaleset.JobStarted) {
	repo := c.repoOf(queue)
	c.cfg.Metrics.JobStarted(repo, j)
	if p, ok := c.cfg.Pods.Get(j.RunnerName); ok && isManaged(p) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.cfg.Pods.Patch(ctx, p.Name, p.UID, map[string]string{c.cfg.Labels.State(): pods.StateBusy}, nil); err != nil && !pods.IsGone(err) {
				c.cfg.Metrics.PodErrors.WithLabelValues("patch_busy").Inc()
			}
		}()
	}
}

// OnJobCompleted implements sessions.Handler. The runner pod exits by itself
// after its job; it is deleted after a grace period in case it does not.
func (c *Controller) OnJobCompleted(queue string, j *scaleset.JobCompleted) {
	c.cfg.Metrics.JobCompleted(c.repoOf(queue), j)
	if j.RunnerName == "" {
		return
	}
	name := j.RunnerName
	time.AfterFunc(completedGrace, func() {
		p, ok := c.cfg.Pods.Get(name)
		if !ok || !isManaged(p) {
			return
		}
		c.deletePod(context.Background(), p, "completed")
	})
}

func (c *Controller) repoOf(queue string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if qs, ok := c.queues[queue]; ok {
		return qs.q.Repo
	}
	return ""
}

func isManaged(p *corev1.Pod) bool {
	return p.Labels[pods.LabelManagedBy] == pods.ManagedByValue
}

// OnPod is the pod informer callback: finished pods are deleted, deleted
// pods are forgotten, and every change triggers a reconciliation.
func (c *Controller) OnPod(p *corev1.Pod, deleted bool) {
	if !isManaged(p) {
		c.kick() // an ARC pod changed the node usage
		return
	}
	if deleted {
		c.cfg.Mailbox.Forget(p.Name)
		c.warm.Release(p.Name)
		c.mu.Lock()
		delete(c.assigned, p.Name)
		delete(c.sources, p.Name)
		c.mu.Unlock()
		c.kick()
		return
	}
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		go c.deletePod(context.Background(), p, "finished")
	}
}

// OnClaimed records a configuration delivered to an agent.
func (c *Controller) OnClaimed(pod, uid string, _ time.Duration) {
	c.mu.Lock()
	at, ok := c.assigned[pod]
	source := c.sources[pod]
	c.mu.Unlock()
	if p, found := c.cfg.Pods.Get(pod); found && ok {
		c.cfg.Metrics.ObserveClaim(p.Labels[c.cfg.Labels.Profile()], source, c.cfg.Now().Sub(at))
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ann := map[string]string{c.cfg.Labels.ClaimedAnnotation(): c.cfg.Now().UTC().Format(time.RFC3339)}
		if err := c.cfg.Pods.Patch(ctx, pod, types.UID(uid), nil, ann); err != nil && !pods.IsGone(err) {
			c.cfg.Metrics.PodErrors.WithLabelValues("patch_claimed").Inc()
		}
	}()
}

// Run reconciles until ctx is done.
func (c *Controller) Run(ctx context.Context) {
	tick := time.NewTicker(reconcileEvery)
	defer tick.Stop()
	gc := time.NewTicker(gcEvery)
	defer gc.Stop()
	runnerGC := time.NewTicker(runnerGCEvery)
	defer runnerGC.Stop()
	c.GC(ctx, true)
	for {
		c.Reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-c.notify:
		case <-tick.C:
		case <-gc.C:
			c.GC(ctx, false)
		case <-runnerGC.C:
			c.GCRunners(ctx)
		}
	}
}

// queueRunners counts the live runner pods of every scale set.
func (c *Controller) queueRunners(all []*corev1.Pod) map[int][]*corev1.Pod {
	out := map[int][]*corev1.Pod{}
	for _, p := range all {
		if pods.Terminated(p) {
			continue
		}
		id, err := strconv.Atoi(p.Labels[c.cfg.Labels.ScaleSetID()])
		if err != nil {
			continue
		}
		out[id] = append(out[id], p)
	}
	return out
}

// Reconcile computes the missing runners and starts them.
func (c *Controller) Reconcile(ctx context.Context) {
	now := c.cfg.Now()
	managed := c.cfg.Pods.Managed()
	byID := c.queueRunners(managed)
	usage := budget.UsageFromPods(c.cfg.Pods.RunnerPods())

	type surplus struct {
		qs   *queueState
		pods []*corev1.Pod
		n    int
	}
	var demands []budget.Demand
	var scaleDown []surplus

	c.mu.Lock()
	for _, qs := range c.queues {
		if qs.scaleSetID == 0 {
			continue
		}
		active := len(byID[qs.scaleSetID]) + qs.inflight
		switch {
		case qs.desired > active:
			demands = append(demands, budget.Demand{
				Queue: qs.q.Name, Repo: qs.q.Repo, Profile: qs.q.Profile, Priority: qs.q.Priority,
				Nodes: preferred(qs.q.Spec), Missing: qs.desired - active,
			})
		case qs.desired < active && qs.inflight == 0:
			scaleDown = append(scaleDown, surplus{qs: qs, pods: byID[qs.scaleSetID], n: active - qs.desired})
		}
	}
	c.mu.Unlock()

	// Assign already-running warm pods before charging the budget for new
	// pods. They already occupy a place, including when the node is full.
	for i := range demands {
		d := &demands[i]
		c.mu.Lock()
		qs := c.queues[d.Queue]
		q, id, warmOK := qs.q, qs.scaleSetID, qs.warmOK
		c.mu.Unlock()
		if !warmOK {
			continue
		}
		for d.Missing > 0 {
			pod, ok := c.warm.Take(managed, q.Profile, q.Spec.Nodes, now)
			if !ok {
				break
			}
			d.Missing--
			c.mu.Lock()
			qs.inflight++
			c.mu.Unlock()
			c.start(ctx, func() {
				defer func() {
					c.mu.Lock()
					qs.inflight--
					c.mu.Unlock()
					c.kick()
				}()
				if err := c.assignWarm(ctx, q, id, pod); err != nil {
					c.warm.Release(pod.Name)
					c.log.Warn("warm assignment failed", "queue", q.Name, "pod", pod.Name, "error", err)
				}
			})
		}
	}

	capacity := c.cfg.Catalog.Capacity
	res := c.planner.Plan(demands, usage, capacity)
	for _, g := range res.Grants {
		if g.Node != "" {
			usage[g.Node]++
		}
	}

	// Warm pods refill with what is left, after real jobs.
	var warmDemands []budget.Demand
	for _, name := range slices.Sorted(maps.Keys(c.cfg.Catalog.Profiles)) {
		p := c.cfg.Catalog.Profiles[name]
		counts := c.warm.Count(managed, name, now)
		c.cfg.Metrics.WarmPods.WithLabelValues(name, "ready").Set(float64(counts.Ready))
		c.cfg.Metrics.WarmPods.WithLabelValues(name, "starting").Set(float64(counts.Pending))
		c.mu.Lock()
		missing := p.Warm - counts.Total() - c.warmInfl[name]
		c.mu.Unlock()
		if missing > 0 {
			spec := c.cfg.Catalog.ProfileSpec(name)
			warmDemands = append(warmDemands, budget.Demand{
				Queue: "warm:" + name, Repo: "warm:" + name, Profile: name, Nodes: preferred(spec), Missing: missing,
			})
		}
	}
	warmRes := c.planner.Plan(warmDemands, usage, capacity)

	blocked := map[string]int{}
	for k, v := range res.Blocked {
		blocked[k] += v
	}
	for _, name := range slices.Sorted(maps.Keys(c.cfg.Catalog.Profiles)) {
		c.cfg.Metrics.BudgetBlocked.WithLabelValues(name).Set(float64(blocked[name]))
	}
	for node, free := range budget.Free(usage, capacity) {
		c.cfg.Metrics.BudgetFree.WithLabelValues(node).Set(float64(free))
	}

	for _, g := range res.Grants {
		c.mu.Lock()
		qs := c.queues[g.Queue]
		qs.inflight++
		c.mu.Unlock()
		c.start(ctx, func() { c.provision(ctx, qs, g.Node) })
	}
	for _, g := range warmRes.Grants {
		profile := g.Queue[len("warm:"):]
		c.mu.Lock()
		c.warmInfl[profile]++
		c.mu.Unlock()
		c.start(ctx, func() { c.createWarm(ctx, profile, g.Node) })
	}
	for _, s := range scaleDown {
		c.scaleDown(ctx, s.qs, s.pods, s.n, now)
	}
}

// preferred lists the allowed nodes, preferred ones first.
func preferred(s catalog.PodSpec) []string {
	var out []string
	for _, n := range s.PreferNodes {
		if slices.Contains(s.Nodes, n) {
			out = append(out, n)
		}
	}
	for _, n := range s.Nodes {
		if !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

func (c *Controller) start(ctx context.Context, fn func()) {
	go func() {
		select {
		case c.sem <- struct{}{}:
		case <-ctx.Done():
			fn() // releases the in-flight counter without starting anything
			return
		}
		defer func() { <-c.sem }()
		fn()
	}()
}

// provision gives one runner to a queue: a warm pod when one is ready,
// otherwise a cold pod created with its configuration already deposited.
func (c *Controller) provision(ctx context.Context, qs *queueState, node string) {
	defer func() {
		c.mu.Lock()
		qs.inflight--
		c.mu.Unlock()
		c.kick()
	}()
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	q, id, warmOK := qs.q, qs.scaleSetID, qs.warmOK
	c.mu.Unlock()

	if warmOK && c.cfg.Catalog.Profiles[q.Profile].Warm > 0 {
		allowed := q.Spec.Nodes
		if node != "" {
			allowed = []string{node}
		}
		if p, ok := c.warm.Take(c.cfg.Pods.Managed(), q.Profile, allowed, c.cfg.Now()); ok {
			// The reservation is kept on success: the pod cache may still show
			// the pod as warm until the label patch arrives, and another queue
			// must not take it meanwhile. OnPod releases it on deletion.
			err := c.assignWarm(ctx, q, id, p)
			if err == nil {
				c.kick() // refill the reserve
				return
			}
			c.warm.Release(p.Name)
			c.log.Warn("warm assignment failed, creating a cold pod", "queue", q.Name, "pod", p.Name, "error", err)
		}
	}
	if err := c.createCold(ctx, q, id, node); err != nil {
		c.log.Warn("cold pod not created", "queue", q.Name, "error", err)
	}
}

func (c *Controller) runnerName(profile string) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 5)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			panic(err)
		}
		b[i] = alphabet[n.Int64()]
	}
	return runnerNamePrefix + profile + "-" + string(b)
}

func (c *Controller) request(name string, q *catalog.Queue, profile string, spec catalog.PodSpec, node string, id int) pods.Request {
	req := pods.Request{
		Name:       name,
		Namespace:  c.cfg.Catalog.Runtime.Namespace,
		Queue:      q,
		Profile:    profile,
		Spec:       spec,
		ScaleSetID: id,
		ClaimURL:   c.cfg.Catalog.Runtime.ClaimURL,
		AgentImage: c.cfg.AgentImage,
		Labels:     c.cfg.Labels,
	}
	if node != "" {
		req.Nodes = []string{node}
	}
	return req
}

func (c *Controller) createCold(ctx context.Context, q catalog.Queue, id int, node string) error {
	name := c.runnerName(q.Profile)
	pod, err := c.cfg.Renderer.Render(c.request(name, &q, q.Profile, q.Spec, node, id))
	if err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("render").Inc()
		return err
	}
	jit, err := c.cfg.API.GenerateJIT(ctx, q.Repo, id, name)
	if err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("jit").Inc()
		return err
	}
	now := c.cfg.Now()
	pod.Annotations[c.cfg.Labels.RunnerIDAnnotation()] = strconv.FormatInt(jit.RunnerID, 10)
	pod.Annotations[c.cfg.Labels.AssignedAnnotation()] = now.UTC().Format(time.RFC3339)
	c.mu.Lock()
	c.assigned[name] = now
	c.sources[name] = "cold"
	c.mu.Unlock()
	created, err := c.cfg.Pods.Create(ctx, pod)
	if err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("create").Inc()
		c.removeRunner(q.Repo, jit.RunnerID)
		c.mu.Lock()
		delete(c.assigned, name)
		delete(c.sources, name)
		c.mu.Unlock()
		return fmt.Errorf("create pod %s: %w", name, err)
	}
	if err := c.cfg.Mailbox.Deposit(name, string(created.UID), claim.Payload{JITConfig: jit.Config, Repo: q.Repo}); err != nil {
		return err
	}
	c.cfg.Metrics.PodsCreated.WithLabelValues(q.Profile, "cold").Inc()
	c.log.Info("cold runner created", "queue", q.Name, "pod", name, "node", node)
	return nil
}

func (c *Controller) createWarm(ctx context.Context, profile, node string) {
	defer func() {
		c.mu.Lock()
		c.warmInfl[profile]--
		c.mu.Unlock()
	}()
	if ctx.Err() != nil {
		return
	}
	spec := c.cfg.Catalog.ProfileSpec(profile)
	name := c.runnerName(profile)
	pod, err := c.cfg.Renderer.Render(c.request(name, nil, profile, spec, node, 0))
	if err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("render").Inc()
		c.log.Warn("warm pod not rendered", "profile", profile, "error", err)
		return
	}
	if _, err := c.cfg.Pods.Create(ctx, pod); err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("create").Inc()
		c.log.Warn("warm pod not created", "profile", profile, "error", err)
		return
	}
	c.cfg.Metrics.PodsCreated.WithLabelValues(profile, "warm").Inc()
	c.log.Info("warm pod created", "profile", profile, "pod", name)
}

// assignWarm registers a runner named after the warm pod, labels the pod
// with its repository and deposits the configuration.
func (c *Controller) assignWarm(ctx context.Context, q catalog.Queue, id int, p *corev1.Pod) error {
	jit, err := c.cfg.API.GenerateJIT(ctx, q.Repo, id, p.Name)
	if err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("jit").Inc()
		return err
	}
	now := c.cfg.Now()
	ann := map[string]string{
		c.cfg.Labels.QueueAnnotation():    q.Name,
		c.cfg.Labels.RunnerIDAnnotation(): strconv.FormatInt(jit.RunnerID, 10),
		c.cfg.Labels.AssignedAnnotation(): now.UTC().Format(time.RFC3339),
	}
	if err := c.cfg.Pods.Patch(ctx, p.Name, p.UID, pods.AssignedLabels(c.cfg.Labels, q.Repo, id), ann); err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("patch_assign").Inc()
		c.removeRunner(q.Repo, jit.RunnerID)
		return err
	}
	c.mu.Lock()
	c.assigned[p.Name] = now
	c.sources[p.Name] = "warm"
	c.mu.Unlock()
	payload := claim.Payload{JITConfig: jit.Config, Repo: q.Repo}
	for _, nc := range q.Spec.Caches {
		if nc.Type != catalog.CacheLanguage {
			continue
		}
		base := warmCacheRoot + "/language-caches/" + q.Repo
		payload.Preflight = &claim.Preflight{Root: warmCacheRoot, ServerNode: nc.ServerNode, Marker: nc.Marker}
		payload.StateDir = base + "/pods/" + string(p.UID)
		if q.Spec.CacheProfile != catalog.CacheProfileLocal {
			payload.Env = map[string]string{
				"GOCACHE":      base + "/go-build",
				"GOMODCACHE":   base + "/go-mod",
				"UV_CACHE_DIR": base + "/uv",
			}
		}
		break
	}
	if err := c.cfg.Mailbox.Deposit(p.Name, string(p.UID), payload); err != nil {
		return err
	}
	c.log.Info("warm runner assigned", "queue", q.Name, "pod", p.Name, "node", p.Spec.NodeName)
	return nil
}

// scaleDown removes idle surplus runners: registered, not busy and assigned
// for a while (a runner just assigned may be about to receive its job).
func (c *Controller) scaleDown(ctx context.Context, qs *queueState, ps []*corev1.Pod, n int, now time.Time) {
	for _, p := range ps {
		if n == 0 {
			return
		}
		if p.Labels[c.cfg.Labels.State()] != pods.StateAssigned {
			continue
		}
		at, err := time.Parse(time.RFC3339, p.Annotations[c.cfg.Labels.AssignedAnnotation()])
		if err != nil || now.Sub(at) < scaleDownMinAge {
			continue
		}
		if id, err := strconv.ParseInt(p.Annotations[c.cfg.Labels.RunnerIDAnnotation()], 10, 64); err == nil {
			rmCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := c.cfg.API.RemoveRunner(rmCtx, qs.q.Repo, id)
			cancel()
			if gh.IsJobStillRunning(err) {
				continue
			}
			if err != nil {
				c.cfg.Metrics.PodErrors.WithLabelValues("remove_runner").Inc()
				continue
			}
		}
		c.deletePod(ctx, p, "scale_down")
		n--
	}
}

func (c *Controller) removeRunner(repo string, id int64) {
	if id == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.cfg.API.RemoveRunner(ctx, repo, id); err != nil && !gh.IsJobStillRunning(err) {
		c.cfg.Metrics.PodErrors.WithLabelValues("remove_runner").Inc()
		c.log.Warn("runner not removed", "repo", repo, "runnerID", id, "error", err)
	}
}

// deletePod deletes a pod; a pod that never received its configuration also
// gets its GitHub runner registration removed.
func (c *Controller) deletePod(ctx context.Context, p *corev1.Pod, reason string) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, claimed := p.Annotations[c.cfg.Labels.ClaimedAnnotation()]; !claimed {
		if id, err := strconv.ParseInt(p.Annotations[c.cfg.Labels.RunnerIDAnnotation()], 10, 64); err == nil {
			c.removeRunner(p.Labels[c.cfg.Labels.Repo()], id)
		}
	}
	if err := c.cfg.Pods.Delete(ctx, p.Name, p.UID); err != nil {
		c.cfg.Metrics.PodErrors.WithLabelValues("delete").Inc()
		c.log.Warn("pod not deleted", "pod", p.Name, "reason", reason, "error", err)
		return
	}
	c.log.Info("pod deleted", "pod", p.Name, "reason", reason)
}

// GC deletes the managed pods nobody will use: pods of an unknown queue,
// assigned pods whose configuration was lost or never claimed, warm pods that
// never became ready.
func (c *Controller) GC(ctx context.Context, startup bool) {
	now := c.cfg.Now()
	c.mu.Lock()
	known := map[int]bool{}
	for id := range c.byID {
		known[id] = true
	}
	sessionsReady := len(c.byID) == countResolved(c.queues)
	c.mu.Unlock()

	for _, p := range c.cfg.Pods.Managed() {
		if pods.Terminated(p) {
			continue
		}
		state := p.Labels[c.cfg.Labels.State()]
		if state == pods.StateWarm {
			continue
		}
		id, err := strconv.Atoi(p.Labels[c.cfg.Labels.ScaleSetID()])
		if err == nil && sessionsReady && !known[id] {
			c.gcDelete(ctx, p, "unknown_queue")
			continue
		}
		if state != pods.StateAssigned {
			continue
		}
		if _, claimed := p.Annotations[c.cfg.Labels.ClaimedAnnotation()]; claimed {
			continue
		}
		at, err := time.Parse(time.RFC3339, p.Annotations[c.cfg.Labels.AssignedAnnotation()])
		if err != nil {
			at = p.CreationTimestamp.Time
		}
		age := now.Sub(at)
		switch {
		case age > unclaimedMaxAge:
			c.gcDelete(ctx, p, "unclaimed")
		case age > lostMailboxMinAge && !c.cfg.Mailbox.Pending(p.Name):
			// The configuration is not in memory (dispatcher restarted):
			// the agent will never get it.
			c.gcDelete(ctx, p, "lost_config")
		}
	}
	for _, p := range c.warm.Stale(c.cfg.Pods.Managed(), warmNotReadyMaxAge, now) {
		c.gcDelete(ctx, p, "warm_not_ready")
	}
	if startup {
		c.GCRunners(ctx)
	}
}

func countResolved(qs map[string]*queueState) int {
	n := 0
	for _, q := range qs {
		if q.scaleSetID != 0 {
			n++
		}
	}
	return n
}

func (c *Controller) gcDelete(ctx context.Context, p *corev1.Pod, reason string) {
	c.cfg.Metrics.GCRemoved.WithLabelValues("pod_" + reason).Inc()
	c.deletePod(ctx, p, reason)
}

// GCRunners removes the offline GitHub runners named ci-* that have no pod
// (pod deleted before its runner could unregister).
func (c *Controller) GCRunners(ctx context.Context) {
	podNames := map[string]bool{}
	for _, p := range c.cfg.Pods.Managed() {
		podNames[p.Name] = true
	}
	repos := map[string]bool{}
	c.mu.Lock()
	// A cold runner is registered just before its pod is created.
	for name := range c.assigned {
		podNames[name] = true
	}
	for _, qs := range c.queues {
		repos[qs.q.Repo] = true
	}
	c.mu.Unlock()
	for _, repo := range slices.Sorted(maps.Keys(repos)) {
		if ctx.Err() != nil {
			return
		}
		runners, err := c.cfg.API.ListRunners(ctx, repo)
		if err != nil {
			c.log.Warn("runners not listed", "repo", repo, "error", err)
			continue
		}
		for _, r := range runners {
			if !isDispatcherRunner(r.Name) || r.Status != "offline" || r.Busy || podNames[r.Name] {
				continue
			}
			if err := c.cfg.API.RemoveRunner(ctx, repo, r.ID); err == nil {
				c.cfg.Metrics.GCRemoved.WithLabelValues("runner").Inc()
				c.log.Info("orphan runner removed", "repo", repo, "runner", r.Name)
			}
		}
	}
}

func isDispatcherRunner(name string) bool {
	return len(name) > len(runnerNamePrefix) && name[:len(runnerNamePrefix)] == runnerNamePrefix
}

var _ sessions.Handler = (*Controller)(nil)
