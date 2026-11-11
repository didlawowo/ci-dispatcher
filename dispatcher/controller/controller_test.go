package controller

import (
	"context"
	"testing"
	"time"

	"github.com/actions/scaleset/dispatcher/catalog"
	"github.com/actions/scaleset/dispatcher/claim"
	"github.com/actions/scaleset/dispatcher/gh"
	"github.com/actions/scaleset/dispatcher/metrics"
	"github.com/actions/scaleset/dispatcher/pods"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type testPods struct {
	PodAPI
	pod     *corev1.Pod
	patched chan string
}

func (p *testPods) Managed() []*corev1.Pod    { return []*corev1.Pod{p.pod} }
func (p *testPods) RunnerPods() []*corev1.Pod { return p.Managed() }
func (p *testPods) Patch(_ context.Context, name string, _ types.UID, _ map[string]string, _ map[string]string) error {
	p.patched <- name
	return nil
}

type testAPI struct{ gh.API }

func (testAPI) GenerateJIT(context.Context, string, int, string) (gh.JIT, error) {
	return gh.JIT{RunnerID: 1, Config: "config"}, nil
}

type testMailbox struct {
	Mailbox
	ready     time.Time
	delivered chan string
}

func (m *testMailbox) Ready(string) (time.Time, bool)                { return m.ready, true }
func (m *testMailbox) Deposit(name, _ string, _ claim.Payload) error { m.delivered <- name; return nil }

func TestWarmRunnerAssignedAtFullCapacity(t *testing.T) {
	labels := pods.Labels{Prefix: "example.com/ci"}
	now := time.Now()
	p := &testPods{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "warm-one", UID: "uid", Labels: map[string]string{labels.Profile(): "light", labels.State(): pods.StateWarm}}, Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, patched: make(chan string, 1)}
	mailbox := &testMailbox{ready: now, delivered: make(chan string, 1)}
	cat := &catalog.Catalog{Profiles: map[string]*catalog.Profile{"light": {Warm: 1, Nodes: []string{"node-a"}}}, Capacity: catalog.Capacity{Enforce: true, Nodes: map[string]catalog.NodeCapacity{"node-a": {Places: 1}}}}
	q := catalog.Queue{Name: "queue", Repo: "repo", Profile: "light", MaxRunners: 1, Spec: cat.ProfileSpec("light")}
	c := New(Config{Catalog: cat, Queues: []catalog.Queue{q}, API: testAPI{}, Pods: p, Mailbox: mailbox, Metrics: metrics.New(), Labels: labels, Now: func() time.Time { return now }})
	c.OnScaleSet(q.Name, 1)
	c.mu.Lock()
	c.queues[q.Name].desired = 1
	c.mu.Unlock()
	c.Reconcile(context.Background())
	select {
	case name := <-mailbox.delivered:
		if name != p.pod.Name {
			t.Fatalf("assigned %q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("warm pod was not assigned at full capacity")
	}
}
