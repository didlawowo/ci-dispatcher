package controller

import (
	"context"
	"testing"
	"time"

	"github.com/actions/scaleset/dispatcher/catalog"
	"github.com/actions/scaleset/dispatcher/pods"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type samePodsFake struct {
	scaleDownPods
	pods []*corev1.Pod
}

func (p *samePodsFake) Managed() []*corev1.Pod         { return p.pods }
func (p *samePodsFake) RunnerPods() []*corev1.Pod      { return p.pods }
func (p *samePodsFake) Get(string) (*corev1.Pod, bool) { return nil, false }

func TestReconcileDoesNotScaleDownRunnersOfAnotherRepositoryWithTheSameScaleSetID(t *testing.T) {
	api := &scaleDownAPI{}
	podAPI := &samePodsFake{}
	cfg, q := newColdTestConfig(t, api, podAPI, &coldTestMailbox{}, "registry/dispatcher@sha256:abc")
	qk := q
	qk.Name, qk.Repo = "qk", "keryx"
	qd := q
	qd.Name, qd.Repo = "qd", "demo"
	cfg.Queues = []catalog.Queue{qk, qd}
	now := cfg.Now()
	podAPI.pods = []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "ci-keryx-idle",
		Labels:      map[string]string{cfg.Labels.State(): pods.StateAssigned, cfg.Labels.Repo(): "keryx", cfg.Labels.ScaleSetID(): "9"},
		Annotations: map[string]string{cfg.Labels.AssignedAnnotation(): now.Add(-time.Hour).Format(time.RFC3339), cfg.Labels.RunnerIDAnnotation(): "7", cfg.Labels.ClaimedAnnotation(): "x"}}}}
	c := New(cfg)
	c.OnScaleSet("qk", 9)
	c.OnScaleSet("qd", 9)
	c.mu.Lock()
	c.queues["qk"].desired = 1
	c.queues["qd"].desired = 0
	c.mu.Unlock()
	c.Reconcile(context.Background())
	if len(podAPI.deleted) != 0 || len(api.removeCalls) != 0 {
		t.Fatalf("pod de keryx supprimé par la file de demo : deleted=%v remove=%v", podAPI.deleted, api.removeCalls)
	}
}

func TestGCDeletesAPodWhoseScaleSetIDOnlyCollidesWithAnotherRepository(t *testing.T) {
	podAPI := &samePodsFake{}
	cfg, q := newColdTestConfig(t, &scaleDownAPI{}, podAPI, &coldTestMailbox{}, "registry/dispatcher@sha256:abc")
	qa := q
	qa.Name, qa.Repo = "a", "r1"
	qb := q
	qb.Name, qb.Repo = "b", "r2"
	cfg.Queues = []catalog.Queue{qa, qb}
	podAPI.pods = []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "orphan",
		Labels:      map[string]string{cfg.Labels.State(): pods.StateBusy, cfg.Labels.Repo(): "r2", cfg.Labels.ScaleSetID(): "7"},
		Annotations: map[string]string{cfg.Labels.ClaimedAnnotation(): "x"}}}}
	c := New(cfg)
	c.OnScaleSet("a", 7)
	c.OnScaleSet("b", 3)
	c.GC(context.Background(), false)
	t.Logf("deleted=%v", podAPI.deleted)
}
