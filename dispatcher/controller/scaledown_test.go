package controller

import (
	"context"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/dispatcher/gh"
	"github.com/actions/scaleset/dispatcher/pods"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type scaleDownAPI struct {
	coldTestAPI
	runners []gh.Runner
}

func (a *scaleDownAPI) ListRunners(context.Context, string) ([]gh.Runner, error) {
	return a.runners, nil
}

type scaleDownPods struct {
	PodAPI
	deleted []string
}

func (p *scaleDownPods) Delete(_ context.Context, name string, _ types.UID) error {
	p.deleted = append(p.deleted, name)
	return nil
}

// Un runner chaud qui vient de prendre un job porte encore l'état assigned :
// la réduction ne doit pas le supprimer quand GitHub le dit occupé.
func TestScaleDownSparesARunnerGitHubReportsBusy(t *testing.T) {
	podAPI := &scaleDownPods{}
	for _, busy := range []bool{true, false} {
		api := &scaleDownAPI{runners: []gh.Runner{{ID: 7, Name: "ci-docker-a", Busy: busy}}}
		cfg, q := newColdTestConfig(t, api, podAPI, &coldTestMailbox{}, "registry/dispatcher@sha256:abc")
		c := New(cfg)
		now := time.Now()
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ci-docker-a",
			Labels:      map[string]string{cfg.Labels.State(): pods.StateAssigned},
			Annotations: map[string]string{cfg.Labels.AssignedAnnotation(): now.Add(-time.Hour).Format(time.RFC3339), cfg.Labels.RunnerIDAnnotation(): "7", cfg.Labels.ClaimedAnnotation(): "x"}}}
		c.scaleDown(context.Background(), &queueState{q: q}, []*corev1.Pod{p}, 1, now)
		if want := map[bool]int{true: 0, false: 1}[busy]; len(podAPI.deleted) != want || len(api.removeCalls) != want {
			t.Fatalf("busy=%v : %d pod(s) supprimé(s), %d RemoveRunner, attendu %d", busy, len(podAPI.deleted), len(api.removeCalls), want)
		}
		podAPI.deleted = nil
	}
}

// Un pod dont le job est terminé ne compte plus comme runner actif : sinon la
// fin d'un job crée un faux surplus qui vise le runner libre.
func TestCompletedJobPodIsNotCountedAsActive(t *testing.T) {
	cfg, _ := newColdTestConfig(t, &scaleDownAPI{}, &scaleDownPods{}, &coldTestMailbox{}, "registry/dispatcher@sha256:abc")
	c := New(cfg)
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{cfg.Labels.ScaleSetID(): "4"}}}
	}
	c.OnJobCompleted("q", &scaleset.JobCompleted{RunnerName: "ci-docker-done"})
	if got := len(c.queueRunners([]*corev1.Pod{pod("ci-docker-done"), pod("ci-docker-idle")})[4]); got != 1 {
		t.Fatalf("%d runner(s) actif(s), attendu 1 (le pod au job terminé ne compte plus)", got)
	}
}

// Comme ARC : minRunners runners libres en plus des jobs attribués.
func TestDesiredKeepsMinRunnersIdleOnTopOfAssignedJobs(t *testing.T) {
	cfg, q := newColdTestConfig(t, &scaleDownAPI{}, &scaleDownPods{}, &coldTestMailbox{}, "registry/dispatcher@sha256:abc")
	c := New(cfg)
	c.queues[q.Name].q.MinRunners, c.queues[q.Name].q.MaxRunners = 2, 7
	for jobs, want := range map[int]int{0: 2, 1: 3, 6: 7} {
		c.OnStatistics(q.Name, &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: jobs})
		if got := c.queues[q.Name].desired; got != want {
			t.Errorf("%d job(s) : desired = %d, attendu %d", jobs, got, want)
		}
	}
}
