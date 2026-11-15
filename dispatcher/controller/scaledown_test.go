package controller

import (
	"context"
	"testing"
	"time"

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
