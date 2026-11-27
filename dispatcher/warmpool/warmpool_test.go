package warmpool

import (
	"testing"
	"time"

	"github.com/actions/scaleset/dispatcher/pods"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Un pod chaud ne sert une file que s'il porte la confiance actuelle de son
// profil : un pod créé avant que le profil passe en public est ignoré.
func TestTakeExigeLaConfianceDuProfil(t *testing.T) {
	l := pods.Labels{Prefix: "ci.example.com"}
	now := time.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-light-1", Labels: map[string]string{
			l.State(): pods.StateWarm, l.Profile(): "light", l.Trust(): "private"}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	p := New(l, func(string) (time.Time, bool) { return now, true })
	if _, ok := p.Take([]*corev1.Pod{pod}, "light", "public", nil, now); ok {
		t.Fatal("un pod chaud privé a servi un profil devenu public")
	}
	if got, ok := p.Take([]*corev1.Pod{pod}, "light", "private", nil, now); !ok || got.Name != pod.Name {
		t.Fatal("le pod chaud de même confiance n'a pas été pris")
	}
}
