package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/actions/scaleset"

	"github.com/actions/scaleset/dispatcher/catalog"
	"github.com/actions/scaleset/dispatcher/gh"
)

// visibilityAPI fails a repository's first lookups as listed in failures.
type visibilityAPI struct {
	gh.API
	failures map[string]int
	err      map[string]error
	calls    map[string]int
}

func (a *visibilityAPI) IsPublic(_ context.Context, repo string) (bool, error) {
	a.calls[repo]++
	if a.calls[repo] <= a.failures[repo] {
		return false, a.err[repo]
	}
	return false, nil
}

func testCatalog() *catalog.Catalog {
	return &catalog.Catalog{
		Profiles: map[string]*catalog.Profile{"light": {Nodes: []string{"node-a"}}},
		Defaults: catalog.Defaults{Profiles: []string{"light"}, MaxRunners: 1},
		Repos:    map[string]*catalog.Repo{"steady": {}, "flaky": {}, "broken": {}, "gone": {}},
	}
}

func TestResolveSurvivesVisibilityErrors(t *testing.T) {
	saved := visibilityDelays
	visibilityDelays = []time.Duration{time.Millisecond, time.Millisecond}
	defer func() { visibilityDelays = saved }()

	bad := errors.New("HTTP 502")
	api := &visibilityAPI{
		failures: map[string]int{"flaky": 2, "broken": 100, "gone": 100},
		err:      map[string]error{"flaky": bad, "broken": bad, "gone": fmt.Errorf("get repo: %w", scaleset.NotFoundError)},
		calls:    map[string]int{},
	}
	queues, excluded, err := resolve(context.Background(), testCatalog(), api, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("one repository's error stopped the dispatcher: %v", err)
	}
	served := map[string]bool{}
	for _, q := range queues {
		served[q.Repo] = true
	}
	if !served["steady"] || !served["flaky"] || served["broken"] || served["gone"] {
		t.Fatalf("served %v, want steady and flaky only", served)
	}
	if api.calls["flaky"] != 3 || api.calls["broken"] != 3 {
		t.Fatalf("lookups flaky=%d broken=%d, want 3 each (1 + 2 retries)", api.calls["flaky"], api.calls["broken"])
	}
	if !slices.Equal(excluded, []string{"broken", "gone"}) {
		t.Fatalf("excluded %v", excluded)
	}
	if api.calls["gone"] != 1 {
		t.Fatalf("a missing repository was looked up %d times, want 1", api.calls["gone"])
	}
}

func TestResolveFailsWhenNoVisibilityIsKnown(t *testing.T) {
	saved := visibilityDelays
	visibilityDelays = nil
	defer func() { visibilityDelays = saved }()
	bad := errors.New("HTTP 502")
	api := &visibilityAPI{failures: map[string]int{"steady": 1, "flaky": 1, "broken": 1, "gone": 1}, err: map[string]error{"steady": bad, "flaky": bad, "broken": bad, "gone": bad}, calls: map[string]int{}}
	if _, _, err := resolve(context.Background(), testCatalog(), api, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("a dispatcher with no known repository must not keep the Lease")
	}
}

func TestRecheckExcludedRestartsOnceARepoIsBack(t *testing.T) {
	saved := visibilityRecheckEvery
	visibilityRecheckEvery = time.Millisecond
	defer func() { visibilityRecheckEvery = saved }()
	api := &visibilityAPI{failures: map[string]int{"broken": 2}, err: map[string]error{"broken": errors.New("HTTP 502")}, calls: map[string]int{}}
	if err := recheckExcluded(context.Background(), api, []string{"broken"}); err == nil {
		t.Fatal("no restart once the repository answers")
	}
	if api.calls["broken"] != 3 {
		t.Fatalf("lookups %d, want 3", api.calls["broken"])
	}
}
