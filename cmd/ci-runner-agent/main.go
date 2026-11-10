// Command ci-runner-agent runs as the main process of every runner pod
// created by ci-dispatcher.
//
//	ci-runner-agent install <path>   copy itself (init container)
//	ci-runner-agent run              wait for a JIT configuration, then run the runner
//
// run reports the pod ready, long-polls the dispatcher for its JIT runner
// configuration (authenticated by the projected service account token),
// prepares the language cache when asked to, then starts the runner with
// ACTIONS_RUNNER_INPUT_JITCONFIG. It forwards SIGTERM and SIGINT to the
// runner and empties the per-pod state directory when the runner exits.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	claim "github.com/actions/scaleset/dispatcher/claim/protocol"
	"github.com/actions/scaleset/dispatcher/preflight"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("component", "ci-runner-agent")
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ci-runner-agent install <path> | run")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "install":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: ci-runner-agent install <path>")
			os.Exit(2)
		}
		if err := install(os.Args[2]); err != nil {
			log.Error("install failed", "error", err)
			os.Exit(1)
		}
	case "run":
		os.Exit(run(log))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// install copies the running executable to dst, executable by every user
// (the runner image uses its own user).
func install(dst string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	in, err := os.Open(self)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

type agent struct {
	claimURL  string
	tokenFile string
	http      *http.Client
	log       *slog.Logger
}

var errFatal = errors.New("fatal")

func (a *agent) do(ctx context.Context, method, path string) (*http.Response, error) {
	token, err := os.ReadFile(a.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(a.claimURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	return a.http.Do(req)
}

// claim returns the payload, nil when the poll timed out.
func (a *agent) claim(ctx context.Context) (*claim.Payload, error) {
	if resp, err := a.do(ctx, http.MethodPost, "/v1/ready"); err == nil {
		resp.Body.Close()
	}
	resp, err := a.do(ctx, http.MethodGet, "/v1/claim?wait=50s")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var p claim.Payload
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&p); err != nil {
			return nil, fmt.Errorf("decode configuration: %w", err)
		}
		if p.JITConfig == "" {
			return nil, fmt.Errorf("%w: empty configuration", errFatal)
		}
		return &p, nil
	case http.StatusNoContent:
		return nil, nil
	case http.StatusGone:
		return nil, fmt.Errorf("%w: configuration already delivered", errFatal)
	default:
		return nil, fmt.Errorf("claim: HTTP %d", resp.StatusCode)
	}
}

func run(log *slog.Logger) int {
	a := &agent{
		claimURL:  os.Getenv("CI_DISPATCHER_CLAIM_URL"),
		tokenFile: os.Getenv("CI_DISPATCHER_TOKEN_FILE"),
		http:      &http.Client{Timeout: 90 * time.Second},
		log:       log,
	}
	if a.claimURL == "" || a.tokenFile == "" {
		log.Error("CI_DISPATCHER_CLAIM_URL and CI_DISPATCHER_TOKEN_FILE are required")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var payload *claim.Payload
	delay := time.Second
	for payload == nil {
		p, err := a.claim(ctx)
		switch {
		case ctx.Err() != nil:
			log.Info("stopped before receiving a configuration")
			return 0
		case errors.Is(err, errFatal):
			log.Error("cannot receive a configuration", "error", err)
			return 1
		case err != nil:
			// The dispatcher may not see this pod yet, or be restarting.
			log.Warn("claim failed, retrying", "error", err, "in", delay)
			select {
			case <-ctx.Done():
				return 0
			case <-time.After(delay):
			}
			delay = min(delay*2, 10*time.Second)
		default:
			delay = time.Second
			payload = p
		}
	}
	stop()
	log.Info("configuration received", "repo", payload.Repo)

	stateDir := os.Getenv("CI_DISPATCHER_STATE_DIR")
	if payload.StateDir != "" {
		stateDir = payload.StateDir
	}
	if payload.Preflight != nil {
		if err := runPreflight(payload); err != nil {
			log.Error("language cache not ready", "error", err)
			return 1
		}
	}
	return runRunner(log, payload, stateDir)
}

func runPreflight(p *claim.Payload) error {
	cmd := exec.Command("/bin/bash", "-c", preflight.Script)
	cmd.Env = append(os.Environ(),
		"CACHE_ROOT="+p.Preflight.Root,
		"CACHE_SCOPE="+p.Repo,
		"CACHE_SERVER_NODE="+p.Preflight.ServerNode,
		"CACHE_MARKER="+p.Preflight.Marker,
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd.Run()
}

// runnerEnv is the environment of the runner: the agent's own, without the
// dispatcher variables, plus the configuration.
func runnerEnv(p *claim.Payload) []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "CI_DISPATCHER_") || strings.HasPrefix(kv, "ACTIONS_RUNNER_INPUT_JITCONFIG=") {
			continue
		}
		env = append(env, kv)
	}
	for k, v := range p.Env {
		env = append(env, k+"="+v)
	}
	// Same rule as the cache preflight: the backend follows the node.
	if server := os.Getenv("CACHE_SERVER_NODE"); server != "" {
		backend := "nfs"
		if os.Getenv("RUNNER_NODE_NAME") == server {
			backend = "local"
		}
		env = append(env, "RUNNER_CACHE_STORAGE="+backend)
	}
	return append(env, "ACTIONS_RUNNER_INPUT_JITCONFIG="+p.JITConfig)
}

func runRunner(log *slog.Logger, p *claim.Payload, stateDir string) int {
	command := os.Getenv("RUNNER_CMD")
	if command == "" {
		command = "/home/runner/run.sh"
	}
	cmd := exec.Command(command)
	cmd.Env = runnerEnv(p)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	defer cleanState(log, stateDir)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		log.Error("runner not started", "command", command, "error", err)
		return 1
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	for {
		select {
		case s := <-sigs:
			_ = cmd.Process.Signal(s)
		case err := <-done:
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
					return 128 + int(ws.Signal())
				}
				return exit.ExitCode()
			}
			if err != nil {
				log.Error("runner failed", "error", err)
				return 1
			}
			return 0
		}
	}
}

// cleanState empties the per-pod state directory, within 30 seconds.
func cleanState(log *slog.Logger, dir string) {
	if dir == "" {
		return
	}
	done := make(chan error, 1)
	go func() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
			done <- err
			return
		}
		var errs []error
		for _, e := range entries {
			errs = append(errs, os.RemoveAll(filepath.Join(dir, e.Name())))
		}
		done <- errors.Join(errs...)
	}()
	select {
	case err := <-done:
		if err != nil {
			log.Warn("runner workspace cleanup incomplete", "error", err)
		}
	case <-time.After(30 * time.Second):
		log.Warn("runner workspace cleanup incomplete", "error", "timeout")
	}
}
