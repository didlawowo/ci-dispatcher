//go:build linux

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	claim "github.com/actions/scaleset/dispatcher/claim/protocol"
)

// setSubreaper makes the test process a subreaper, like PID 1 of the container.
func setSubreaper(t *testing.T) {
	t.Helper()
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, 36, 1, 0); errno != 0 {
		t.Skipf("PR_SET_CHILD_SUBREAPER unavailable: %v", syscall.Errno(errno))
	}
}

func TestRunRunnerReapsOrphanedProcesses(t *testing.T) {
	setSubreaper(t)

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "runner.sh")
	script := fmt.Sprintf(`#!/bin/sh
dir=%q
( sleep 30 & echo $! > "$dir/pid" )
kill -9 "$(cat "$dir/pid")"
sleep 0.5
if kill -0 "$(cat "$dir/pid")" 2>/dev/null; then
	exit 3
fi
exit 0
`, dir)

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	if err := os.Chmod(scriptPath, 0o755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	t.Setenv("RUNNER_CMD", scriptPath)
	got := runRunner(slog.New(slog.NewTextHandler(io.Discard, nil)), &claim.Payload{JITConfig: "x"}, "")
	if got != 0 {
		t.Errorf("runRunner returned %d, want 0: the orphan stayed a zombie", got)
	}
}

func TestRunRunnerKeepsTheRunnerExitCode(t *testing.T) {
	setSubreaper(t)

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "runner.sh")
	script := "#!/bin/sh\nexit 7\n"

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	if err := os.Chmod(scriptPath, 0o755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	t.Setenv("RUNNER_CMD", scriptPath)
	got := runRunner(slog.New(slog.NewTextHandler(io.Discard, nil)), &claim.Payload{JITConfig: "x"}, "")
	if got != 7 {
		t.Errorf("runRunner returned %d, want 7", got)
	}
}
