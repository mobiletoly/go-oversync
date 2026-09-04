package oversync

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func timeoutRunnerPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "scripts", "run-with-timeout.pl"))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunWithTimeout_ArgvPreservationAndOrdinaryExit(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "argv")
	command := exec.Command(timeoutRunnerPath(t), "5", "--", "/usr/bin/perl", "-e", `open my $f, ">", $ARGV[0] or die $!; print {$f} join("\n", @ARGV[1..$#ARGV]); exit 23`, marker, "a b", "*", "$HOME", "quote'word")
	command.Env = append(os.Environ(), "OVERSYNC_TIMEOUT_GRACE_SECONDS=1")
	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("exit=%v want 23", err)
	}
	content, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "a b\n*\n$HOME\nquote'word"; got != want {
		t.Fatalf("argv changed: got %q want %q", got, want)
	}
}

func TestRunWithTimeout_PropagatesHUPINTAndTERM(t *testing.T) {
	t.Parallel()
	for _, signal := range []syscall.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			ready := filepath.Join(t.TempDir(), "ready")
			program := `$SIG{HUP}=sub{exit 31}; $SIG{INT}=sub{exit 32}; $SIG{TERM}=sub{exit 33}; open my $f, ">", $ARGV[0] or die $!; close $f; sleep 30`
			command := exec.Command(timeoutRunnerPath(t), "20", "--", "/usr/bin/perl", "-e", program, ready)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForFile(t, ready)
			if err := command.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			err := command.Wait()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 128+int(signal) {
				t.Fatalf("signal %v exit=%v want %d", signal, err, 128+int(signal))
			}
		})
	}
}

func TestRunWithTimeout_HungCompileAndVetSurrogatesExit124(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"pre-test compile", "vet"} {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			command := exec.Command(timeoutRunnerPath(t), "1", "--", "/usr/bin/perl", "-e", `$SIG{TERM}="IGNORE"; sleep 30`)
			command.Env = append(os.Environ(), "OVERSYNC_TIMEOUT_GRACE_SECONDS=0")
			err := command.Run()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 124 {
				t.Fatalf("exit=%v want 124", err)
			}
			if elapsed := time.Since(started); elapsed > 3*time.Second {
				t.Fatalf("hard timeout took %s", elapsed)
			}
		})
	}
}

func TestRunWithTimeout_TERMCleanupForcedKILLAndDescendantTermination(t *testing.T) {
	t.Parallel()
	temp := t.TempDir()
	termMarker := filepath.Join(temp, "term")
	pidFile := filepath.Join(temp, "pid")
	program := `$SIG{TERM}=sub{open my $f, ">", $ARGV[0]; print {$f} "term"; close $f}; my $kid=fork(); die $! unless defined $kid; if(!$kid){$SIG{TERM}="IGNORE"; sleep 30; exit} open my $p, ">", $ARGV[1] or die $!; print {$p} $kid; close $p; sleep 30`
	command := exec.Command(timeoutRunnerPath(t), "2", "--", "/usr/bin/perl", "-e", program, termMarker, pidFile)
	command.Env = append(os.Environ(), "OVERSYNC_TIMEOUT_GRACE_SECONDS=1")
	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 124 {
		t.Fatalf("exit=%v want 124", err)
	}
	if _, err := os.Stat(termMarker); err != nil {
		t.Fatalf("TERM cleanup did not run: %v", err)
	}
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidBytes))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for syscall.Kill(pid, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("descendant %d survived forced group kill", pid)
	}
}

func TestRunWithTimeout_LeaderExitAllowsDescendantTERMCleanupWithinOriginalDeadline(t *testing.T) {
	t.Parallel()
	temp := t.TempDir()
	ready := filepath.Join(temp, "ready")
	cleaned := filepath.Join(temp, "cleaned")
	program := `my $kid=fork(); die $! unless defined $kid; if(!$kid){$SIG{TERM}=sub{select undef,undef,undef,0.3; open my $f, ">", $ARGV[1] or die $!; close $f; exit 0}; open my $r, ">", $ARGV[0] or die $!; close $r; sleep 30; exit 0} while(!-e $ARGV[0]){select undef,undef,undef,0.01} exit 0`
	started := time.Now()
	command := exec.Command(timeoutRunnerPath(t), "3", "--", "/usr/bin/perl", "-e", program, ready, cleaned)
	command.Env = append(os.Environ(), "OVERSYNC_TIMEOUT_GRACE_SECONDS=1")
	if err := command.Run(); err != nil {
		t.Fatalf("runner returned leader failure: %v", err)
	}
	if _, err := os.Stat(cleaned); err != nil {
		t.Fatalf("descendant TERM cleanup did not finish: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 250*time.Millisecond || elapsed >= 3*time.Second {
		t.Fatalf("cleanup elapsed %s, want at least 250ms and below hard deadline", elapsed)
	}
}

func TestRunWithTimeout_InvokesLabeledContainerCleanupTrap(t *testing.T) {
	t.Parallel()
	temp := t.TempDir()
	logPath := filepath.Join(temp, "docker.log")
	writeExecutable(t, filepath.Join(temp, "go"), "#!/bin/sh\ntrap 'exit 143' HUP INT TERM\nwhile :; do sleep 1; done\n")
	writeExecutable(t, filepath.Join(temp, "docker"), fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nif [ \"${1-}\" = ps ]; then printf 'container-for-test\\n'; fi\n", logPath))
	cleanupRunner, err := filepath.Abs(filepath.Join("..", "scripts", "go-test-with-cleanup.sh"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(timeoutRunnerPath(t), "2", "--", cleanupRunner, "./oversync")
	command.Env = append(os.Environ(), "PATH="+temp+":"+os.Getenv("PATH"), "OVERSYNC_TIMEOUT_GRACE_SECONDS=1", "OVERSYNC_TEST_RUN_ID=runner-test")
	err = command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 124 {
		t.Fatalf("exit=%v want 124", err)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(logged)
	if !strings.Contains(text, "label=com.mobiletoly.go-oversync.test-run=runner-test") || !strings.Contains(text, "rm -fv container-for-test") {
		t.Fatalf("cleanup trap calls missing: %q", text)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}
