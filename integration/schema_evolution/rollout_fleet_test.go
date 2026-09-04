package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRolloutFleet_ThreeProcessesFenceAndReopen(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, _, a, ledger := rolloutServingFixture(t)
	defer a.Close()
	qualification := digest([]byte("TestRolloutFleet_ThreeProcessesFenceAndReopen"))
	cfg, err := RolloutTableConfiguration(RolloutTableLedger, ledger, qualification)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	qualifyRolloutFleet(t, pool, a, cfg)
}

func qualifyRolloutFleet(t *testing.T, pool *pgxpool.Pool, a *Authority, cfg *oversync.ServiceConfig) DrainProof {
	t.Helper()
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "rollout-member")
	buildCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "./cmd/rollout-member")
	build.Env = append(os.Environ(), "GOTOOLCHAIN=go1.25.12")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	bytes, err := os.ReadFile(binary)
	require.NoError(t, err)
	h := sha256.Sum256(bytes)
	binaryHash := hex.EncodeToString(h[:])
	token, err := exampleauth.New("rollout-fleet-test").GenerateToken("retirement-scope-0", time.Hour)
	require.NoError(t, err)
	var members []*LocalFleetProcess
	var credentials []string
	for _, name := range []string{"server-a", "server-b", "writer"} {
		role := "m8_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		password := strings.ReplaceAll(uuid.NewString(), "-", "")
		_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD '`+password+`'`)
		require.NoError(t, err)
		require.NoError(t, a.GrantTableRuntime(ctx, role))
		parsed, err := url.Parse(pool.Config().ConnString())
		require.NoError(t, err)
		parsed.User = url.UserPassword(role, password)
		runtimeDSN := parsed.String()
		credentials = append(credentials, runtimeDSN)
		config := LocalMemberConfig{Name: name, TableStage: RolloutTableLedger, Ledger: cfg.Readiness.Expected.GenerationLedger, QualificationSHA256: cfg.Readiness.Expected.QualificationSHA256, JWTSecret: "rollout-fleet-test", ClientPolicy: "table_evolution-preserving-v1", ClientTokens: []string{"Bearer " + token}}
		file := filepath.Join(t.TempDir(), "member.json")
		data, err := json.Marshal(config)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(file, data, 0600))
		// Bootstrap takes the authority's shared lock; no exclusive owner remains held.
		a.Close()
		launchCtx, launchCancel := context.WithTimeout(ctx, 15*time.Second)
		member, err := StartLocalFleetProcess(launchCtx, binary, binaryHash, []string{"-config", file}, []string{"DATABASE_URL=" + runtimeDSN})
		launchCancel()
		require.NoError(t, err)
		members = append(members, member)
		t.Cleanup(member.Stop)
		a, err = OpenClosedRolloutAuthority(ctx, pool)
		require.NoError(t, err)
		defer a.Close()
	}
	p := rolloutTestPlan(t, a, cfg, "reopen")
	p.Members = nil
	for _, member := range members {
		p.Members = append(p.Members, member.Member())
	}
	a.Close()
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	_, err = ReopenLocalFleet(ctx, pool, p, report, cfg, members[:2])
	require.ErrorContains(t, err, "membership_incomplete")
	wrong := p
	wrong.Members = append([]RolloutMember(nil), p.Members...)
	wrong.Members[0].ProcessStartID = uuid.NewString()
	_, err = ReopenLocalFleet(ctx, pool, wrong, report, cfg, members)
	require.ErrorContains(t, err, "complete_unblocked_report_required")
	wrongReport, err := RolloutReportReadOnly(ctx, pool, wrong)
	require.NoError(t, err)
	_, err = ReopenLocalFleet(ctx, pool, wrong, wrongReport, cfg, members)
	require.ErrorContains(t, err, "membership_changed")
	result, err := ReopenLocalFleet(ctx, pool, p, report, cfg, members)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	replay, err := ReopenLocalFleet(ctx, pool, p, report, cfg, members)
	require.NoError(t, err)
	require.Equal(t, result, replay)
	request, err := http.NewRequest(http.MethodPost, members[2].initial.Address+"/write?scope=retirement-scope-0", nil)
	require.NoError(t, err)
	request.Header.Set("X-Rollout-Process", members[2].initial.Readiness.ProcessStartID)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, 204, response.StatusCode)
	_ = response.Body.Close()
	watch := openFleetWatch(t, members[0].Address(), token, cfg.Readiness.Generations[0].Proof)
	defer watch.Body.Close()
	proof, err := Drain(ctx, pool, nil, func(ctx context.Context) error { return FenceLocalFleet(ctx, pool, members) })
	require.NoError(t, err)
	readDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, watch.Body); close(readDone) }()
	select {
	case <-readDone:
	case <-time.After(3 * time.Second):
		t.Fatal("fenced watch survived its process and database role")
	}
	for _, member := range members {
		if usage, ok := member.command.ProcessState.SysUsage().(*syscall.Rusage); ok {
			bytes := usage.Maxrss
			if runtime.GOOS != "darwin" {
				bytes *= 1024
			}
			t.Logf("fleet member=%s peak_rss_bytes=%d", member.Member().Name, bytes)
		}
	}
	for _, dsn := range credentials {
		stale, err := pgx.Connect(ctx, dsn)
		if stale != nil {
			_ = stale.Close(ctx)
		}
		require.Error(t, err, "old credentials must reject direct SQL and automatic restarts")
	}
	var requested, closed bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT requested,closed FROM app_evolution.maintenance CROSS JOIN app_evolution.barrier`).Scan(&requested, &closed))
	require.True(t, requested)
	require.True(t, closed)
	for _, member := range members {
		role := pgx.Identifier{member.initial.Role}.Sanitize()
		_, err = pool.Exec(ctx, `DROP OWNED BY `+role)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `DROP ROLE `+role)
		require.NoError(t, err)
	}
	return proof
}

func openFleetWatch(t *testing.T, address, token string, proof oversync.SchemaGenerationProof) *http.Response {
	t.Helper()
	request := func(method, path string) *http.Request {
		r, err := http.NewRequest(method, address+path, strings.NewReader("{}"))
		require.NoError(t, err)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set(oversync.SourceIDHeader, "fleet-watch-source")
		r.Header.Set(oversync.SchemaGenerationIDHeader, fmt.Sprint(proof.ID))
		r.Header.Set(oversync.SchemaGenerationFingerprintHeader, proof.Fingerprint)
		r.Header.Set("Content-Type", "application/json")
		return r
	}
	connected, err := http.DefaultClient.Do(request("POST", "/sync/connect"))
	require.NoError(t, err)
	defer connected.Body.Close()
	body, err := io.ReadAll(connected.Body)
	require.NoError(t, err)
	require.Equal(t, 200, connected.StatusCode, string(body))
	watch, err := http.DefaultClient.Do(request("GET", "/sync/watch"))
	require.NoError(t, err)
	require.Equal(t, 200, watch.StatusCode)
	return watch
}
