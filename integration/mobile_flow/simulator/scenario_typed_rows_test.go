package simulator

import (
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
)

type typedRowsStatusRecorder struct {
	base http.RoundTripper

	mu           sync.Mutex
	pushStatuses []int
}

func (r *typedRowsStatusRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(request)
	if err == nil && request.URL.Path == "/sync/push" {
		r.mu.Lock()
		r.pushStatuses = append(r.pushStatuses, response.StatusCode)
		r.mu.Unlock()
	}
	return response, err
}

func (r *typedRowsStatusRecorder) sawPushStatus(status int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, actual := range r.pushStatuses {
		if actual == status {
			return true
		}
	}
	return false
}

func TestTypedRowsScenarioVerifiesDeclaredCodecConversion(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		Logger:    logger,
		JWTSecret: "typed-rows-contract-test-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const userID = "typed-rows-contract-user"
	app, err := newMobileApp(&mobileAppConfig{
		DatabaseFile:     filepath.Join(t.TempDir(), "mobile.db"),
		ServerURL:        server.URL(),
		UserID:           userID,
		DeviceName:       "typed-rows-contract-device",
		JWTSecret:        "typed-rows-contract-test-secret",
		OversqliteConfig: standardScenarioOversqliteConfig(),
		PreserveDB:       true,
		Logger:           logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.close()

	ctx := context.Background()
	if err := app.onLaunch(ctx); err != nil {
		t.Fatal(err)
	}
	baseTransport := app.client.HTTP.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	statusRecorder := &typedRowsStatusRecorder{base: baseTransport}
	app.client.HTTP.Transport = statusRecorder
	scenario := &TypedRowsScenario{BaseScenario: &BaseScenario{
		simulator: &Simulator{logger: logger},
		config:    &config.ScenarioConfig{UserID: userID},
		app:       app,
	}}
	if err := scenario.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	if err := scenario.Verify(ctx, &DatabaseVerifier{pool: server.Pool, logger: logger}); err != nil {
		t.Fatal(err)
	}
	if !statusRecorder.sawPushStatus(http.StatusOK) {
		t.Fatal("typed-rows scenario did not receive a successful direct push response")
	}

	var localRows, outboxRows, serverRows int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM typed_rows`).Scan(&localRows); err != nil {
		t.Fatal(err)
	}
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if err := server.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM business.typed_rows WHERE _sync_scope_id=$1`, userID).Scan(&serverRows); err != nil {
		t.Fatal(err)
	}
	if localRows != 2 || outboxRows != 0 || serverRows != 2 {
		t.Fatalf("typed-row boundary state local=%d outbox=%d server=%d, want 2/0/2", localRows, outboxRows, serverRows)
	}
}
