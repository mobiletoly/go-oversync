package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type LocalMemberConfig struct {
	Name                string
	TableStage          RolloutTableStage
	ColumnStage         RolloutColumnStage
	Ledger              *oversync.GenerationLedger
	QualificationSHA256 string
	JWTSecret           string
	ClientPolicy        string
	ClientTokens        []string
	Successor           *LocalMemberSuccessor
}

// LocalMemberSuccessor selects the compiled unchanged-schema successor path.
// The predecessor ledger remains explicit; no lifecycle history is inferred.
type LocalMemberSuccessor struct {
	PreviousLedger *oversync.GenerationLedger
	Generation     oversync.SchemaGeneration
	Revision       int64
}

func localMemberConfiguration(input LocalMemberConfig) (*oversync.ServiceConfig, error) {
	ledger := input.Ledger
	if input.Successor != nil {
		ledger = input.Successor.PreviousLedger
	}
	var cfg *oversync.ServiceConfig
	var err error
	if input.ColumnStage != 0 {
		cfg, err = RolloutColumnConfiguration(input.ColumnStage, ledger, input.QualificationSHA256)
	} else {
		cfg, err = RolloutTableConfiguration(input.TableStage, ledger, input.QualificationSHA256)
	}
	if err != nil {
		return nil, err
	}
	if input.Successor != nil {
		return RolloutSuccessorConfiguration(cfg, input.Successor.Generation, input.Ledger, input.Successor.Revision, input.QualificationSHA256)
	}
	return cfg, nil
}

// ServeLocalRolloutMember is the reference child-process entry point. Readiness
// and its database session come from this process's own successful Bootstrap.
// Its loopback control endpoint uses the unpredictable process-start identity.
func ServeLocalRolloutMember(ctx context.Context, dsn string, input LocalMemberConfig, handshake io.Writer) error {
	if input.Name == "" || dsn == "" || handshake == nil {
		return fmt.Errorf("member_identity_required")
	}
	admissionDigest, err := memberAdmissionDigest(input)
	if err != nil {
		return err
	}
	cfg, err := localMemberConfiguration(input)
	if err != nil {
		return err
	}
	base, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("member_database_configuration_invalid")
	}
	pair, err := oversync.NewReadinessPoolPair(ctx, base, 2)
	if err != nil {
		return err
	}
	defer pair.Close()
	var privileged bool
	if err = pair.ExecutionPool().QueryRow(ctx, `SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname=current_user`).Scan(&privileged); err != nil {
		return err
	}
	if privileged {
		return fmt.Errorf("fleet_member_cannot_use_privileged_database_role")
	}
	cfg.Readiness.PoolPair = pair
	service, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	defer service.Close(context.Background())
	if err = service.Bootstrap(ctx); err != nil {
		return err
	}
	watchContext, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go func() { _ = service.RunBundleChangeListener(watchContext) }()
	pinned, err := pair.ExecutionPool().Acquire(ctx)
	if err != nil {
		return err
	}
	defer pinned.Release()
	att := FleetAttestation{Name: input.Name, Readiness: service.Readiness(), RuntimeAdmissionSHA256: admissionDigest, ClientPolicy: input.ClientPolicy}
	if err = pinned.QueryRow(ctx, `SELECT current_user,pg_backend_pid(),backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()`).Scan(&att.Role, &att.BackendPID, &att.BackendStart); err != nil {
		return err
	}
	if err = pinned.QueryRow(ctx, `SELECT epoch,client_policy FROM app_evolution.maintenance WHERE singleton`).Scan(&att.Epoch, &att.ClientPolicy); err != nil {
		return err
	}
	if att.ClientPolicy != input.ClientPolicy {
		return fmt.Errorf("member_client_policy_mismatch")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	att.Address = "http://" + listener.Addr().String()
	var mutex sync.Mutex
	mux := http.NewServeMux()
	handler := registerMemberSyncRoutes(mux, service, input)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Rollout-Process") != att.Readiness.ProcessStartID {
			http.NotFound(w, r)
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		live := att
		if err := pinned.QueryRow(r.Context(), `SELECT epoch FROM app_evolution.maintenance WHERE singleton`).Scan(&live.Epoch); err != nil {
			http.Error(w, "member unavailable", 503)
			return
		}
		live.Readiness = service.Readiness()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(live)
	})
	mux.HandleFunc("/write", rolloutMemberWriteHandler(service, input.ColumnStage != 0, att.Readiness.ProcessStartID))
	server := http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	if err = json.NewEncoder(handshake).Encode(att); err != nil {
		_ = server.Close()
		return err
	}
	select {
	case <-ctx.Done():
		_ = server.Close()
		return nil
	case err = <-stopped:
		return err
	}
}

func rolloutMemberWriteHandler(service *oversync.SyncService, column bool, processID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-Rollout-Process") != processID {
			http.NotFound(w, r)
			return
		}
		scope := r.URL.Query().Get("scope")
		if scope == "" {
			http.Error(w, "scope required", 400)
			return
		}
		_, err := oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}).ExecWrite(r.Context(), scope, oversync.ScopeWriteOptions{WriterID: "schema-evolution-writer", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: sha256.Sum256([]byte("fleet-write:" + scope)), OperationValidUntil: time.Now().Add(time.Minute)}}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			statement := `UPDATE main.people SET name='Fleet' WHERE _sync_scope_id=$1`
			if column {
				statement = `UPDATE main.upgrade_people SET name='Fleet' WHERE _sync_scope_id=$1`
			}
			_, err := tx.Exec(ctx, statement, scope)
			return err
		})
		if err != nil {
			http.Error(w, "write unavailable", 503)
			return
		}
		w.WriteHeader(204)
	}
}
