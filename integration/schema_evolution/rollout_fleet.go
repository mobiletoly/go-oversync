package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type FleetAttestation struct {
	Name, Role, Address    string
	RuntimeAdmissionSHA256 string
	ClientPolicy           string
	BackendPID             int
	BackendStart           time.Time
	Epoch                  int64
	Readiness              oversync.ReadinessReport
}

type LocalFleetProcess struct {
	binaryDigest string
	initial      FleetAttestation
	command      *exec.Cmd
	exited       chan struct{}
	once         sync.Once
}

// StartLocalFleetProcess launches a real compiled Go member. The deployment
// driver supplies its executable and private environment; neither is accepted
// through the sync HTTP API. The executable's exact bytes are checked first.
func StartLocalFleetProcess(ctx context.Context, path, binaryDigest string, args, environment []string) (*LocalFleetProcess, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(file, (256<<20)+1))
	_ = file.Close()
	if err != nil {
		return nil, err
	}
	if n > 256<<20 || hex.EncodeToString(h.Sum(nil)) != binaryDigest {
		return nil, fmt.Errorf("fleet_binary_mismatch")
	}
	startupLog := &boundedFleetLog{}
	command := exec.Command(path, args...)
	command.Stderr = startupLog
	command.Env = append(os.Environ(), environment...)
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = command.Start(); err != nil {
		return nil, err
	}
	member := &LocalFleetProcess{command: command, binaryDigest: binaryDigest, exited: make(chan struct{})}
	go func() { _ = command.Wait(); close(member.exited) }()
	type handshake struct {
		att FleetAttestation
		err error
	}
	received := make(chan handshake, 1)
	go func() {
		var att FleetAttestation
		err := json.NewDecoder(io.LimitReader(output, 1<<20)).Decode(&att)
		received <- handshake{att, err}
	}()
	select {
	case value := <-received:
		if value.err != nil {
			member.Stop()
			return nil, fmt.Errorf("fleet_member_bootstrap_failed: %w: %s", value.err, startupLog.text())
		}
		member.initial = value.att
	case <-ctx.Done():
		member.Stop()
		return nil, ctx.Err()
	}
	if !member.initial.Readiness.Ready || member.initial.Address == "" || member.initial.Readiness.ProcessStartID == "" {
		member.Stop()
		return nil, fmt.Errorf("fleet_private_bootstrap_failed")
	}
	return member, nil
}
func (m *LocalFleetProcess) Stop() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		if m.command.Process != nil {
			_ = m.command.Process.Kill()
		}
	})
}
func (m *LocalFleetProcess) Member() RolloutMember {
	return RolloutMember{Name: m.initial.Name, Role: m.initial.Role, ProcessStartID: m.initial.Readiness.ProcessStartID, ApplicationCodeSHA256: m.initial.Readiness.ApplicationCodeSHA256, BinarySHA256: m.binaryDigest, RuntimeAdmissionSHA256: m.initial.RuntimeAdmissionSHA256}
}

// Address is the loopback HTTP endpoint advertised by this exact child process.
func (m *LocalFleetProcess) Address() string { return m.initial.Address }
func (m *LocalFleetProcess) Attestation(ctx context.Context) (FleetAttestation, error) {
	var att FleetAttestation
	select {
	case <-m.exited:
		return att, fmt.Errorf("fleet_member_exited")
	default:
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.initial.Address+"/ready", nil)
	if err != nil {
		return att, err
	}
	req.Header.Set("X-Rollout-Process", m.initial.Readiness.ProcessStartID)
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return att, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return att, fmt.Errorf("fleet_member_not_ready")
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&att); err != nil {
		return att, err
	}
	if att.RuntimeAdmissionSHA256 != m.initial.RuntimeAdmissionSHA256 || att.ClientPolicy != m.initial.ClientPolicy || att.Readiness.ProcessStartID != m.initial.Readiness.ProcessStartID || att.BackendPID != m.initial.BackendPID || !att.BackendStart.Equal(m.initial.BackendStart) {
		return att, fmt.Errorf("fleet_process_identity_changed")
	}
	return att, nil
}

// FenceLocalFleet disables credentials and terminates existing database sessions
// before stopping processes. Automatic restart with old credentials cannot write.
// Drain invokes it only after the durable global maintenance request is closed.
func FenceLocalFleet(ctx context.Context, pool *pgxpool.Pool, members []*LocalFleetProcess) error {
	var requested bool
	if err := pool.QueryRow(ctx, `SELECT requested FROM app_evolution.maintenance WHERE singleton`).Scan(&requested); err != nil {
		return err
	}
	if !requested {
		return fmt.Errorf("fleet_maintenance_request_required")
	}
	for _, member := range members {
		if member == nil || !rolloutRoleName.MatchString(member.initial.Role) {
			return fmt.Errorf("fleet_role_identity_required")
		}
		var authority string
		if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&authority); err != nil {
			return err
		}
		if authority == member.initial.Role {
			return fmt.Errorf("fleet_cannot_use_authority_credential")
		}
		if _, err := pool.Exec(ctx, `ALTER ROLE `+pgx.Identifier{member.initial.Role}.Sanitize()+` NOLOGIN`); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=$1 AND pid<>pg_backend_pid()`, member.initial.Role); err != nil {
			return err
		}
		member.Stop()
		select {
		case <-member.exited:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func verifyLocalFleet(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, target *oversync.ServiceConfig, members []*LocalFleetProcess) error {
	if len(members) != len(p.Members) || len(members) < 1 {
		return fmt.Errorf("fleet_membership_incomplete")
	}
	for i, member := range members {
		if member == nil || member.Member() != p.Members[i] {
			return fmt.Errorf("fleet_membership_changed")
		}
		att, err := member.Attestation(ctx)
		if err != nil {
			return err
		}
		if att.Name != p.Members[i].Name || att.Role != p.Members[i].Role || att.Epoch != p.Epoch || att.ClientPolicy != p.Policy.ClientAdmissionPolicy || !att.Readiness.Ready || att.Readiness.DeploymentSHA256 != oversync.DeploymentContractDigest(target.Readiness.Expected) || att.Readiness.CatalogRevision != p.CatalogRevision || att.Readiness.CatalogFingerprint != p.CatalogFingerprint {
			return fmt.Errorf("fleet_target_mismatch")
		}
		var live bool
		if err = r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2 AND usename=$3 AND datname=current_database())`, att.BackendPID, att.BackendStart, att.Role).Scan(&live); err != nil {
			return err
		}
		if !live {
			return fmt.Errorf("fleet_database_session_missing")
		}
	}
	return nil
}

// ReopenLocalFleet is separate from ApplyRollout: only this actual process driver
// can supply the live membership proof needed for the final admission transition.
func ReopenLocalFleet(ctx context.Context, pool *pgxpool.Pool, p RolloutPlan, report RolloutReport, target *oversync.ServiceConfig, members []*LocalFleetProcess) (RolloutResult, error) {
	var result RolloutResult
	if p.Action != "reopen" {
		return result, fmt.Errorf("reopen_plan_required")
	}
	planHash, err := validateRolloutRequest(p, report, target)
	if err != nil {
		return result, err
	}
	if stored, storedReport, done, statusErr := RolloutStatus(ctx, pool, p.ID); statusErr == nil && done.State == "completed" {
		oldHash, err := RolloutPlanDigest(stored)
		if err != nil || oldHash != planHash || !reflect.DeepEqual(storedReport, report) {
			return result, fmt.Errorf("rollout_operation_identity_conflict")
		}
		return done, nil
	}
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	ctx, queryCancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer queryCancel()
	if err = verifyLocalFleet(ctx, pool, p, target, members); err != nil {
		return result, err
	}
	a, err := OpenClosedRolloutAuthority(ctx, pool)
	if err != nil {
		return result, err
	}
	defer a.Close()
	_, err = a.reserveRollout(ctx, p, planHash, report, &result)
	if err != nil {
		return result, err
	}
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := validateFleetReopen(ctx, tx, p, report, target, members); err != nil {
			return err
		}
		result = RolloutResult{OperationID: p.ID, State: "completed"}
		body, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err = recordRolloutBatch(ctx, tx, p, body); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE app_evolution.rollout_operations SET state='completed',result=$2,completed_at=transaction_timestamp() WHERE operation_id=$1`, p.ID, body); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE app_evolution.barrier SET closed=false WHERE singleton AND revision=$1`, p.Epoch); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE app_evolution.maintenance SET requested=false,completed_at=transaction_timestamp() WHERE singleton AND epoch=$1`, p.Epoch)
		return err
	})
	return result, err
}

type boundedFleetLog struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedFleetLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	left := 4096 - len(b.data)
	if left > 0 {
		b.data = append(b.data, p[:min(left, len(p))]...)
	}
	return len(p), nil
}
func (b *boundedFleetLog) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.data))
}

func validateFleetReopen(ctx context.Context, tx pgx.Tx, p RolloutPlan, report RolloutReport, target *oversync.ServiceConfig, members []*LocalFleetProcess) error {
	if err := requireRolloutAuthority(ctx, tx); err != nil {
		return err
	}
	if err := lockAdmission(ctx, tx, true); err != nil {
		return err
	}
	if err := validateRecoveryAdmission(ctx, tx, &p); err != nil {
		return err
	}
	for _, member := range p.Members {
		if err := validateRuntimeRole(ctx, tx, member.Role); err != nil {
			return err
		}
	}
	live, err := inspectRollout(ctx, tx, p)
	if err != nil {
		return err
	}
	if len(live.Blockers) != 0 || live.StateDigest != report.StateDigest {
		return fmt.Errorf("reopen_report_stale_or_blocked")
	}
	if err = verifyLocalFleet(ctx, tx, p, target, members); err != nil {
		return err
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(target.RegisteredTables, target.SchemaGenerations)
	if err != nil {
		return err
	}
	evidence, err := target.Readiness.Adapter.Inspect(ctx, tx)
	if err != nil {
		return err
	}
	if err = oversync.ValidateApplicationReadiness(target.Readiness, registry, evidence); err != nil {
		return err
	}
	return nil
}
