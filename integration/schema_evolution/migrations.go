package schemaevolution

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

//go:embed migrations/*.sql control.sql operation-encoding-v*.json
var migrationFiles embed.FS

var migrationName = regexp.MustCompile(`^([0-9]{4})_[a-z][a-z0-9_]*[.]sql$`)
var authorityLock = [2]int32{1869833075, 6}
var workerLock = [2]int32{1869833075, 7}

// Manifest validates exact file identities, including a mounted copy checked
// against the immutable embedded application artifact by Apply.
func Manifest(files fs.FS) ([]oversync.MigrationIdentity, error) {
	entries, err := fs.ReadDir(files, "migrations")
	if err != nil {
		return nil, err
	}
	var result []oversync.MigrationIdentity
	seen := map[int64]bool{}
	for _, entry := range entries {
		match := migrationName.FindStringSubmatch(entry.Name())
		if entry.IsDir() || match == nil {
			return nil, fmt.Errorf("invalid migration filename")
		}
		version, _ := strconv.ParseInt(match[1], 10, 64)
		if version == 0 || seen[version] {
			return nil, fmt.Errorf("duplicate/zero migration version")
		}
		seen[version] = true
		path := "migrations/" + entry.Name()
		bytes, err := fs.ReadFile(files, path)
		if err != nil {
			return nil, err
		}
		class := "expand"
		if version == 4 {
			class = "constraint"
		}
		result = append(result, oversync.MigrationIdentity{Version: version, Filename: path, SHA256: digest(bytes), Transactional: version != 3, Class: class})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })
	return result, nil
}

// DrainProof can only be obtained after the controller stops every supplied
// process and its external writer callback, then closes the durable barrier.
// Fleet membership and disabling old-process restart remain controller duties.
type DrainProof struct{ pool *pgxpool.Pool }

func Drain(ctx context.Context, pool *pgxpool.Pool, services []*oversync.SyncService, stopWriters func(context.Context) error) (DrainProof, error) {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	// Closure commits before any callback or wait. A failed drain cannot reopen it.
	if _, err := pool.Exec(bounded, `UPDATE app_evolution.maintenance SET epoch=epoch+CASE WHEN requested THEN 0 ELSE 1 END,requested=true,completed_at=NULL WHERE singleton`); err != nil {
		return DrainProof{}, err
	}
	for _, service := range services {
		service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	}
	if stopWriters == nil {
		return DrainProof{}, fmt.Errorf("writer drain callback required")
	}
	if err := stopWriters(bounded); err != nil {
		return DrainProof{}, err
	}
	for _, service := range services {
		if err := service.Close(bounded); err != nil {
			return DrainProof{}, err
		}
	}
	// Existing application writes hold a shared row lock through commit. UPDATE
	// waits for those transactions before recording closure.
	err := pgx.BeginTxFunc(bounded, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := lockAdmission(bounded, tx, true); err != nil {
			return err
		}
		_, err := tx.Exec(bounded, `UPDATE app_evolution.barrier SET closed=true,revision=(SELECT epoch FROM app_evolution.maintenance WHERE singleton) WHERE singleton`)
		return err
	})
	if err != nil {
		return DrainProof{}, err
	}
	return DrainProof{pool: pool}, nil
}

type Authority struct {
	lockWait time.Duration
	conn     *pgxpool.Conn
	files    fs.FS
	manifest []oversync.MigrationIdentity
}

// OpenFreshAuthority is only for a new database with no application schema or
// serving processes. Existing databases require Drain and OpenAuthority.
func OpenFreshAuthority(ctx context.Context, pool *pgxpool.Pool) (*Authority, error) {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded

	a, err := openAuthority(ctx, pool)
	if err != nil {
		return nil, err
	}
	var exists bool
	err = a.conn.QueryRow(ctx, `SELECT to_regnamespace('app_evolution') IS NOT NULL OR to_regnamespace('main') IS NOT NULL`).Scan(&exists)
	if err != nil || exists {
		a.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("fresh application database required")
	}
	sql, _ := migrationFiles.ReadFile("control.sql")
	if _, err = a.conn.Exec(ctx, string(sql)); err != nil {
		a.Close()
		return nil, err
	}
	return a, nil
}

func OpenAuthority(ctx context.Context, proof DrainProof) (*Authority, error) {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded

	if proof.pool == nil {
		return nil, fmt.Errorf("completed drain required")
	}
	a, err := openAuthority(ctx, proof.pool)
	if err != nil {
		return nil, err
	}
	var closed bool
	err = a.conn.QueryRow(ctx, `SELECT closed FROM app_evolution.barrier WHERE singleton`).Scan(&closed)
	if err != nil || !closed {
		a.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("application barrier is not closed")
	}
	return a, nil
}

func openAuthority(ctx context.Context, pool *pgxpool.Pool) (*Authority, error) {
	started := time.Now()
	conn, err := acquireSessionLock(ctx, pool, authorityLock)
	wait := time.Since(started)
	if err != nil {
		return nil, err
	}
	manifest, err := Manifest(migrationFiles)
	if err != nil {
		releaseSessionLock(conn, authorityLock)
		return nil, err
	}
	return &Authority{conn: conn, files: migrationFiles, manifest: manifest, lockWait: wait}, nil
}

func acquireSessionLock(ctx context.Context, pool *pgxpool.Pool, key [2]int32) (*pgxpool.Conn, error) {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pool.Acquire(bounded)
	if err != nil {
		return nil, err
	}
	for {
		var locked bool
		err = conn.QueryRow(bounded, `SELECT pg_try_advisory_lock($1::int,$2::int)`, key[0], key[1]).Scan(&locked)
		if err == nil && locked {
			return conn, nil
		}
		if err != nil {
			releaseSessionLock(conn, key)
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-bounded.Done():
			timer.Stop()
			releaseSessionLock(conn, key)
			return nil, bounded.Err()
		case <-timer.C:
		}
	}
}

func releaseSessionLock(conn *pgxpool.Conn, key [2]int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseSessionLockContext(ctx, conn, key)
}

func releaseSessionLockContext(ctx context.Context, conn *pgxpool.Conn, key [2]int32) {
	if conn == nil {
		return
	}
	var unlocked bool
	if err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1::int,$2::int)`, key[0], key[1]).Scan(&unlocked); err != nil {
		_ = conn.Conn().Close(ctx)
	}
	conn.Release()
}

func (a *Authority) Close() {
	if a.conn != nil {
		releaseSessionLock(a.conn, authorityLock)
		a.conn = nil
	}
}

// Apply is deliberately limited to this application's four migrations. It
// replays every file on fresh databases and never stamps a final-schema baseline.
func (a *Authority) Apply(ctx context.Context, through int64) error {
	if through < 1 || through > 4 {
		return fmt.Errorf("unsupported migration boundary")
	}
	if err := a.checkAppliedPrefix(ctx, through); err != nil {
		return err
	}
	for _, m := range a.manifest {
		if m.Version > through {
			break
		}
		if err := a.applyMigration(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (a *Authority) checkAppliedPrefix(ctx context.Context, through int64) error {
	rows, err := a.conn.Query(ctx, `SELECT version,identity FROM app_evolution.migrations ORDER BY version`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var version int64
		var identity oversync.MigrationIdentity
		if err := rows.Scan(&version, &identity); err != nil {
			return err
		}
		if version < 1 || version > through || version > int64(len(a.manifest)) || a.manifest[version-1] != identity {
			return fmt.Errorf("migration prefix drift")
		}
	}
	return rows.Err()
}

func (a *Authority) applyMigration(ctx context.Context, m oversync.MigrationIdentity) error {
	if m.Class == "contract" {
		return fmt.Errorf("contract migrations are not authorized")
	}
	content, err := fs.ReadFile(a.files, m.Filename)
	if err != nil {
		return err
	}
	if digest(content) != m.SHA256 {
		return fmt.Errorf("mounted migration checksum mismatch")
	}
	var complete bool
	err = a.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.migrations WHERE version=$1 AND state='completed')`, m.Version).Scan(&complete)
	if err != nil || complete {
		return err
	}
	if m.Version == 4 {
		if err := a.requireMaintainingCertificate(ctx); err != nil {
			return err
		}
	}
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err = a.conn.Exec(bounded, `INSERT INTO app_evolution.migrations(version,identity,state,started_at) VALUES($1,$2,'started',clock_timestamp()) ON CONFLICT(version) DO UPDATE SET state='started',failed_at=NULL`, m.Version, m)
	if err != nil {
		return err
	}
	if m.Transactional {
		err = pgx.BeginTxFunc(bounded, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(bounded, string(content)); err != nil {
				return err
			}
			_, err := tx.Exec(bounded, `UPDATE app_evolution.migrations SET state='completed',completed_at=clock_timestamp(),failed_at=NULL WHERE version=$1`, m.Version)
			return err
		})
	} else {
		err = a.reconcileIndex(bounded, string(content), m.Version)
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = a.conn.Exec(cleanup, `UPDATE app_evolution.migrations SET state='failed',failed_at=clock_timestamp() WHERE version=$1 AND state<>'completed'`, m.Version)
	}
	return err
}

func (a *Authority) reconcileIndex(ctx context.Context, statement string, version int64) error {
	var exists, valid, matching bool
	err := a.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('main.upgrade_people_name_idx')),COALESCE((SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid=to_regclass('main.upgrade_people_name_idx')),false),COALESCE((SELECT pg_get_indexdef(indexrelid)='CREATE INDEX upgrade_people_name_idx ON main.upgrade_people USING btree (name)' FROM pg_index WHERE indexrelid=to_regclass('main.upgrade_people_name_idx')),false)`).Scan(&exists, &valid, &matching)
	if err != nil {
		return err
	}
	if exists && !matching {
		return fmt.Errorf("index definition drift requires operator correction")
	}
	if exists && !valid {
		if _, err = a.conn.Exec(ctx, `DROP INDEX CONCURRENTLY main.upgrade_people_name_idx`); err != nil {
			return err
		}
	}
	if !exists || !valid {
		if _, err = a.conn.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if err = a.conn.QueryRow(ctx, `SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid='main.upgrade_people_name_idx'::regclass`).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("index reconciliation incomplete")
	}
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.migrations SET state='completed',completed_at=clock_timestamp(),failed_at=NULL WHERE version=$1`, version)
	return err
}

func encodeJSON(v any) []byte { b, _ := json.Marshal(v); return b }

// Abandon retains application progress and successful registered changes. The
// controller must subsequently publish and restart an old-only deployment;
// abandoned work can never enable its target generation.
func (a *Authority) Abandon(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return pgx.BeginTxFunc(bounded, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(bounded, `UPDATE app_evolution.jobs SET state='abandoned',pause_reason='operator_abandoned' WHERE job=$1`, JobID); err != nil {
			return err
		}
		_, err := tx.Exec(bounded, `UPDATE app_evolution.deployment SET generations=(SELECT jsonb_agg(CASE WHEN value->'Requirement'->'Proof'->>'ID'='3' THEN jsonb_set(value,'{Enabled}','false') ELSE value END) FROM jsonb_array_elements(generations)) WHERE singleton`)
		return err
	})
}

// GrantRuntime demonstrates the application/migration credential boundary.
// The migration role retains ownership of DDL, ledgers and certificates. Runtime
// attaches to an existing provisioned layout and cannot replace any trigger.
func (a *Authority) GrantRuntime(ctx context.Context, role string) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded

	if role == "" {
		return fmt.Errorf("runtime role required")
	}
	r := pgx.Identifier{role}.Sanitize()
	statements := []string{
		`REVOKE TRIGGER ON main.upgrade_people FROM ` + r,
		`REVOKE TRIGGER ON main.upgrade_people FROM PUBLIC`,
		`GRANT USAGE ON SCHEMA sync TO ` + r,
		`GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA sync TO ` + r,
		`GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA sync TO ` + r,
		`GRANT USAGE ON SCHEMA main,app_evolution TO ` + r,
		`GRANT SELECT,INSERT,UPDATE,DELETE ON main.upgrade_people TO ` + r,
		`GRANT SELECT ON ALL TABLES IN SCHEMA app_evolution TO ` + r,
		`GRANT INSERT ON app_evolution.scope_inventory TO ` + r,
		`GRANT UPDATE(deleted) ON app_evolution.scope_inventory TO ` + r,
		`GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA app_evolution TO ` + r,
	}
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, statement := range statements {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return err
			}
		}
		return validateRuntimeRole(ctx, tx, role)
	})
}

// ACLs do not revoke ownership. Conservatively reject every privileged role
// reachable through membership, even when INHERIT or SET options currently deny use.
func validateRuntimeRole(ctx context.Context, tx pgx.Tx, role string) error {
	var unsafe bool
	err := tx.QueryRow(ctx, `WITH RECURSIVE reachable(oid) AS (
 SELECT oid FROM pg_roles WHERE rolname=$1
 UNION SELECT m.roleid FROM pg_auth_members m JOIN reachable r ON m.member=r.oid
 ), protected AS (SELECT oid,nspowner FROM pg_namespace WHERE nspname IN ('main','sync','app_evolution'))
 SELECT EXISTS(SELECT 1 FROM reachable r JOIN pg_roles p USING(oid) WHERE
 p.rolsuper OR p.rolcreaterole OR p.rolbypassrls OR p.rolreplication
 OR has_parameter_privilege(r.oid,'session_replication_role','SET')
 OR EXISTS(SELECT 1 FROM pg_db_role_setting s, unnest(s.setconfig) setting
 WHERE s.setrole IN (0,r.oid) AND s.setdatabase IN (0,(SELECT oid FROM pg_database WHERE datname=current_database()))
 AND split_part(setting,'=',1)='session_replication_role' AND lower(split_part(setting,'=',2))<>'origin')
 OR EXISTS(SELECT 1 FROM protected n WHERE n.nspowner=r.oid OR has_schema_privilege(r.oid,n.oid,'CREATE'))
 OR EXISTS(SELECT 1 FROM pg_class c JOIN protected n ON n.oid=c.relnamespace WHERE c.relowner=r.oid)
 OR EXISTS(SELECT 1 FROM pg_proc f JOIN protected n ON n.oid=f.pronamespace WHERE f.proowner=r.oid)
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='main' AND c.relkind IN ('r','p') AND has_table_privilege(r.oid,c.oid,'TRIGGER'))
 OR has_table_privilege(r.oid,'app_evolution.scope_inventory','DELETE,TRUNCATE,TRIGGER')
 OR EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='app_evolution.scope_inventory'::regclass
 AND a.attnum>0 AND NOT a.attisdropped AND a.attname<>'deleted'
 AND has_column_privilege(r.oid,a.attrelid,a.attnum,'UPDATE'))
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='app_evolution' AND c.relkind IN ('r','p') AND c.relname<>'scope_inventory'
 AND (has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
 OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE')))
 )`, role).Scan(&unsafe)
	if err != nil {
		return err
	}
	if unsafe {
		return fmt.Errorf("runtime role has protected ownership or reachable provisioning privileges")
	}
	return nil
}
