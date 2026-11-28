package oversync

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OperationAdmissionAdapter extends startup evidence with an application-owned
// durable admission fence. Admit must acquire its shared transaction lock before
// checking closure. Core retains that lock until the operation returns. The
// controller closes admission first, then waits for admitted work to drain.
// Applications must also fence SQL writers that do not enter a core operation.
type OperationAdmissionAdapter interface {
	Admit(context.Context, DatabaseReadinessReader) error
}

type operationAdmissionKey struct{}
type operationAdmission struct {
	mu       sync.Mutex
	service  *SyncService
	admitted bool
	release  func()
}

// ReadinessPoolPair contains caller-owned execution and admission pools created
// from one immutable pgx configuration. Its unexported provenance prevents an
// independently assembled pair from being used as equivalence evidence.
type ReadinessPoolPair struct {
	execution  *pgxpool.Pool
	admission  *pgxpool.Pool
	provenance *struct{}
}

// NewReadinessPoolPair creates the execution and bounded admission pools from
// the same copied connection configuration. Only admission pool sizing differs.
func NewReadinessPoolPair(ctx context.Context, base *pgxpool.Config, admissionMaxConns int32) (*ReadinessPoolPair, error) {
	if base == nil {
		return nil, fmt.Errorf("readiness pool config is required")
	}
	if admissionMaxConns < 1 {
		return nil, fmt.Errorf("admission pool max connections must be positive")
	}
	if prohibitedPoolHook(base) {
		return nil, fmt.Errorf("readiness pools require declarative session initialization; connection hooks are prohibited")
	}
	executionConfig := base.Copy()
	admissionConfig := base.Copy()
	admissionConfig.MaxConns = admissionMaxConns
	admissionConfig.MinConns = 0
	admissionConfig.MinIdleConns = 0
	execution, err := pgxpool.NewWithConfig(ctx, executionConfig)
	if err != nil {
		return nil, err
	}
	admission, err := pgxpool.NewWithConfig(ctx, admissionConfig)
	if err != nil {
		execution.Close()
		return nil, err
	}
	return &ReadinessPoolPair{execution: execution, admission: admission, provenance: &struct{}{}}, nil
}

// ExecutionPool returns the caller-owned execution pool.
func (p *ReadinessPoolPair) ExecutionPool() *pgxpool.Pool {
	if p == nil {
		return nil
	}
	return p.execution
}

// AdmissionPool returns the caller-owned bounded admission pool.
func (p *ReadinessPoolPair) AdmissionPool() *pgxpool.Pool {
	if p == nil {
		return nil
	}
	return p.admission
}

// Close closes both pools. It is safe to call more than once.
func (p *ReadinessPoolPair) Close() {
	if p == nil {
		return
	}
	if p.admission != nil {
		p.admission.Close()
	}
	if p.execution != nil {
		p.execution.Close()
	}
}

func prohibitedPoolHook(c *pgxpool.Config) bool {
	if c == nil || c.ConnConfig == nil {
		return true
	}
	for _, hook := range []any{c.BeforeConnect, c.AfterConnect, c.BeforeAcquire, c.PrepareConn, c.AfterRelease, c.ShouldPing, c.ConnConfig.Config.ValidateConnect, c.ConnConfig.Config.AfterConnect} {
		if !reflect.ValueOf(hook).IsNil() {
			return true
		}
	}
	return false
}

func (c *ReadinessConfig) admissionPool() *pgxpool.Pool {
	if c == nil || c.PoolPair == nil {
		return nil
	}
	return c.PoolPair.admission
}

func validateAdmissionPool(pool *pgxpool.Pool, cfg *ReadinessConfig) error {
	if cfg == nil {
		return nil
	}
	if _, ok := cfg.Adapter.(OperationAdmissionAdapter); !ok {
		return nil
	}
	pair := cfg.PoolPair
	if pair == nil || pair.provenance == nil || pair.execution == nil || pair.admission == nil || pair.execution == pair.admission || pool != pair.execution {
		return fmt.Errorf("operation admission requires a proven distinct bounded readiness pool pair")
	}
	if cfg.AdmissionPool != nil {
		return fmt.Errorf("operation admission does not accept an independently supplied admission pool")
	}
	if prohibitedPoolHook(pair.execution.Config()) || prohibitedPoolHook(pair.admission.Config()) {
		return fmt.Errorf("readiness pools require declarative session initialization; connection hooks are prohibited")
	}
	return nil
}

func releaseStreamingAdmission(ctx context.Context, s *SyncService) {
	if admission, ok := ctx.Value(operationAdmissionKey{}).(*operationAdmission); ok {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		if admission.service == s && admission.release != nil {
			admission.release()
			// Further API calls on this context must obtain their own admission lease.
			admission.service = nil
			admission.admitted = false
		}
	}
}

func (s *SyncService) beginOperation(contexts ...context.Context) (func(), error) {
	done, err := s.beginLocalOperation()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if len(contexts) > 0 {
		ctx = contexts[0]
	}
	admission, _ := ctx.Value(operationAdmissionKey{}).(*operationAdmission)
	if admission.isAdmitted(s) {
		return done, nil
	}
	if s.config == nil || s.config.Readiness == nil {
		return done, nil
	}
	adapter, ok := s.config.Readiness.Adapter.(OperationAdmissionAdapter)
	if !ok {
		return done, nil
	}
	release, err := s.acquireOperationAdmission(ctx, adapter)
	if err != nil {
		done()
		return nil, errServiceNotReady
	}
	var releaseOnce, doneOnce sync.Once
	releaseLease := func() { releaseOnce.Do(release) }
	admission.setLease(s, releaseLease)
	return func() { releaseLease(); doneOnce.Do(done) }, nil
}

func (s *SyncService) acquireOperationAdmission(ctx context.Context, adapter OperationAdmissionAdapter) (release func(), err error) {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := s.config.Readiness.admissionPool().Acquire(bounded)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(bounded, syncMutationTxOptions())
	if err != nil {
		conn.Release()
		return nil, err
	}
	reader := &readinessReader{ctx: bounded, tx: tx, remaining: 32}
	release = func() { reader.close(); cleanupReadinessTransaction(conn, tx); conn.Release() }
	defer func() {
		if recover() != nil {
			err = errServiceNotReady
		}
		if err != nil {
			release()
			release = nil
		}
	}()
	err = attestOriginSession(bounded, tx)
	if err == nil {
		err = adapter.Admit(bounded, reader)
	}
	reader.close()
	return release, err
}

func (a *operationAdmission) isAdmitted(s *SyncService) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.service == s && a.admitted
}
func (a *operationAdmission) setLease(s *SyncService, release func()) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.service == s {
		a.admitted = true
		a.release = release
	}
}
