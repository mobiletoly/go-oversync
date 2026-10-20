// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ScopeTransactionKind describes the SDK-owned origin of a scoped mutation.
// ServerWrite is available only to trusted in-process writers, never a client
// source supplied through Connect, PushOneShot or WithinSyncBundle.
type ScopeTransactionKind string

const (
	ScopeTransactionConnect        ScopeTransactionKind = "connect"
	ScopeTransactionClientPush     ScopeTransactionKind = "client_push"
	ScopeTransactionServerWrite    ScopeTransactionKind = "server_write"
	ScopeTransactionSnapshotCreate ScopeTransactionKind = "snapshot_create"
	ScopeTransactionSnapshotRetire ScopeTransactionKind = "snapshot_retire"
)

// ScopeTransactionOperation contains exact, SDK-derived scope and source IDs.
// Kind distinguishes a trusted server writer from a client-controlled source.
type ScopeTransactionOperation struct {
	ScopeID  string
	Kind     ScopeTransactionKind
	SourceID string
}

// ScopeTransactionAdmission admits a mutation inside its actual transaction,
// before any scoped initialization, receipt replay or mutation. Implementations
// must be SQL-only and transaction-pure, obeying DatabaseWriteTx's restrictions.
// They may lock application lifecycle state until this transaction finishes;
// they must use the supplied context and capability, never start another SDK
// operation or produce nontransactional effects. Every transaction retry is
// admitted again. The application owns lifecycle policy and its persistence.
type ScopeTransactionAdmission interface {
	AdmitScopeTransaction(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error
}

// ScopeAccessDeniedError denies a scoped mutation without exposing policy data.
type ScopeAccessDeniedError struct{}

func (*ScopeAccessDeniedError) Error() string { return "scope access denied" }

// ScopeAdmissionUnavailableError fails closed when admission cannot decide.
// Err is available to trusted callers; Error and the HTTP envelope are safe.
type ScopeAdmissionUnavailableError struct{ Err error }

func (*ScopeAdmissionUnavailableError) Error() string { return "scope admission unavailable" }
func (e *ScopeAdmissionUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (s *SyncService) admitScopeTransaction(ctx context.Context, tx pgx.Tx, operation ScopeTransactionOperation) error {
	if s.config.ScopeTransactionAdmission == nil {
		return nil
	}
	callbackCtx, capability, err := newRestrictedDatabaseWriteTx(ctx, tx, operation.ScopeID)
	if err == nil {
		err = s.config.ScopeTransactionAdmission.AdmitScopeTransaction(callbackCtx, capability, operation)
	}
	if err == nil {
		return nil
	}
	var denied *ScopeAccessDeniedError
	if errors.As(err, &denied) {
		return &ScopeAccessDeniedError{}
	}
	return &ScopeAdmissionUnavailableError{Err: err}
}
