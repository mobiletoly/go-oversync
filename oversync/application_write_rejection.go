package oversync

import (
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5/pgconn"
)

// ApplicationWriteRejection declares a safe public category for an application
// SQLSTATE. It contains no conversion logic or callback. The declaration is part
// of the immutable deployment evidence, separate from generation fingerprints.
type ApplicationWriteRejection struct{ SQLState, Category string }

type ApplicationWriteRejectedError struct{ Category string }

func (e *ApplicationWriteRejectedError) Error() string {
	return "application write rejected: " + e.Category
}

var applicationSQLState = regexp.MustCompile(`^P[0-9A-Z]{4}$`)
var applicationErrorCategory = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func validateApplicationWriteRejections(entries []ApplicationWriteRejection) error {
	seen := map[string]bool{}
	for _, entry := range entries {
		if !applicationSQLState.MatchString(entry.SQLState) || !applicationErrorCategory.MatchString(entry.Category) || seen[entry.SQLState] {
			return readinessFailure(DeploymentContractMismatch)
		}
		seen[entry.SQLState] = true
	}
	return nil
}

func (s *SyncService) classifyApplicationWriteError(err error) error {
	if s == nil || s.config == nil || s.config.Readiness == nil {
		return err
	}
	var rollback *WriteRollbackUnconfirmedError
	var commit *CommitOutcomeUnknownError
	if errors.As(err, &rollback) || errors.As(err, &commit) {
		return err
	}
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return err
	}
	for _, entry := range s.config.Readiness.Expected.WriteRejections {
		if entry.SQLState == pgerr.Code {
			return &ApplicationWriteRejectedError{Category: entry.Category}
		}
	}
	return err
}
