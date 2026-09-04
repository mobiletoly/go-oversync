// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLoadOrCreateOperationDeadline_PersistsBeforeAnyOversyncReceipt(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{JWTSecret: "operation-deadline-test-secret"})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	operationID := uuid.New()
	first, err := LoadOrCreateOperationDeadline(ctx, ts.Pool, ts.BusinessSchema, operationID, time.Hour)
	require.NoError(t, err)
	second, err := LoadOrCreateOperationDeadline(ctx, ts.Pool, ts.BusinessSchema, operationID, 2*time.Hour)
	require.NoError(t, err)
	require.Equal(t, first, second)

	var receiptExists bool
	require.NoError(t, ts.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM sync.scope_write_receipts
			WHERE operation_id = $1
		)
	`, operationID).Scan(&receiptExists))
	require.False(t, receiptExists)
}
