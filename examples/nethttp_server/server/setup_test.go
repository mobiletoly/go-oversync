package server

import (
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRegisteredTablesForBusinessSchema_UsesBusinessRichV0Contract(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("custom_business")

	require.Equal(t, []oversync.RegisteredTable{
		{Schema: "custom_business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "categories", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "teams", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "team_members", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "typed_rows", SyncKeyColumns: []string{"id"}},
	}, tables)
	for _, table := range tables {
		require.NotContains(t, table.Table, "blob_")
	}
}

func TestRegisteredTablesForBusinessSchema_DefaultsToBusinessSchema(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("")

	require.NotEmpty(t, tables)
	for _, table := range tables {
		require.Equal(t, "business", table.Schema)
	}
}

func TestConfiguredPoolSizeFromEnv_Defaults(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(50), maxConns)
	require.Equal(t, int32(5), minConns)
}

func TestConfiguredPoolSizeFromEnv_Overrides(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "120")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "12")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(120), maxConns)
	require.Equal(t, int32(12), minConns)
}

func TestConfiguredPoolSizeFromEnv_RejectsInvalidValues(t *testing.T) {
	t.Run("non-positive max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "0")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("negative min", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "-1")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("min exceeds max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "10")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "11")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})
}
