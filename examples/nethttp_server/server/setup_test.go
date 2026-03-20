package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
