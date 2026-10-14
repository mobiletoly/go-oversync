package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfiguredCapacityRows_OwnedProfileBounds(t *testing.T) {
	for _, raw := range []string{"", "50", "1000", "3399", "10000", "50000"} {
		rows, err := configuredCapacityRows(raw)
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, 50)
		require.LessOrEqual(t, rows, 50000)
	}
	for _, raw := range []string{"49", "50001", "-1", " 3399", "3399 ", "invalid", "9999999999999999999999"} {
		_, err := configuredCapacityRows(raw)
		require.Error(t, err)
	}
}
