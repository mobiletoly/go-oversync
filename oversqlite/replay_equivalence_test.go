package oversqlite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayEquivalentJSON_RFC3339InstantsWithDifferentOffsetsMatch(t *testing.T) {
	equal, err := replayEquivalentJSON(
		`{"created_at":"2026-03-24T18:42:11Z","updated_at":"2026-03-24T18:42:11.123456Z"}`,
		`{"created_at":"2026-03-24T20:42:11+02:00","updated_at":"2026-03-24T21:42:11.123456+03:00"}`,
	)
	require.NoError(t, err)
	require.True(t, equal)
}

func TestReplayEquivalentJSON_NaiveTimestampTextDoesNotMatchZonedInstant(t *testing.T) {
	equal, err := replayEquivalentJSON(
		`{"created_at":"2026-03-24 18:42:11"}`,
		`{"created_at":"2026-03-24T18:42:11+02:00"}`,
	)
	require.NoError(t, err)
	require.False(t, equal)
}
