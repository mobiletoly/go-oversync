package oversqlite

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile the documented decision API against the exported client surface.
var _ interface {
	PendingConflict(context.Context) (*PendingConflict, error)
	ResolveConflict(context.Context, PendingConflict, MergeResult) error
	RetryConflict(context.Context, PendingConflict) error
} = (*Client)(nil)

func TestConflictDocumentation_PreservingLifecycle(t *testing.T) {
	for _, path := range []string{"README.md", "../docs/documentation/advanced-concepts.md"} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		for _, name := range []string{
			"PendingConflict", "ResolveConflict", "RetryConflict", "default resolver",
			"ServerWinsResolver", "ClientWinsResolver", "server_row", "one transaction",
		} {
			require.Contains(t, string(data), name)
		}
		require.NotContains(t, string(data), "resolutions clear `_sync_outbox_*")
		require.NotContains(t, string(data), "exhaustion also clears")
	}
}
