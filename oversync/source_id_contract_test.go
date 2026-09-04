package oversync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSnapshotSourceReplacement_PreservesExactVisibleASCII(t *testing.T) {
	actor := Actor{UserID: "user-1", SourceID: "old/source?token#part%25"}
	replacement, err := validateSnapshotSourceReplacement(actor, &SnapshotSourceReplacement{
		PreviousSourceID: actor.SourceID,
		NewSourceID:      "new/source?token#part%26",
		Reason:           "history_pruned",
	})
	require.NoError(t, err)
	require.Equal(t, actor.SourceID, replacement.PreviousSourceID)
	require.Equal(t, "new/source?token#part%26", replacement.NewSourceID)
}

func TestValidateSnapshotSourceReplacement_RejectsNormalizedOrNonASCIISourceIDs(t *testing.T) {
	for _, invalid := range []string{"", " source", "source ", "source\tvalue", "söurce"} {
		t.Run(invalid, func(t *testing.T) {
			_, err := validateSnapshotSourceReplacement(
				Actor{UserID: "user-1", SourceID: invalid},
				&SnapshotSourceReplacement{
					PreviousSourceID: invalid,
					NewSourceID:      "replacement",
					Reason:           "history_pruned",
				},
			)
			var invalidErr *SnapshotSessionInvalidError
			require.ErrorAs(t, err, &invalidErr)
			require.Equal(t, "previous_source_id is invalid", invalidErr.Error())
			if invalid != "" {
				require.NotContains(t, invalidErr.Error(), invalid)
			}
		})
	}
}

func TestValidateSnapshotSourceReplacement_RejectsReservedServerSources(t *testing.T) {
	service := &SyncService{config: &ServiceConfig{ReservedServerSourceIDs: []string{"server-writer"},
		RegisteredTableCatalogRevision: 1}}
	_, err := service.validateSnapshotSourceReplacement(
		Actor{UserID: "user-1", SourceID: "device-a"},
		&SnapshotSourceReplacement{
			PreviousSourceID: "device-a",
			NewSourceID:      "server-writer",
			Reason:           "history_pruned",
		},
	)
	var invalidErr *SnapshotSessionInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.NotContains(t, invalidErr.Error(), "server-writer")
	require.NotContains(t, invalidErr.Error(), "reserved")
}
