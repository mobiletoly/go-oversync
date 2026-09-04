package oversqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

// Automatic retry cannot change a retired generation's immutable proof. Stop
// this loop without deleting rows, clearing intents, rotating sources or resetting
// cursors. The application chooses a preserving upgrade with explicit conflicts.
func (c *Client) stopForRetiredGeneration(err error) bool {
	var generation *oversync.SchemaGenerationRequestError
	if !errors.As(err, &generation) || generation.Code != oversync.SchemaGenerationRetired {
		return false
	}
	proof := c.schemaGenerationProof
	proof.ID = generation.GenerationID
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if persistErr := c.recordApplicationActionRequired(persistCtx, proof); persistErr != nil {
		c.logger.Error("failed to persist retired-generation application action", "error", persistErr)
	}
	c.logger.Error("automatic sync stopped; upgrade application and preserve local work", "generation_id", generation.GenerationID)
	return true
}

func (c *Client) recordApplicationActionRequired(ctx context.Context, proof oversync.SchemaGenerationProof) error {
	if c == nil || c.db == nil {
		return fmt.Errorf("client database is required")
	}
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _sync_action_required(singleton_key,action,generation_id,generation_fingerprint,observed_at)
		VALUES(1,'upgrade_application',?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(singleton_key) DO UPDATE SET
			action=excluded.action,
			generation_id=excluded.generation_id,
			generation_fingerprint=excluded.generation_fingerprint,
			observed_at=excluded.observed_at
	`, proof.ID, proof.Fingerprint)
	if err != nil {
		return fmt.Errorf("persist application action required: %w", err)
	}
	return nil
}

// ApplicationActionRequired reports durable local action state without making a
// network request. A retirement recorded for an older generation does not gate a
// client that has completed a preserving upgrade to a different proof.
func (c *Client) ApplicationActionRequired(ctx context.Context) (ApplicationActionRequired, error) {
	if c == nil || c.db == nil {
		return ApplicationActionRequired{}, fmt.Errorf("client database is required")
	}
	var action ApplicationAction
	var proof oversync.SchemaGenerationProof
	err := c.db.QueryRowContext(ctx, `
		SELECT action,generation_id,generation_fingerprint
		FROM _sync_action_required WHERE singleton_key=1
	`).Scan(&action, &proof.ID, &proof.Fingerprint)
	if err != nil {
		return ApplicationActionRequired{}, fmt.Errorf("load application action required: %w", err)
	}
	if action == ApplicationActionNone || proof != c.schemaGenerationProof {
		return ApplicationActionRequired{}, nil
	}
	if action != ApplicationActionUpgrade {
		return ApplicationActionRequired{}, fmt.Errorf("invalid persisted application action %q", action)
	}
	return ApplicationActionRequired{Action: action, Generation: proof}, nil
}
