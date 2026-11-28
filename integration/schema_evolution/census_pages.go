package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"time"

	"github.com/jackc/pgx/v5"
)

type censusProgress struct {
	Phase    string
	Sequence int64
	Scope    string
	Key      *string
	Hash     []byte
}

// initializeCensus freezes limits before the first population page. Re-entry
// compares the exact job contract; it never resets progress or extends time.
func (a *Authority) initializeCensus(ctx context.Context, b Bounds) error {
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var epoch int64
		var closed bool
		if err := tx.QueryRow(ctx, `SELECT closed,revision FROM app_evolution.barrier WHERE singleton FOR UPDATE`).Scan(&closed, &epoch); err != nil {
			return err
		}
		if !closed {
			return fmt.Errorf("census_requires_drain")
		}
		var existing bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.jobs WHERE job=$1)`, JobID).Scan(&existing); err != nil {
			return err
		}
		if existing {
			var frozen Bounds
			var actualEpoch int64
			var algorithm, revision string
			if err := tx.QueryRow(ctx, `SELECT bounds,epoch,algorithm,revision FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&frozen, &actualEpoch, &algorithm, &revision); err != nil {
				return err
			}
			if digest(encodeJSON(frozen)) != digest(encodeJSON(b)) || epoch != actualEpoch || algorithm != backfillRequirement.AlgorithmSHA256 || revision != JobRevision {
				return fmt.Errorf("immutable census contract changed")
			}
			return nil
		}
		if err := checkEvidenceCapacity(ctx, tx, b, 8192); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO app_evolution.jobs(job,revision,algorithm,transition_revision,phase,watermark,state,bounds,epoch,census) SELECT $1,$2,$3,$4,'maintaining',COALESCE(max(sequence),0),'census',$5,$6,'' FROM app_evolution.scope_inventory`, JobID, JobRevision, backfillRequirement.AlgorithmSHA256, TransitionRevision, b, epoch)
		if err != nil {
			return err
		}
		state, _ := sha256.New().(encoding.BinaryMarshaler).MarshalBinary()
		if _, err = tx.Exec(ctx, `INSERT INTO app_evolution.census_progress(job,phase,hash_state) VALUES($1,'scopes',$2)`, JobID, state); err != nil {
			return err
		}
		return checkEvidenceCapacity(ctx, tx, b, 0)
	})
}

func (a *Authority) captureCensusPage(ctx context.Context, b Bounds) (done bool, err error) {
	bounded, cancel := context.WithDeadline(ctx, minDeadline(time.Now().Add(b.QueryTimeout), b.Deadline))
	defer cancel()
	err = pgx.BeginTxFunc(bounded, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := lockAdmission(bounded, tx, true); err != nil {
			return err
		}
		var epoch, watermark int64
		var state string
		if err := tx.QueryRow(bounded, `SELECT epoch,watermark,state FROM app_evolution.jobs WHERE job=$1 FOR UPDATE`, JobID).Scan(&epoch, &watermark, &state); err != nil {
			return err
		}
		if state != "census" {
			done = true
			return nil
		}
		var closed bool
		var current int64
		if err := tx.QueryRow(bounded, `SELECT closed,revision FROM app_evolution.barrier WHERE singleton FOR SHARE`).Scan(&closed, &current); err != nil {
			return err
		}
		if !closed || current != epoch {
			return fmt.Errorf("census_epoch_changed")
		}
		var populationAdvanced bool
		if err := tx.QueryRow(bounded, `SELECT EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE sequence>$1)`, watermark).Scan(&populationAdvanced); err != nil {
			return err
		}
		if populationAdvanced {
			return fmt.Errorf("maintenance_census_changed")
		}
		var p censusProgress
		if err := tx.QueryRow(bounded, `SELECT phase,sequence,scope_id,row_key::text,hash_state FROM app_evolution.census_progress WHERE job=$1`, JobID).Scan(&p.Phase, &p.Sequence, &p.Scope, &p.Key, &p.Hash); err != nil {
			return err
		}
		h := sha256.New()
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(p.Hash); err != nil {
			return err
		}
		count, err := copyCensusPage(bounded, tx, b, epoch, watermark, &p, h)
		if err != nil {
			return err
		}
		done, err = advanceCensusPhase(bounded, tx, epoch, &p, h, count)
		if err != nil {
			return err
		}
		p.Hash, err = h.(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(bounded, `UPDATE app_evolution.census_progress SET phase=$2,sequence=$3,scope_id=$4,row_key=$5::uuid,hash_state=$6 WHERE job=$1`, JobID, p.Phase, p.Sequence, p.Scope, p.Key, p.Hash); err != nil {
			return err
		}
		return checkEvidenceCapacity(bounded, tx, b, 0)
	})
	return done, err
}

func copyCensusPage(ctx context.Context, tx pgx.Tx, b Bounds, epoch, watermark int64, p *censusProgress, h hash.Hash) (int, error) {
	query := `SELECT sequence::text,scope_id,incarnation::text,deleted::text FROM app_evolution.scope_inventory WHERE sequence>$1 AND sequence<=$2 ORDER BY sequence LIMIT $3`
	args := []any{p.Sequence, watermark, b.ScopePage}
	if p.Phase == "rows" {
		query = `SELECT p._sync_scope_id,p.id::text,''::text,''::text FROM main.upgrade_people p JOIN app_evolution.census_scopes s ON s.epoch=$4 AND s.scope_id=p._sync_scope_id AND NOT s.deleted WHERE ($1::uuid IS NULL OR (p._sync_scope_id COLLATE "C",p.id)>($2 COLLATE "C",$1::uuid)) ORDER BY p._sync_scope_id COLLATE "C",p.id LIMIT $3`
		args = []any{p.Key, p.Scope, b.Rows, epoch}
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	var page [][4]string
	for rows.Next() {
		var f [4]string
		if err := rows.Scan(&f[0], &f[1], &f[2], &f[3]); err != nil {
			rows.Close()
			return 0, err
		}
		page = append(page, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var reserve int64
	for _, f := range page {
		reserve += 256
		for _, v := range f {
			reserve += int64(len(v))
		}
	}
	if err := checkEvidenceCapacity(ctx, tx, b, reserve); err != nil {
		return 0, err
	}
	for _, f := range page {
		if p.Phase == "scopes" {
			if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.census_scopes VALUES($1,$2::bigint,$3,$4::uuid,$5::boolean)`, epoch, f[0], f[1], f[2], f[3]); err != nil {
				return 0, err
			}
			if err := tx.QueryRow(ctx, `SELECT $1::bigint`, f[0]).Scan(&p.Sequence); err != nil {
				return 0, err
			}
		} else {
			if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.census_rows VALUES($1,$2,$3::uuid)`, epoch, f[0], f[1]); err != nil {
				return 0, err
			}
			p.Scope = f[0]
			key := f[1]
			p.Key = &key
		}
		for _, v := range f {
			_ = binary.Write(h, binary.BigEndian, uint64(len(v)))
			_, _ = h.Write([]byte(v))
		}
	}
	return len(page), nil
}

func finalizeCensus(ctx context.Context, tx pgx.Tx, epoch int64, digest string) error {
	// Admission stayed closed across every page. The final comparison also detects
	// privileged population drift rather than publishing an incomplete inventory.
	if err := verifyCensusPopulation(ctx, tx, epoch); err != nil {
		return err
	}
	var unsettled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.scope_state WHERE state_code=1) OR EXISTS(SELECT 1 FROM main.upgrade_people p WHERE NOT EXISTS(SELECT 1 FROM app_evolution.scope_inventory i WHERE i.scope_id=p._sync_scope_id AND NOT i.deleted))`).Scan(&unsettled); err != nil {
		return err
	}
	if unsettled {
		return fmt.Errorf("census_population_unsettled")
	}
	_, err := tx.Exec(ctx, `UPDATE app_evolution.jobs SET census=$2,state='paused' WHERE job=$1 AND state='census'`, JobID, digest)
	return err
}

func advanceCensusPhase(ctx context.Context, tx pgx.Tx, epoch int64, p *censusProgress, h hash.Hash, count int) (bool, error) {
	if count != 0 {
		return false, nil
	}
	if p.Phase == "scopes" {
		p.Phase = "rows"
		return false, nil
	}
	if err := finalizeCensus(ctx, tx, epoch, hex.EncodeToString(h.Sum(nil))); err != nil {
		return false, err
	}
	p.Phase = "complete"
	return true, nil
}
