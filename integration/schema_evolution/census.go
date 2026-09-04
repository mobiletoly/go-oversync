package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/mobiletoly/go-oversync/oversync"
)

// Query results are streamed in stable order; no complete population is retained
// in Go or aggregated into a PostgreSQL JSON/string value.
func censusDigest(ctx context.Context, r oversync.DatabaseReadinessReader, epoch int64) (string, error) {
	h := sha256.New()
	for _, query := range []string{
		`SELECT sequence::text,scope_id,incarnation::text,deleted::text FROM app_evolution.census_scopes WHERE epoch=$1 ORDER BY sequence`,
		`SELECT scope_id,row_key::text,''::text,''::text FROM app_evolution.census_rows WHERE epoch=$1 ORDER BY scope_id COLLATE "C",row_key`,
	} {
		rows, err := r.Query(ctx, query, epoch)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var fields [4]string
			if err := rows.Scan(&fields[0], &fields[1], &fields[2], &fields[3]); err != nil {
				rows.Close()
				return "", err
			}
			for _, field := range fields {
				_ = binary.Write(h, binary.BigEndian, uint64(len(field)))
				_, _ = h.Write([]byte(field))
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyCensus(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	var epoch int64
	var expected string
	if err := r.QueryRow(ctx, `SELECT epoch,census FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&epoch, &expected); err != nil {
		return err
	}
	if err := verifyCensusPopulation(ctx, r, epoch); err != nil {
		return err
	}
	actual, err := censusDigest(ctx, r, epoch)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("maintenance_census_identity_changed")
	}
	return nil
}

type maintenanceCertificate struct {
	oversync.BackfillCertificate
	Epoch  int64
	Census string
}

func verifyCertificateFence(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	var cert maintenanceCertificate
	if err := r.QueryRow(ctx, `SELECT certificate FROM app_evolution.certificates ORDER BY certificate_id DESC LIMIT 1`).Scan(&cert); err != nil {
		return err
	}
	var epoch int64
	var census string
	if err := r.QueryRow(ctx, `SELECT j.epoch,j.census FROM app_evolution.jobs j JOIN app_evolution.maintenance m ON m.epoch=j.epoch WHERE j.job=$1`, JobID).Scan(&epoch, &census); err != nil {
		return err
	}
	if cert.Epoch != epoch || cert.Census != census || cert.TransitionPhase != "coexistence" {
		return fmt.Errorf("certificate_maintenance_identity_changed")
	}
	return nil
}

func verifyCensusPopulation(ctx context.Context, r oversync.DatabaseReadinessReader, epoch int64) error {
	var drift bool
	if err := r.QueryRow(ctx, `SELECT
 EXISTS((SELECT sequence,scope_id,incarnation,deleted FROM app_evolution.scope_inventory EXCEPT SELECT sequence,scope_id,incarnation,deleted FROM app_evolution.census_scopes WHERE epoch=$1) UNION ALL (SELECT sequence,scope_id,incarnation,deleted FROM app_evolution.census_scopes WHERE epoch=$1 EXCEPT SELECT sequence,scope_id,incarnation,deleted FROM app_evolution.scope_inventory))
 OR EXISTS((SELECT _sync_scope_id,id FROM main.upgrade_people EXCEPT SELECT scope_id,row_key FROM app_evolution.census_rows WHERE epoch=$1) UNION ALL (SELECT scope_id,row_key FROM app_evolution.census_rows WHERE epoch=$1 EXCEPT SELECT _sync_scope_id,id FROM main.upgrade_people))`, epoch).Scan(&drift); err != nil {
		return err
	}
	if drift {
		return fmt.Errorf("maintenance_census_changed")
	}

	return nil
}
