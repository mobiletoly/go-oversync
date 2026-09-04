package server

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/baselinereadiness"
	"github.com/mobiletoly/go-oversync/oversync"
)

//go:embed baseline-readiness.json
var baselineReadinessManifest []byte

func prepareBaselineReadiness(ctx context.Context, pool *pgxpool.Pool, cfg *oversync.ServiceConfig) error {
	return baselinereadiness.Prepare(ctx, pool, cfg, baselineReadinessManifest)
}
