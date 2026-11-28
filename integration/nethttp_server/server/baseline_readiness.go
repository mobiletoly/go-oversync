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

// PrepareBaselineReadiness installs the nethttp integration server's static
// application contract. Repository helpers sharing its database must use this
// function so every process expects the same durable identity.
func PrepareBaselineReadiness(ctx context.Context, pool *pgxpool.Pool, cfg *oversync.ServiceConfig) error {
	return prepareBaselineReadinessManifest(ctx, pool, cfg, baselineReadinessManifest)
}

func prepareBaselineReadinessManifest(ctx context.Context, pool *pgxpool.Pool, cfg *oversync.ServiceConfig, manifest []byte) error {
	return baselinereadiness.Prepare(ctx, pool, cfg, manifest)
}
