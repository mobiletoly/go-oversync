package oversync

import "context"

type syncMaintenanceContextKey struct{}

func withSyncMaintenanceContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, syncMaintenanceContextKey{}, true)
}

func isSyncMaintenanceContext(ctx context.Context) bool {
	marked, _ := ctx.Value(syncMaintenanceContextKey{}).(bool)
	return marked
}
