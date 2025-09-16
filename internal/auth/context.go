// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
)

type contextKey string

const (
	sourceIDKey contextKey = "source_id"
	userIDKey   contextKey = "user_id"
)

// SetSourceID sets the source ID in the context
func SetSourceID(ctx context.Context, sourceID string) context.Context {
	return context.WithValue(ctx, sourceIDKey, sourceID)
}

// GetSourceID retrieves the source ID from the context
func GetSourceID(ctx context.Context) (string, bool) {
	sourceID, ok := ctx.Value(sourceIDKey).(string)
	return sourceID, ok
}

// SetUserID sets the user ID in the context
func SetUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// GetUserID retrieves the user ID from the context
func GetUserID(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(userIDKey).(string)
	return userID, ok
}

// SetAuthContext sets both user and source ID in context (simplified version)
func SetAuthContext(ctx context.Context, userID, sourceID, _ string, _ []string) context.Context {
	ctx = SetUserID(ctx, userID)
	ctx = SetSourceID(ctx, sourceID)
	return ctx
}
