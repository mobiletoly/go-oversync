// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JWTAuth handles JWT authentication
type JWTAuth struct {
	secret []byte
}

// NewJWTAuth creates a new JWT authenticator
func NewJWTAuth(secret string) *JWTAuth {
	return &JWTAuth{
		secret: []byte(secret),
	}
}

// JWTClaims represents JWT claims for single-user multi-device sync
type JWTClaims struct {
	DeviceID string `json:"did"` // Device ID (becomes source_id)
	jwt.RegisteredClaims
}

// GenerateToken generates a JWT token for single-user multi-device sync
func (j *JWTAuth) GenerateToken(userID, deviceID string, expiration time.Duration) (string, error) {
	claims := &JWTClaims{
		DeviceID: deviceID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "go-oversync",
			Subject:   userID, // User ID goes in standard 'sub' claim
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(j.secret)
}

// ValidateToken validates a JWT token and returns the claims
func (j *JWTAuth) ValidateToken(tokenString string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return j.secret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		// Additional validation per auth spec
		if claims.DeviceID == "" {
			return nil, fmt.Errorf("missing did (device ID) in token")
		}
		if claims.Subject == "" {
			return nil, fmt.Errorf("missing sub (user ID) in token")
		}
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}

// Middleware returns an HTTP middleware for JWT authentication
func (j *JWTAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		bearerToken := strings.Split(authHeader, " ")
		if len(bearerToken) != 2 || bearerToken[0] != "Bearer" {
			http.Error(w, "Invalid authorization header format", http.StatusUnauthorized)
			return
		}

		claims, err := j.ValidateToken(bearerToken[1])
		if err != nil {
			// Safely log token prefix (max 20 chars)
			tokenPrefix := bearerToken[1]
			if len(tokenPrefix) > 20 {
				tokenPrefix = tokenPrefix[:20]
			}
			slog.Error("JWT validation failed", "error", err, "token_prefix", tokenPrefix)
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		r = r.WithContext(ContextWithActor(r.Context(), Actor{
			UserID:   claims.Subject,
			SourceID: claims.DeviceID,
		}))

		next.ServeHTTP(w, r)
	})
}
