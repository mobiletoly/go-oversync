package exampleauth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type contextUserIDKey struct{}

type TokenAuth struct {
	secret []byte
}

type Claims struct {
	jwt.RegisteredClaims
}

func New(secret string) *TokenAuth {
	return &TokenAuth{secret: []byte(secret)}
}

func (a *TokenAuth) GenerateToken(userID string, expiration time.Duration) (string, error) {
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "go-oversync-examples",
			Subject:   strings.TrimSpace(userID),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(a.secret)
}

func (a *TokenAuth) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return nil, fmt.Errorf("missing sub (user ID) in token")
	}
	return claims, nil
}

func (a *TokenAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}
		bearerToken := strings.Split(authHeader, " ")
		if len(bearerToken) != 2 || bearerToken[0] != "Bearer" {
			http.Error(w, "Invalid authorization header format", http.StatusUnauthorized)
			return
		}
		claims, err := a.ValidateToken(bearerToken[1])
		if err != nil {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUserID(r.Context(), claims.Subject)))
	})
}

func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, contextUserIDKey{}, strings.TrimSpace(userID))
}

func UserIDFromContext(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(contextUserIDKey{}).(string)
	return strings.TrimSpace(userID), ok
}
