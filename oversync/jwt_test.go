package oversync

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTAuth_GenerateToken(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")
	userID := "test-user-123"
	deviceID := "test-device-456"
	duration := time.Hour

	token, err := jwtAuth.GenerateToken(userID, deviceID, duration)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	if token == "" {
		t.Error("Generated token should not be empty")
	}

	// Verify the token can be parsed
	claims, err := jwtAuth.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate generated token: %v", err)
	}

	if claims.DeviceID != deviceID {
		t.Errorf("Expected device_id %s, got %s", deviceID, claims.DeviceID)
	}

	if claims.Subject != userID {
		t.Errorf("Expected user_id %s, got %s", userID, claims.Subject)
	}

	// Verify token expiration
	if claims.ExpiresAt == nil {
		t.Error("Token should have expiration time")
	}

	expectedExpiry := time.Now().Add(duration)
	actualExpiry := claims.ExpiresAt.Time
	timeDiff := actualExpiry.Sub(expectedExpiry).Abs()

	if timeDiff > time.Second {
		t.Errorf("Token expiry time differs by more than 1 second: expected ~%v, got %v", expectedExpiry, actualExpiry)
	}
}

func TestJWTAuth_ValidateToken_Success(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")
	userID := "test-user-456"
	deviceID := "test-device-789"

	// Generate a valid token
	token, err := jwtAuth.GenerateToken(userID, deviceID, time.Hour)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	// Validate the token
	claims, err := jwtAuth.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token: %v", err)
	}

	if claims.DeviceID != deviceID {
		t.Errorf("Expected device_id %s, got %s", deviceID, claims.DeviceID)
	}

	if claims.Subject != userID {
		t.Errorf("Expected user_id %s, got %s", userID, claims.Subject)
	}

	if claims.Issuer != "go-oversync" {
		t.Errorf("Expected issuer 'go-oversync', got %s", claims.Issuer)
	}

	if claims.Subject != userID {
		t.Errorf("Expected subject %s, got %s", userID, claims.Subject)
	}
}

func TestJWTAuth_ValidateToken_InvalidSecret(t *testing.T) {
	jwtAuth1 := NewJWTAuth("secret-1")
	jwtAuth2 := NewJWTAuth("secret-2")

	// Generate token with first secret
	token, err := jwtAuth1.GenerateToken("test-user", "test-device", time.Hour)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	// Try to validate with different secret
	_, err = jwtAuth2.ValidateToken(token)
	if err == nil {
		t.Error("Expected validation to fail with different secret")
	}
}

func TestJWTAuth_ValidateToken_ExpiredToken(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")

	// Generate token with very short expiration
	token, err := jwtAuth.GenerateToken("test-user", "test-device", time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	// Wait for token to expire
	time.Sleep(10 * time.Millisecond)

	// Try to validate expired token
	_, err = jwtAuth.ValidateToken(token)
	if err == nil {
		t.Error("Expected validation to fail for expired token")
	}
}

func TestJWTAuth_ValidateToken_MalformedToken(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")

	testCases := []struct {
		name  string
		token string
	}{
		{"empty token", ""},
		{"invalid format", "not.a.jwt"},
		{"random string", "random-string"},
		{"partial token", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := jwtAuth.ValidateToken(tc.token)
			if err == nil {
				t.Errorf("Expected validation to fail for %s", tc.name)
			}
		})
	}
}

func TestJWTAuth_ValidateToken_WrongSigningMethod(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")

	// Create token with wrong signing method (RS256 instead of HS256)
	claims := &JWTClaims{
		DeviceID: "test-device",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   "test-user",
		},
	}

	// This would normally require RSA keys, but we're just testing the validation
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tokenString, _ := token.SigningString()

	// Try to validate token with wrong signing method
	_, err := jwtAuth.ValidateToken(tokenString)
	if err == nil {
		t.Error("Expected validation to fail for wrong signing method")
	}
}

func TestJWTAuth_ValidateToken_MissingDeviceID(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret")

	// Create token without device_id
	claims := &JWTClaims{
		DeviceID: "", // Empty device ID
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   "test-user",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtAuth.secret)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}

	// Try to validate token without device_id
	_, err = jwtAuth.ValidateToken(tokenString)
	if err == nil {
		t.Error("Expected validation to fail for missing device_id")
	}
}

func TestJWTAuth_TokenRoundTrip(t *testing.T) {
	jwtAuth := NewJWTAuth("test-secret-roundtrip")

	testCases := []struct {
		userID   string
		deviceID string
		duration time.Duration
	}{
		{"user-1", "device-1", time.Hour},
		{"user-with-special-chars-@#$", "device-special", 30 * time.Minute},
		{"very-long-user-id", "very-long-device-id-with-many-characters", 24 * time.Hour},
		{"123", "456", time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.userID+"-"+tc.deviceID, func(t *testing.T) {
			// Generate token
			token, err := jwtAuth.GenerateToken(tc.userID, tc.deviceID, tc.duration)
			if err != nil {
				t.Fatalf("Failed to generate token: %v", err)
			}

			// Validate token
			claims, err := jwtAuth.ValidateToken(token)
			if err != nil {
				t.Fatalf("Failed to validate token: %v", err)
			}

			// Verify user ID and device ID match
			if claims.Subject != tc.userID {
				t.Errorf("user ID mismatch: expected %s, got %s", tc.userID, claims.Subject)
			}
			if claims.DeviceID != tc.deviceID {
				t.Errorf("device ID mismatch: expected %s, got %s", tc.deviceID, claims.DeviceID)
			}

			// Verify token is not expired
			if claims.ExpiresAt != nil && claims.ExpiresAt.Before(time.Now()) {
				t.Error("Token should not be expired immediately after generation")
			}
		})
	}
}
