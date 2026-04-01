package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestDummySigninGeneratesJWT(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{})
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	defer ts.Close()

	body := map[string]string{
		"user":     "test-user",
		"password": "any",
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(ts.URL()+"/dummy-signin", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("signin request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	var out struct {
		Token     string `json:"token"`
		ExpiresIn int64  `json:"expires_in"`
		User      string `json:"user"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Token == "" {
		t.Fatalf("empty token")
	}
	claims, err := ts.Auth.ValidateToken(out.Token)
	if err != nil {
		t.Fatalf("token validation failed: %v", err)
	}
	if claims.Subject != "test-user" {
		t.Fatalf("unexpected sub: %s", claims.Subject)
	}
	if claims.ExpiresAt == nil || time.Until(claims.ExpiresAt.Time) <= 0 {
		t.Fatalf("token expired")
	}
}
