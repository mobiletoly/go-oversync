package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestDummySigninPreservesExactUserIdentity(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{})
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	defer ts.Close()

	for _, userID := range []string{"test-user", " test-user "} {
		body := map[string]string{"user": userID, "password": "any"}
		b, _ := json.Marshal(body)
		resp, err := http.Post(ts.URL()+"/dummy-signin", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("signin request failed: %v", err)
		}
		var out struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
			User      string `json:"user"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			_ = resp.Body.Close()
			t.Fatalf("decode response: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || out.Token == "" {
			t.Fatalf("unexpected signin response: status=%d token=%q", resp.StatusCode, out.Token)
		}
		claims, err := ts.Auth.ValidateToken(out.Token)
		if err != nil {
			t.Fatalf("token validation failed: %v", err)
		}
		if claims.Subject != userID || out.User != userID {
			t.Fatalf("identity changed: subject=%q response=%q want=%q", claims.Subject, out.User, userID)
		}
		if claims.ExpiresAt == nil || time.Until(claims.ExpiresAt.Time) <= 0 {
			t.Fatalf("token expired")
		}
	}
}
