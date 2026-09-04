package server

import (
	"bytes"
	"net/http"
	"testing"
)

func TestRuntimeResetEndpointIsNotExposed(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{})
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	defer ts.Close()

	resp, err := http.Post(ts.URL()+"/test/reset", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("reset request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected runtime reset endpoint to be absent, got status %d", resp.StatusCode)
	}
}
