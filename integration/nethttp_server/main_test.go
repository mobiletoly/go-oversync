package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewHTTPServer_UsesStreamingSafeTimeouts(t *testing.T) {
	server := newHTTPServer(":0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	require.Equal(t, 10*time.Second, server.ReadHeaderTimeout)
	require.Equal(t, 120*time.Second, server.ReadTimeout)
	require.Equal(t, 120*time.Second, server.IdleTimeout)
	require.Zero(t, server.WriteTimeout, "SSE watches must not be terminated by a global write timeout")
}
