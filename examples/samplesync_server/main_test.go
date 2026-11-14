package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewHTTPServer_UsesStreamingSafeTimeouts(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer(":0", handler)

	require.Equal(t, 10*time.Second, server.ReadHeaderTimeout)
	require.Equal(t, 120*time.Second, server.ReadTimeout, "a trickled push body must not hold the push-body budget forever")
	require.Equal(t, 2*time.Minute, server.IdleTimeout)
	require.Equal(t, 1<<20, server.MaxHeaderBytes)
	require.Zero(t, server.WriteTimeout, "SSE watches must not be terminated by a global write timeout")
}
