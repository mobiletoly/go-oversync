package oversqlite

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientHTTPRedirectPolicyRejectsSameOriginBeforeForwardingSecrets(t *testing.T) {
	var seen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		seen.Add(1)
	}))
	defer server.Close()

	client := &Client{HTTP: &http.Client{}}
	req, err := http.NewRequest(http.MethodGet, server.URL+"/start", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Oversync-Source-ID", "source-1")
	req.Header.Set("Oversync-Schema-Generation-ID", "1")
	req.Header.Set("Oversync-Schema-Generation-Fingerprint", "fingerprint")

	response, err := client.doHTTP(req)
	if response != nil {
		response.Body.Close()
	}
	require.ErrorContains(t, err, "sync protocol redirects are not permitted")
	require.Zero(t, seen.Load())
}

func TestClientHTTPRedirectPolicyRejectsCrossOriginBeforeForwardingSecrets(t *testing.T) {
	var seen atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/secret", http.StatusFound)
	}))
	defer source.Close()

	client := &Client{HTTP: &http.Client{}}
	req, err := http.NewRequest(http.MethodGet, source.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Oversync-Source-ID", "source-1")
	req.Header.Set("Oversync-Schema-Generation-Fingerprint", "fingerprint")

	response, err := client.doHTTP(req)
	if response != nil {
		response.Body.Close()
	}
	require.ErrorContains(t, err, "sync protocol redirects are not permitted")
	require.Zero(t, seen.Load())
}

func TestClientHTTPRedirectPolicyRejectsDowngradeBeforeForwardingSecrets(t *testing.T) {
	var seen atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
	}))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/secret", http.StatusFound)
	}))
	defer source.Close()

	transport := source.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // test-only TLS fixture
	client := &Client{HTTP: &http.Client{Transport: transport}}
	req, err := http.NewRequest(http.MethodGet, source.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Oversync-Source-ID", "source-1")
	req.Header.Set("Oversync-Schema-Generation-Fingerprint", "fingerprint")

	response, err := client.doHTTP(req)
	if response != nil {
		response.Body.Close()
	}
	require.ErrorContains(t, err, "sync protocol redirects are not permitted")
	require.Zero(t, seen.Load())
}
