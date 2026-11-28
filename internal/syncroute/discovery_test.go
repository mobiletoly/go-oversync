package syncroute

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsCanonicalGenerationDiscovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "minimum", method: http.MethodGet, path: "/sync/generations/1", want: true},
		{name: "maximum", method: http.MethodGet, path: "/sync/generations/2147483647", want: true},
		{name: "mounted prefix", method: http.MethodGet, path: "/tenant/acme/sync/generations/7", want: true},
		{name: "wrong method", method: http.MethodPost, path: "/sync/generations/1"},
		{name: "bare missing generation", method: http.MethodGet, path: "/sync/generations"},
		{name: "missing generation", method: http.MethodGet, path: "/sync/generations/"},
		{name: "leading zero", method: http.MethodGet, path: "/sync/generations/01"},
		{name: "zero", method: http.MethodGet, path: "/sync/generations/0"},
		{name: "negative", method: http.MethodGet, path: "/sync/generations/-1"},
		{name: "overflow", method: http.MethodGet, path: "/sync/generations/2147483648"},
		{name: "suffix", method: http.MethodGet, path: "/sync/generations/1/other"},
		{name: "encoded digit", method: http.MethodGet, path: "/sync/generations/%31"},
		{name: "unrelated", method: http.MethodGet, path: "/sync/pull"},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, test.path, nil)
			require.Equal(t, test.want, IsCanonicalGenerationDiscovery(req))
		})
	}
}

func TestIsGenerationDiscoveryPathIncludesMalformedNamespaceRequests(t *testing.T) {
	for _, path := range []string{
		"/sync/generations", "/sync/generations/", "/sync/generations/01", "/sync/generations/1/other",
		"/tenant/acme/sync/generations/not-a-number",
	} {
		require.True(t, IsGenerationDiscoveryPath(httptest.NewRequest(http.MethodGet, path, nil)), path)
		require.False(t, IsGenerationDiscoveryPath(httptest.NewRequest(http.MethodPost, path, nil)), path)
	}
	require.False(t, IsGenerationDiscoveryPath(httptest.NewRequest(http.MethodGet, "/sync/pull", nil)))
}

func TestIsRootGenerationDiscoveryPathExcludesMountedAndWrongPrefixes(t *testing.T) {
	for _, path := range []string{
		"/sync/generations",
		"/sync/generations/",
		"/sync/generations//1",
		"/sync/generations/1",
	} {
		require.True(t, IsRootGenerationDiscoveryPath(httptest.NewRequest(http.MethodGet, path, nil)), path)
	}
	for _, path := range []string{
		"/tenant/acme/sync/generations/1",
		"/wrong/sync/generations/1",
		"/sync/pull",
	} {
		require.False(t, IsRootGenerationDiscoveryPath(httptest.NewRequest(http.MethodGet, path, nil)), path)
	}
	require.False(t, IsRootGenerationDiscoveryPath(httptest.NewRequest(http.MethodPost, "/sync/generations/1", nil)))
}
