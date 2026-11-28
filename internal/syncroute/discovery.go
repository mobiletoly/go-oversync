// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package syncroute

import (
	"net/http"
	"strconv"
	"strings"
)

const (
	generationDiscoveryNamespace = "/sync/generations"
	generationDiscoveryPrefix    = generationDiscoveryNamespace + "/"
)

// IsGenerationDiscoveryPath reports whether a GET targets the discovery
// namespace, including malformed paths that the handler must reject with the
// canonical protocol error instead of an operational-readiness response.
func IsGenerationDiscoveryPath(r *http.Request) bool {
	return r != nil && r.Method == http.MethodGet && r.URL != nil &&
		(r.URL.Path == generationDiscoveryNamespace || discoveryNamespaceOffset(r.URL.Path) >= 0)
}

// IsRootGenerationDiscoveryPath reports whether a GET targets the root
// discovery namespace, including its bare missing-generation form.
func IsRootGenerationDiscoveryPath(r *http.Request) bool {
	return IsGenerationDiscoveryPath(r) &&
		(r.URL.Path == generationDiscoveryNamespace || strings.HasPrefix(r.URL.Path, generationDiscoveryPrefix))
}

// IsCanonicalGenerationDiscovery reports whether the request can be routed
// around operational readiness to the authenticated discovery handler.
func IsCanonicalGenerationDiscovery(r *http.Request) bool {
	if r == nil || r.Method != http.MethodGet || r.URL == nil || r.URL.EscapedPath() != r.URL.Path {
		return false
	}
	generationID := r.PathValue("generation_id")
	if generationID == "" {
		offset := discoveryNamespaceOffset(r.URL.Path)
		if offset < 0 {
			return false
		}
		generationID = r.URL.Path[offset+len(generationDiscoveryPrefix):]
	}
	if generationID == "" || len(generationID) > 10 || generationID[0] == '0' {
		return false
	}
	for i := range len(generationID) {
		if generationID[i] < '0' || generationID[i] > '9' {
			return false
		}
	}
	value, err := strconv.ParseInt(generationID, 10, 32)
	return err == nil && value > 0
}

func discoveryNamespaceOffset(path string) int {
	if strings.HasPrefix(path, generationDiscoveryPrefix) {
		return 0
	}
	offset := strings.LastIndex(path, generationDiscoveryPrefix)
	if offset <= 0 {
		return -1
	}
	return offset
}
