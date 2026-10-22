package main

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestFixtureMode_RejectUnknown(t *testing.T) {
	previous := os.Args
	os.Args = []string{"fixture", "--unknown"}
	t.Cleanup(func() { os.Args = previous })
	require.ErrorContains(t, run(), "unsupported local fixture mode")
}

func TestQualificationReadiness_PrivatePublication(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(root, 0700))
	path := filepath.Join(root, "ready.json")
	t.Setenv("OVERSYNC_QUALIFICATION_READY_FILE", path)
	t.Setenv("OVERSYNC_QUALIFICATION_READY_NONCE", strings.Repeat("1", 32))
	t.Setenv("OVERSQLITE_PROCESS_OWNER", "oversqlite-"+strings.Repeat("2", 32))
	t.Setenv("JWT_SECRET", "never-publish-this-secret")
	generation := map[string]any{"schema_generation": map[string]any{"id": 1, "fingerprint": strings.Repeat("a", 64)}}
	require.NoError(t, publishQualificationReadiness("categories", "http://127.0.0.1:1234", "owned_database", generation, nil))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal(data, &value))
	require.Equal(t, "categories", value["profile"])
	require.Equal(t, "owned_database", value["database"])
	require.Equal(t, strings.Repeat("1", 32), value["nonce"])
	require.Equal(t, float64(os.Getpid()), value["pid"])
	require.Equal(t, float64(syscall.Getpgrp()), value["pgid"])
	require.Equal(t, value["nonce"], value["reset_domain"])
	require.Len(t, value["fixture_sha256"], 64)
	require.NotContains(t, string(data), os.Getenv("JWT_SECRET"))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.Error(t, publishQualificationReadiness("activity", "http://127.0.0.1:9999", "other", generation, nil))
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, unchanged)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1, "atomic publication must remove its temporary name")
}

func TestQualificationReadiness_RejectUnsafeOrPartialPublication(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(root, 0700))
	path := filepath.Join(root, "ready.json")
	t.Setenv("OVERSYNC_QUALIFICATION_READY_FILE", path)
	t.Setenv("OVERSYNC_QUALIFICATION_READY_NONCE", "")
	t.Setenv("OVERSQLITE_PROCESS_OWNER", "oversqlite-"+strings.Repeat("2", 32))
	require.Error(t, publishQualificationReadiness("categories", "url", "database", map[string]any{}, nil))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.Error(t, writeQualificationReadiness("relative.json", map[string]any{}))
	public := filepath.Join(root, "public")
	require.NoError(t, os.Mkdir(public, 0755))
	require.Error(t, writeQualificationReadiness(filepath.Join(public, "ready.json"), map[string]any{}))
	link := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(root, link))
	require.Error(t, writeQualificationReadiness(filepath.Join(link, "ready.json"), map[string]any{}))
	require.Error(t, writeQualificationReadiness(path, map[string]any{"unencodable": make(chan int)}))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "partial JSON must never be published")
	sentinel := filepath.Join(root, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("unrelated"), 0600))
	require.NoError(t, os.Symlink(sentinel, path))
	require.Error(t, writeQualificationReadiness(path, map[string]any{}))
	unchanged, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "unrelated", string(unchanged))
}
