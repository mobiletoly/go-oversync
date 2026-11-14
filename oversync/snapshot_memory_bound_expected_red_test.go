//go:build oversync_expected_red

package oversync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSnapshotMaterializationHasNoFullSnapshotCollections is the opt-in
// structural probe introduced expected-red in Phase 0. Phase 1 turns it green
// by requiring bounded keyset/COPY seams and rejecting snapshot-wide results/maps.
func TestSnapshotMaterializationHasNoFullSnapshotCollections(t *testing.T) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve expected-red probe source path")
	}
	sourcePath := filepath.Join(filepath.Dir(currentFile), "snapshot_sessions.go")
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", sourcePath, err)
	}

	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "materializeSnapshotRows" {
			target = fn
			break
		}
	}
	if target == nil {
		t.Fatal("materializeSnapshotRows declaration not found")
	}

	fullSnapshotSliceResult := false
	if target.Type.Results != nil {
		for _, field := range target.Type.Results.List {
			if _, ok := field.Type.(*ast.ArrayType); ok {
				fullSnapshotSliceResult = true
			}
		}
	}
	mapAllocations := 0
	ast.Inspect(target.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok || ident.Name != "make" {
			return true
		}
		switch call.Args[0].(type) {
		case *ast.MapType:
			mapAllocations++
		}
		return true
	})

	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read %s: %v", sourcePath, err)
	}
	text := string(source)
	requiredBoundedSeams := []string{
		"snapshotMaterializationBatchRows",
		"snapshotMaterializationBatchBytes",
		`limitPlaceholder := "$4"`,
		`limitPlaceholder = "$5"`,
		"LIMIT %s",
		"tx.CopyFrom",
	}
	var missing []string
	for _, seam := range requiredBoundedSeams {
		if !strings.Contains(text, seam) {
			missing = append(missing, seam)
		}
	}
	if fullSnapshotSliceResult || mapAllocations > 0 || len(missing) > 0 {
		t.Fatalf(
			"expected bounded materialization, observed full_snapshot_slice_result=%t map_allocations=%d missing_bounded_seams=%v",
			fullSnapshotSliceResult,
			mapAllocations,
			missing,
		)
	}
}
