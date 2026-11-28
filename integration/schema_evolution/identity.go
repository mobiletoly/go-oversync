package schemaevolution

import (
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"io/fs"
	"sort"
)

// Exact application source bytes bind algorithm/deployment identity. An updated
// worker cannot resume an old job by silently retaining a hand-written code hash.
//
//go:embed encoding.go run_capacity.go maintenance.go census.go census_pages.go backfill_repair.go descriptor.go backfill.go backfill_apply.go backfill_prepare.go lifecycle.go migrations.go readiness.go telemetry.go validation.go identity.go control.sql migrations/*.sql
var applicationSource embed.FS

func applicationSourceDigest(domain string) string {
	var paths []string
	_ = fs.WalkDir(applicationSource, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	hash := sha256.New()
	frame := func(data []byte) {
		_ = binary.Write(hash, binary.BigEndian, uint64(len(data)))
		_, _ = hash.Write(data)
	}
	frame([]byte(domain))
	for _, path := range paths {
		data, _ := applicationSource.ReadFile(path)
		frame([]byte(path))
		frame(data)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
