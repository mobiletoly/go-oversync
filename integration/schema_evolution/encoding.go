// Package schemaevolution is the mobile integration fixture's PostgreSQL
// migration and SQL backfill authority. It is not part of the Oversync SDK.
package schemaevolution

import (
	"crypto/sha256"
	"encoding/hex"
)

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
