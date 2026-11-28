package schemaevolution

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"io"
)

// ReadRolloutArtifact accepts one bounded, structurally unambiguous operator
// artifact. Unknown properties and duplicate keys cannot alter its interpretation.
func ReadRolloutArtifact(input io.Reader, target any) error {
	data, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("rollout input exceeds one MiB")
	}
	if err = jcs.ValidateStructure(data); err != nil {
		return fmt.Errorf("invalid rollout JSON structure")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid rollout artifact fields: %w", err)
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("rollout artifact must contain one JSON value")
	}
	return nil
}
