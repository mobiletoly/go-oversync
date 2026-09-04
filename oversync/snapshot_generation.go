// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

func validateSnapshotSessionGeneration(storedID SchemaGenerationID, storedFingerprint string, proof SchemaGenerationProof) error {
	if storedID < 1 {
		return &SessionGenerationError{Code: SessionGenerationStateCorrupt, SessionKind: "snapshot", RequestedGenerationID: proof.ID, Field: "schema_generation_id", Message: "snapshot session generation state is corrupt"}
	}
	if !isLowerHexFingerprint(storedFingerprint) {
		return &SessionGenerationError{Code: SessionGenerationStateCorrupt, SessionKind: "snapshot", RequestedGenerationID: proof.ID, StoredGenerationID: storedID, Field: "schema_generation_fingerprint", Message: "snapshot session generation state is corrupt"}
	}
	if proof.ID < storedID {
		return sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, storedID, "", "schema generation downgrade is forbidden")
	}
	if proof.ID != storedID || proof.Fingerprint != storedFingerprint {
		return &SessionGenerationError{Code: SessionGenerationMismatch, SessionKind: "snapshot", RequestedGenerationID: proof.ID, StoredGenerationID: storedID, Message: "snapshot session uses another schema generation"}
	}
	return nil
}
