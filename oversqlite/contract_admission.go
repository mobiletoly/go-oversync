package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) fetchDiscoveredContract(ctx context.Context) ([]byte, error) {
	if err := validateSourceID(c.sourceID); err != nil {
		return nil, err
	}
	return withRetryValue(ctx, c.normalizedRetryPolicy(), "generation_discovery", func() ([]byte, error) {
		endpoint := strings.TrimRight(c.BaseURL, "/") + "/sync/generations/" + strconv.FormatInt(int64(c.config.GenerationID), 10)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		token, err := c.Token(ctx)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set(oversync.SourceIDHeader, c.sourceID)
		req.Header.Set("Accept-Encoding", "gzip")
		response, err := c.executeBoundedRequest(ctx, "generation_discovery", req, snapshotControlBodyLimit, snapshotControlBodyLimit, nil)
		if err != nil {
			return nil, err
		}
		if response.statusCode != http.StatusOK {
			return nil, decodeDiscoveryError(response.statusCode, response.body, c.config.GenerationID)
		}
		return response.body, nil
	})
}

// Admission runs on the existing client under its write mutex. All network
// operations finish before acquiring the SQLite writer for final validation.
func (c *Client) ensureAdmittedContract(ctx context.Context) (bool, error) {
	if c.localGeneration != nil {
		return true, nil
	}
	if c.config.GenerationID < 1 {
		return false, fmt.Errorf("server contract is not installed")
	}
	raw, err := c.fetchDiscoveredContract(ctx)
	if err != nil {
		return false, err
	}
	response, err := decodeDiscoveredContract(raw, c.config.GenerationID)
	if err != nil {
		return false, err
	}
	local, err := deriveLocalContract(ctx, c.db, c.config.GenerationID, c.config.Tables, response.SchemaGeneration)
	if err != nil {
		return false, err
	}
	compiled, err := compileLocalContract(local)
	if err != nil {
		return false, err
	}
	config := cloneCoordinatorClientConfig(c.config)
	config.localContract, config.generationProof, config.discovery = &local, local.Proof, response
	validator := &Client{db: c.db, BaseURL: c.BaseURL, Token: c.Token, HTTP: c.HTTP, config: config, sourceID: c.sourceID, schemaGenerationProof: local.Proof, localGeneration: compiled, discovery: response, pkByTable: c.pkByTable, keyByTable: c.keyByTable, tableOrder: c.tableOrder, tableInfo: c.tableInfo}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return false, err
	}
	if response.Limits.MaxRowDecodedBytes > int64(^uint(0)>>1) {
		return false, fmt.Errorf("discovery max_row_decoded_bytes exceeds local integer range")
	}
	if err := c.installAdmittedContract(ctx, validator, string(canonical), int(response.Limits.MaxRowDecodedBytes)); err != nil {
		return false, err
	}
	c.config, c.localGeneration, c.schemaGenerationProof, c.discovery = config, compiled, local.Proof, response
	return true, nil
}

func (c *Client) installAdmittedContract(ctx context.Context, validator *Client, descriptor string, maxRowBytes int) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	shape, err := managedPhysicalShape(ctx, tx, c.config.Tables)
	if err != nil {
		return err
	}
	config := cloneCoordinatorClientConfig(c.config)
	if err := readRetainedContract(ctx, tx, config, shape); err != nil {
		return err
	}
	if config.localContract != nil {
		return fmt.Errorf("contract was already admitted during discovery")
	}
	if err := validator.validatePendingCapturedValues(ctx, tx, maxRowBytes); err != nil {
		return err
	}
	for _, table := range c.config.Tables {
		schema := validator.schemaForTable(table.TableName)
		for _, metadata := range []string{"_sync_dirty_rows", "_sync_row_state", "_sync_managed_tables"} {
			if _, err := tx.ExecContext(ctx, "UPDATE "+metadata+" SET schema_name=? WHERE table_name=? AND schema_name=?", schema, table.TableName, unconfirmedLocalSchema); err != nil {
				return err
			}
		}
		info, err := c.getTableInfoTx(tx, table.TableName)
		if err != nil {
			return err
		}
		// Verify ownership under the unconfirmed namespace before replacing SQL.
		if err := createTriggersForTableWithInfo(tx, unconfirmedLocalSchema, table, info); err != nil {
			return err
		}
		if err := dropManagedTriggersForTableInTx(ctx, tx, table.TableName); err != nil {
			return err
		}
		if err := createTriggersForTableWithInfo(tx, schema, table, info); err != nil {
			return err
		}
	}
	proof := validator.schemaGenerationProof
	if _, err := tx.ExecContext(ctx, `INSERT INTO _sync_admitted_contracts(generation_id,fingerprint,descriptor,descriptor_hash) VALUES(?,?,?,?)`, proof.ID, proof.Fingerprint, descriptor, contractBytesHash(descriptor)); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE _sync_contract_identity SET fingerprint=? WHERE singleton_key=1 AND fingerprint=''`, proof.Fingerprint)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("contract admission identity changed")
	}
	upgradeIdentityExists, err := sqliteNamedTableExistsNoCase(ctx, tx, "_sync_upgrade_identity")
	if err != nil {
		return err
	}
	if upgradeIdentityExists {
		var generation int64
		var fingerprint string
		var ready int
		err := tx.QueryRowContext(ctx, `
			SELECT generation_id,generation_fingerprint,ready
			FROM _sync_upgrade_identity WHERE singleton_key=1
		`).Scan(&generation, &fingerprint, &ready)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			if generation != int64(proof.ID) || ready != 1 || fingerprint != "" && fingerprint != proof.Fingerprint {
				return fmt.Errorf("coordinated-opening identity changed during contract admission")
			}
			if fingerprint == "" {
				result, err := tx.ExecContext(ctx, `
					UPDATE _sync_upgrade_identity
					SET generation_fingerprint=?
					WHERE singleton_key=1 AND generation_id=? AND generation_fingerprint='' AND ready=1
				`, proof.Fingerprint, proof.ID)
				if err != nil {
					return err
				}
				changed, err := result.RowsAffected()
				if err != nil || changed != 1 {
					return fmt.Errorf("coordinated-opening identity changed during contract admission")
				}
			}
		}
	}
	if err := bindLocalBaselineContext(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) validatePendingCapturedValues(ctx context.Context, tx *immediateTx, maxRowBytes int) error {
	if maxRowBytes < 1 {
		return fmt.Errorf("server must advertise a positive payload bound")
	}
	var oversized int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM _sync_dirty_rows WHERE length(CAST(payload AS BLOB))>? OR length(CAST(key_json AS BLOB))>?)`, maxRowBytes, maxRowBytes).Scan(&oversized); err != nil {
		return err
	}
	if oversized != 0 {
		return fmt.Errorf("pending local row exceeds the server payload bound")
	}
	rows, err := tx.QueryContext(ctx, `SELECT table_name,key_json,payload FROM _sync_dirty_rows ORDER BY rowid`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var table, key string
		var payload sql.NullString
		if err := rows.Scan(&table, &key, &payload); err != nil {
			return err
		}
		if _, _, err := c.decodeDirtyKeyForPush(tx, table, key); err != nil {
			return err
		}
		if !payload.Valid {
			continue
		}
		value, err := jcs.DecodeObject([]byte(payload.String))
		if err != nil {
			return err
		}
		if err := c.validateAndUnwrapCapturedPayload(table, value); err != nil {
			return err
		}
		if err := c.processPayloadWithDeclaredCodecs(table, value); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Discovery errors identify the selected ID without inventing a fingerprint.
func decodeDiscoveryError(status int, raw []byte, selected oversync.SchemaGenerationID) error {
	if status != http.StatusConflict && status != http.StatusUpgradeRequired && status != http.StatusBadRequest {
		return &retryHTTPError{Operation: "generation_discovery", StatusCode: status}
	}
	if err := jcs.ValidateStructure(raw); err != nil {
		return err
	}
	_, err := requiredContractFields(raw, "error", "message", "retryable", "remediation", "schema_generation_id")
	if err != nil {
		return err
	}
	var response struct {
		oversync.GenerationErrorResponse
		GenerationID string `json:"schema_generation_id"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.GenerationID != strconv.FormatInt(int64(selected), 10) || response.Retryable || response.Message == "" {
		return fmt.Errorf("invalid generation discovery error context")
	}
	expectedStatus, remediation := 0, ""
	switch response.Error {
	case string(oversync.SchemaGenerationUnknown):
		expectedStatus, remediation = http.StatusConflict, "upgrade_or_reconfigure_client"
	case string(oversync.SchemaGenerationRetired):
		expectedStatus, remediation = http.StatusUpgradeRequired, "upgrade_application"
	case "invalid_generation_discovery_request":
		expectedStatus, remediation = http.StatusBadRequest, "correct_request"
	}
	if status != expectedStatus || response.Remediation != remediation {
		return fmt.Errorf("invalid generation discovery error code, status or remediation")
	}
	return &oversync.SchemaGenerationRequestError{Code: oversync.SchemaGenerationRequestErrorCode(response.Error), GenerationID: selected, Message: response.Error}
}
