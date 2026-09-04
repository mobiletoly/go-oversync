// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const managedLayoutDifferenceLimit = 20

type managedLayoutFact struct {
	Kind      string
	Identity  string
	Attribute string
	Value     string
}

type managedLayoutDifference struct {
	Category  string
	Kind      string
	Identity  string
	Attribute string
	Expected  string
	Actual    string
}

type managedLayoutFactKey struct {
	Kind      string
	Identity  string
	Attribute string
}

var expectedManagedLayoutFactsByName = map[string][]managedLayoutFact{
	syncSchemaLayoutName: {
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "identity", Value: "a"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.capture_ordinal", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "type", Value: "pg_catalog.bytea"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.key_bytes", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "type", Value: "pg_catalog.int2"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.op_code", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "type", Value: "pg_catalog.jsonb"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.payload_db", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.txid", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_capture_stage.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "ordinal", Value: "8"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "type", Value: "pg_catalog.bytea"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_hash", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.byte_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "ordinal", Value: "9"},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "default", Value: "now()"},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "ordinal", Value: "10"},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "type", Value: "pg_catalog.timestamptz"},
		{Kind: "column", Identity: "sync.bundle_log.committed_at", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.planned_row_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.row_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.source_bundle_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_log.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_rows.bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "type", Value: "pg_catalog.bytea"},
		{Kind: "column", Identity: "sync.bundle_rows.key_bytes", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "type", Value: "pg_catalog.int2"},
		{Kind: "column", Identity: "sync.bundle_rows.op_code", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "type", Value: "pg_catalog.json"},
		{Kind: "column", Identity: "sync.bundle_rows.payload_wire", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_rows.row_ordinal", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.bundle_rows.table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.bundle_rows.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.meta.catalog_fingerprint", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.meta.catalog_revision", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.meta.layout_name", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.meta.next_table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.meta.protocol_label", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "type", Value: "pg_catalog.bool"},
		{Kind: "column", Identity: "sync.meta.singleton_key", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.row_state.bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "default", Value: "false"},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "type", Value: "pg_catalog.bool"},
		{Kind: "column", Identity: "sync.row_state.deleted", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "type", Value: "pg_catalog.bytea"},
		{Kind: "column", Identity: "sync.row_state.key_bytes", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.row_state.table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.row_state.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "type", Value: "pg_catalog.uuid"},
		{Kind: "column", Identity: "sync.scope_state.initialization_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "type", Value: "pg_catalog.timestamptz"},
		{Kind: "column", Identity: "sync.scope_state.initialized_at", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "type", Value: "pg_catalog.timestamptz"},
		{Kind: "column", Identity: "sync.scope_state.lease_expires_at", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "type", Value: "pg_catalog.int2"},
		{Kind: "column", Identity: "sync.scope_state.state_code", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.scope_state.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "type", Value: "pg_catalog.bytea"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.key_bytes", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "type", Value: "pg_catalog.json"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.payload_wire", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.row_ordinal", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "type", Value: "pg_catalog.uuid"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.snapshot_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_session_rows.wire_byte_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "default", Value: "0"},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "ordinal", Value: "8"},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_sessions.byte_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "ordinal", Value: "9"},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "type", Value: "pg_catalog.timestamptz"},
		{Kind: "column", Identity: "sync.snapshot_sessions.expires_at", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "default", Value: "0"},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_sessions.row_count", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_fingerprint", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.snapshot_sessions.schema_generation_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "type", Value: "pg_catalog.uuid"},
		{Kind: "column", Identity: "sync.snapshot_sessions.snapshot_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.snapshot_sessions.source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.snapshot_sessions.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_fingerprint", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.source_generation_bindings.schema_generation_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_generation_bindings.source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.source_generation_bindings.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "default", Value: "0"},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.source_state.max_committed_source_bundle_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "default", Value: "''::text"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "collation", Value: "pg_catalog.default"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "default", Value: "''::text"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_state.retirement_reason", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "collation", Value: "pg_catalog.default"},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.source_state.state", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.source_state.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "ordinal", Value: "7"},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.table_catalog.introduced_revision", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "not_null", Value: "false"},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "ordinal", Value: "8"},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.table_catalog.retired_revision", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.table_catalog.schema_name", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "ordinal", Value: "6"},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "type", Value: "pg_catalog.int2"},
		{Kind: "column", Identity: "sync.table_catalog.state_code", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_column", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "ordinal", Value: "5"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "type", Value: "pg_catalog.int2"},
		{Kind: "column", Identity: "sync.table_catalog.sync_key_kind", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "type", Value: "pg_catalog.int4"},
		{Kind: "column", Identity: "sync.table_catalog.table_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.table_catalog.table_name", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "default", Value: "1"},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "ordinal", Value: "3"},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.user_state.next_bundle_seq", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "default", Value: "0"},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "ordinal", Value: "4"},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.user_state.retained_bundle_floor", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "collation", Value: "pg_catalog.C"},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "identity", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "ordinal", Value: "2"},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "type", Value: "pg_catalog.text"},
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "typmod", Value: "-1"},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "collation", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "default", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "generated", Value: ""},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "identity", Value: "a"},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "not_null", Value: "true"},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "ordinal", Value: "1"},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "type", Value: "pg_catalog.int8"},
		{Kind: "column", Identity: "sync.user_state.user_pk", Attribute: "typmod", Value: "-1"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "columns", Value: "[\"op_code\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "expression", Value: "(op_code = ANY (ARRAY[1, 2, 3]))"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_op_code_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "columns", Value: "[\"op_code\",\"payload_db\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "expression", Value: "(((op_code = 3) AND (payload_db IS NULL)) OR ((op_code = ANY (ARRAY[1, 2])) AND (payload_db IS NOT NULL)))"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_payload_by_op_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "columns", Value: "[\"capture_ordinal\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "delete_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "referenced_columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "referenced_table", Value: "sync.table_catalog"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_table_id_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_capture_stage.bundle_capture_stage_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "columns", Value: "[\"user_pk\",\"bundle_seq\"]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "columns", Value: "[\"planned_row_count\"]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "expression", Value: "((planned_row_count IS NULL) OR (planned_row_count > 0))"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_planned_row_count_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "columns", Value: "[\"user_pk\",\"source_id\",\"source_bundle_id\"]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "type", Value: "u"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_source_tuple_key", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_log.bundle_log_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "columns", Value: "[\"user_pk\",\"bundle_seq\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "referenced_columns", Value: "[\"user_pk\",\"bundle_seq\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "referenced_table", Value: "sync.bundle_log"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_bundle_fk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "columns", Value: "[\"op_code\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "expression", Value: "(op_code = ANY (ARRAY[1, 2, 3]))"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_op_code_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "columns", Value: "[\"op_code\",\"payload_wire\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "expression", Value: "(((op_code = 3) AND (payload_wire IS NULL)) OR ((op_code = ANY (ARRAY[1, 2])) AND (payload_wire IS NOT NULL)))"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_payload_by_op_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "columns", Value: "[\"user_pk\",\"bundle_seq\",\"row_ordinal\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "delete_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "referenced_columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "referenced_table", Value: "sync.table_catalog"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_table_id_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.bundle_rows.bundle_rows_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "columns", Value: "[\"catalog_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "expression", Value: "(catalog_fingerprint ~ '^[0-9a-f]{64}$'::text)"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_fingerprint_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "columns", Value: "[\"catalog_revision\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "expression", Value: "(catalog_revision >= 1)"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_catalog_revision_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "columns", Value: "[\"layout_name\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "expression", Value: "(layout_name = 'server_postgres_sync_catalog_v1'::text)"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_layout_name_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "columns", Value: "[\"next_table_id\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "expression", Value: "((next_table_id >= 1) AND (next_table_id <= '2147483648'::bigint))"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_next_table_id_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "columns", Value: "[\"singleton_key\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "columns", Value: "[\"protocol_label\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "expression", Value: "(protocol_label = 'v1'::text)"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_protocol_label_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "columns", Value: "[\"singleton_key\"]"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "expression", Value: "singleton_key"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.meta.meta_singleton_key_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "columns", Value: "[\"user_pk\",\"table_id\",\"key_bytes\"]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.row_state.row_state_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "delete_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "referenced_columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "referenced_table", Value: "sync.table_catalog"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_table_id_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.row_state.row_state_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "columns", Value: "[\"state_code\"]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "expression", Value: "(state_code = ANY (ARRAY[0, 1, 2]))"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_code_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "columns", Value: "[\"state_code\",\"initializer_source_id\",\"initialization_id\",\"lease_expires_at\",\"initialized_at\",\"initialized_by_source_id\"]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "expression", Value: "(((state_code = 0) AND (initializer_source_id IS NULL) AND (initialization_id IS NULL) AND (lease_expires_at IS NULL) AND (initialized_at IS NULL) AND (initialized_by_source_id IS NULL)) OR ((state_code = 1) AND (initializer_source_id IS NOT NULL) AND (initialization_id IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (initialized_at IS NULL) AND (initialized_by_source_id IS NULL)) OR ((state_code = 2) AND (initializer_source_id IS NULL) AND (initialization_id IS NULL) AND (lease_expires_at IS NULL) AND (initialized_at IS NOT NULL) AND (initialized_by_source_id IS NOT NULL)))"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_fields_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.scope_state.scope_state_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "columns", Value: "[\"snapshot_id\",\"table_id\",\"key_bytes\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "type", Value: "u"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_logical_row_key", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "columns", Value: "[\"snapshot_id\",\"row_ordinal\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "columns", Value: "[\"snapshot_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "referenced_columns", Value: "[\"snapshot_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "referenced_table", Value: "sync.snapshot_sessions"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_snapshot_id_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "delete_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "referenced_columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "referenced_table", Value: "sync.table_catalog"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.snapshot_session_rows.snapshot_session_rows_table_id_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "columns", Value: "[\"schema_generation_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "expression", Value: "(schema_generation_fingerprint ~ '^[0-9a-f]{64}$'::text)"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_fingerprint_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "columns", Value: "[\"schema_generation_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "expression", Value: "((schema_generation_id >= 1) AND (schema_generation_id <= 2147483647))"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_generation_id_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "columns", Value: "[\"snapshot_id\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "columns", Value: "[\"user_pk\",\"source_id\",\"schema_generation_id\",\"schema_generation_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "referenced_columns", Value: "[\"user_pk\",\"source_id\",\"schema_generation_id\",\"schema_generation_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "referenced_table", Value: "sync.source_generation_bindings"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_source_generation_fk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.snapshot_sessions.snapshot_sessions_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "columns", Value: "[\"schema_generation_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "expression", Value: "(schema_generation_fingerprint ~ '^[0-9a-f]{64}$'::text)"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_fingerprint_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "columns", Value: "[\"schema_generation_id\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "expression", Value: "((schema_generation_id >= 1) AND (schema_generation_id <= 2147483647))"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_id_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "columns", Value: "[\"user_pk\",\"source_id\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "columns", Value: "[\"user_pk\",\"source_id\",\"schema_generation_id\",\"schema_generation_fingerprint\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "type", Value: "u"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_session_key", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "columns", Value: "[\"user_pk\",\"source_id\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "referenced_columns", Value: "[\"user_pk\",\"source_id\"]"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "referenced_table", Value: "sync.source_state"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.source_generation_bindings.source_generation_bindings_source_fk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "columns", Value: "[\"state\",\"replaced_by_source_id\",\"retirement_reason\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "expression", Value: "((state <> 'active'::text) OR ((replaced_by_source_id = ''::text) AND (retirement_reason = ''::text)))"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_active_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "columns", Value: "[\"max_committed_source_bundle_id\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "expression", Value: "(max_committed_source_bundle_id >= 0)"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_max_committed_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "columns", Value: "[\"user_pk\",\"source_id\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "columns", Value: "[\"state\",\"replaced_by_source_id\",\"retirement_reason\",\"max_committed_source_bundle_id\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "expression", Value: "((state <> 'reserved'::text) OR ((replaced_by_source_id = ''::text) AND (retirement_reason = ''::text) AND (max_committed_source_bundle_id = 0)))"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_reserved_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "columns", Value: "[\"state\",\"replaced_by_source_id\",\"retirement_reason\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "expression", Value: "((state <> 'retired'::text) OR ((replaced_by_source_id <> ''::text) AND (retirement_reason <> ''::text)))"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_retired_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "columns", Value: "[\"state\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "expression", Value: "(state = ANY (ARRAY['active'::text, 'reserved'::text, 'retired'::text]))"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.source_state.source_state_state_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "delete_action", Value: "c"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "match_type", Value: "s"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "referenced_columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "referenced_table", Value: "sync.user_state"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "type", Value: "f"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "update_action", Value: "a"},
		{Kind: "constraint", Identity: "sync.source_state.source_state_user_pk_fkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "columns", Value: "[\"introduced_revision\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "expression", Value: "(introduced_revision >= 1)"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_introduced_revision_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "columns", Value: "[\"schema_name\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "expression", Value: "(((octet_length(schema_name) >= 1) AND (octet_length(schema_name) <= 63)) AND (schema_name ~ '^[a-z0-9_]+$'::text))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_name_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "columns", Value: "[\"schema_name\",\"table_name\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "type", Value: "u"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_schema_table_key", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "columns", Value: "[\"state_code\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "expression", Value: "(state_code = ANY (ARRAY[1, 2]))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_code_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "columns", Value: "[\"state_code\",\"retired_revision\",\"introduced_revision\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "expression", Value: "(((state_code = 1) AND (retired_revision IS NULL)) OR ((state_code = 2) AND (retired_revision > introduced_revision)))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_state_revision_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "columns", Value: "[\"sync_key_column\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "expression", Value: "(((octet_length(sync_key_column) >= 1) AND (octet_length(sync_key_column) <= 63)) AND (sync_key_column ~ '^[a-z0-9_]+$'::text) AND (sync_key_column <> '_sync_scope_id'::text))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_column_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "columns", Value: "[\"sync_key_kind\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "expression", Value: "(sync_key_kind = ANY (ARRAY[1, 2]))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_sync_key_kind_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "columns", Value: "[\"table_id\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "expression", Value: "(table_id > 0)"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_id_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "columns", Value: "[\"table_name\"]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "expression", Value: "(((octet_length(table_name) >= 1) AND (octet_length(table_name) <= 63)) AND (table_name ~ '^[a-z0-9_]+$'::text))"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.table_catalog.table_catalog_table_name_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "columns", Value: "[\"next_bundle_seq\"]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "expression", Value: "(next_bundle_seq >= 1)"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_bundle_seq_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "columns", Value: "[\"user_pk\"]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "type", Value: "p"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_pkey", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "columns", Value: "[\"retained_bundle_floor\",\"next_bundle_seq\"]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "expression", Value: "((retained_bundle_floor >= 0) AND (retained_bundle_floor <= (next_bundle_seq - 1)))"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "type", Value: "c"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_retained_floor_chk", Attribute: "validated", Value: "true"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "columns", Value: "[\"user_id\"]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "deferrable", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "delete_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "expression", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "initially_deferred", Value: "false"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "match_type", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "referenced_columns", Value: "[]"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "referenced_table", Value: ""},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "type", Value: "u"},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "update_action", Value: " "},
		{Kind: "constraint", Identity: "sync.user_state.user_state_user_id_key", Attribute: "validated", Value: "true"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "configuration", Value: "[]"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "language", Value: "plpgsql"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "leakproof", Value: "false"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "parallel", Value: "u"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "return_type", Value: "pg_catalog.trigger"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "security_definer", Value: "false"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "source.sha256", Value: "195be52adb1d5fcb837ad953e22d701b3922eaebc708673a72c38b6b989a6b85"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "strict", Value: "false"},
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "volatility", Value: "v"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "configuration", Value: "[]"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "language", Value: "plpgsql"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "leakproof", Value: "false"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "parallel", Value: "u"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "return_type", Value: "pg_catalog.trigger"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "security_definer", Value: "false"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "source.sha256", Value: "dd695ca6102b95fd5439ae2129bd0088af74bc22e009120ab1f5c6a41c5e6f17"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "strict", Value: "false"},
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "volatility", Value: "v"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "configuration", Value: "[]"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "language", Value: "plpgsql"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "leakproof", Value: "false"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "parallel", Value: "u"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "return_type", Value: "pg_catalog.trigger"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "security_definer", Value: "false"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "source.sha256", Value: "d1045297ccb7adebb07dd8f4ef0606ac0d7523ae7e9a819b26b3c6140fa73dc2"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "strict", Value: "false"},
		{Kind: "function", Identity: "sync.guard_source_generation_binding()", Attribute: "volatility", Value: "v"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "configuration", Value: "[]"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "language", Value: "plpgsql"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "leakproof", Value: "false"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "parallel", Value: "u"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "return_type", Value: "pg_catalog.trigger"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "security_definer", Value: "false"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "source.sha256", Value: "7a0c29c716052a9458dd0dad9068276f998ed497598799f9178c208be0062c43"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "strict", Value: "false"},
		{Kind: "function", Identity: "sync.guard_table_catalog_identity()", Attribute: "volatility", Value: "v"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "configuration", Value: "[]"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "language", Value: "plpgsql"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "leakproof", Value: "false"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "parallel", Value: "u"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "return_type", Value: "pg_catalog.trigger"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "security_definer", Value: "false"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "source.sha256", Value: "4bd230187dbdfe8d93d5c88268764f9decfd21f043538a4087ce729c104af239"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "strict", Value: "false"},
		{Kind: "function", Identity: "sync.reject_registered_table_truncate()", Attribute: "volatility", Value: "v"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"txid\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"capture_ordinal\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "table", Value: "sync.bundle_capture_stage"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "unique", Value: "false"},
		{Kind: "index", Identity: "sync.bcs_tx_user_ordinal_idx", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"capture_ordinal\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "table", Value: "sync.bundle_capture_stage"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_capture_stage_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"bundle_seq\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "table", Value: "sync.bundle_log"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"source_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"source_bundle_id\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "table", Value: "sync.bundle_log"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"bundle_seq\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"row_ordinal\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "table", Value: "sync.bundle_rows"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.bundle_rows_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"singleton_key\", \"opclass\": \"pg_catalog.bool_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "table", Value: "sync.meta"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.meta_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"table_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"key_bytes\", \"opclass\": \"pg_catalog.bytea_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "table", Value: "sync.row_state"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.row_state_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"table_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"key_bytes\", \"opclass\": \"pg_catalog.bytea_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "predicate", Value: "(deleted = false)"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "table", Value: "sync.row_state"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "unique", Value: "false"},
		{Kind: "index", Identity: "sync.rs_user_live_snapshot_idx", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "table", Value: "sync.scope_state"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.scope_state_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"snapshot_id\", \"opclass\": \"pg_catalog.uuid_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"table_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"key_bytes\", \"opclass\": \"pg_catalog.bytea_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "table", Value: "sync.snapshot_session_rows"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_logical_row_key", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"snapshot_id\", \"opclass\": \"pg_catalog.uuid_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"row_ordinal\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "table", Value: "sync.snapshot_session_rows"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_session_rows_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"snapshot_id\", \"opclass\": \"pg_catalog.uuid_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "table", Value: "sync.snapshot_sessions"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.snapshot_sessions_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"source_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "table", Value: "sync.source_generation_bindings"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"source_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"schema_generation_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "attribute.4", Value: "{\"role\": \"key\", \"column\": \"schema_generation_fingerprint\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "attribute_count", Value: "4"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "key_count", Value: "4"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "table", Value: "sync.source_generation_bindings"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.source_generation_bindings_session_key", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"source_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "table", Value: "sync.source_state"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"expires_at\", \"opclass\": \"pg_catalog.timestamptz_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"snapshot_id\", \"opclass\": \"pg_catalog.uuid_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "table", Value: "sync.snapshot_sessions"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "unique", Value: "false"},
		{Kind: "index", Identity: "sync.ss_expires_at_idx", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"source_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"schema_generation_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "attribute.4", Value: "{\"role\": \"key\", \"column\": \"schema_generation_fingerprint\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "attribute_count", Value: "4"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "key_count", Value: "4"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "table", Value: "sync.snapshot_sessions"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "unique", Value: "false"},
		{Kind: "index", Identity: "sync.ss_source_generation_idx", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"snapshot_id\", \"opclass\": \"pg_catalog.uuid_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"table_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "attribute.3", Value: "{\"role\": \"key\", \"column\": \"key_bytes\", \"opclass\": \"pg_catalog.bytea_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "attribute_count", Value: "3"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "key_count", Value: "3"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "table", Value: "sync.snapshot_session_rows"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.ssr_snapshot_table_key_idx", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"table_id\", \"opclass\": \"pg_catalog.int4_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "table", Value: "sync.table_catalog"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"schema_name\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "attribute.2", Value: "{\"role\": \"key\", \"column\": \"table_name\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "attribute_count", Value: "2"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "key_count", Value: "2"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "table", Value: "sync.table_catalog"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.table_catalog_schema_table_key", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_pk\", \"opclass\": \"pg_catalog.int8_ops\", \"collation\": \"\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "primary", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "table", Value: "sync.user_state"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_pkey", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "access_method", Value: "btree"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "attribute.1", Value: "{\"role\": \"key\", \"column\": \"user_id\", \"opclass\": \"pg_catalog.text_ops\", \"collation\": \"pg_catalog.C\", \"descending\": false, \"expression\": \"\", \"nulls_first\": false}"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "attribute_count", Value: "1"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "exclusion", Value: "false"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "key_count", Value: "1"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "live", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "predicate", Value: ""},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "primary", Value: "false"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "ready", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "table", Value: "sync.user_state"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "unique", Value: "true"},
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "valid", Value: "true"},
		{Kind: "namespace", Identity: "sync", Attribute: "present", Value: "true"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.bundle_capture_stage", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.bundle_log", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.bundle_rows", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.meta", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.row_state", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.scope_state", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.snapshot_session_rows", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.snapshot_sessions", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.source_generation_bindings", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.source_state", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.table_catalog", Attribute: "row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "force_row_security", Value: "false"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "is_partition", Value: "false"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "kind", Value: "r"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "persistence", Value: "p"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "replica_identity", Value: "d"},
		{Kind: "relation", Identity: "sync.user_state", Attribute: "row_security", Value: "false"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "cache", Value: "1"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "cycle", Value: "false"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "data_type", Value: "pg_catalog.int8"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "increment", Value: "1"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "maximum", Value: "9223372036854775807"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "minimum", Value: "1"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "owned_by", Value: ""},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "persistence", Value: "p"},
		{Kind: "sequence", Identity: "sync.accepted_push_replay_seq", Attribute: "start", Value: "1"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "cache", Value: "1"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "cycle", Value: "false"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "data_type", Value: "pg_catalog.int8"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "increment", Value: "1"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "maximum", Value: "9223372036854775807"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "minimum", Value: "1"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "owned_by", Value: "sync.bundle_capture_stage.capture_ordinal"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "persistence", Value: "p"},
		{Kind: "sequence", Identity: "sync.bundle_capture_stage_capture_ordinal_seq", Attribute: "start", Value: "1"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "cache", Value: "1"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "cycle", Value: "false"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "data_type", Value: "pg_catalog.int8"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "increment", Value: "1"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "maximum", Value: "9223372036854775807"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "minimum", Value: "1"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "owned_by", Value: ""},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "persistence", Value: "p"},
		{Kind: "sequence", Identity: "sync.history_pruned_error_seq", Attribute: "start", Value: "1"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "cache", Value: "1"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "cycle", Value: "false"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "data_type", Value: "pg_catalog.int8"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "increment", Value: "1"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "maximum", Value: "9223372036854775807"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "minimum", Value: "1"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "owned_by", Value: ""},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "persistence", Value: "p"},
		{Kind: "sequence", Identity: "sync.rejected_registered_write_seq", Attribute: "start", Value: "1"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "cache", Value: "1"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "cycle", Value: "false"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "data_type", Value: "pg_catalog.int8"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "increment", Value: "1"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "maximum", Value: "9223372036854775807"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "minimum", Value: "1"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "owned_by", Value: "sync.user_state.user_pk"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "persistence", Value: "p"},
		{Kind: "sequence", Identity: "sync.user_state_user_pk_seq", Attribute: "start", Value: "1"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "arguments", Value: "[]"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "constraint", Value: ""},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "enabled", Value: "O"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "events", Value: "update"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "function", Value: "sync.guard_source_generation_binding()"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "level", Value: "row"},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "new_transition", Value: ""},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "old_transition", Value: ""},
		{Kind: "trigger", Identity: "sync.source_generation_bindings.source_generation_bindings_identity_guard", Attribute: "timing", Value: "before"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "arguments", Value: "[]"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "constraint", Value: ""},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "enabled", Value: "O"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "events", Value: "update,delete"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "function", Value: "sync.guard_table_catalog_identity()"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "level", Value: "row"},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "new_transition", Value: ""},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "old_transition", Value: ""},
		{Kind: "trigger", Identity: "sync.table_catalog.table_catalog_identity_guard", Attribute: "timing", Value: "before"},
	},
}

type registeredTableTriggerTargetInventoryRow struct {
	rootSchema   string
	rootTable    string
	targetSchema string
	targetTable  string
}

func loadRegisteredTableTriggerTargetInventory(
	ctx context.Context,
	q syncCatalogQuerier,
	registeredTables []RegisteredTable,
) ([]registeredTableTriggerTargetInventoryRow, error) {
	if len(registeredTables) == 0 {
		return nil, nil
	}
	schemas := make([]string, 0, len(registeredTables))
	tables := make([]string, 0, len(registeredTables))
	for _, table := range registeredTables {
		schemas = append(schemas, table.normalizedSchema())
		tables = append(tables, table.normalizedTable())
	}
	rows, err := q.Query(ctx, `
WITH RECURSIVE configured AS (
  SELECT DISTINCT schema_name, table_name
  FROM unnest(@schemas::text[], @tables::text[]) AS requested(schema_name, table_name)
), roots AS (
  SELECT configured.schema_name, configured.table_name, relation.oid AS root_oid
  FROM configured
  JOIN pg_namespace AS namespace ON namespace.nspname = configured.schema_name
  JOIN pg_class AS relation
    ON relation.relnamespace = namespace.oid
   AND relation.relname = configured.table_name
), relation_tree AS (
  SELECT schema_name, table_name, root_oid, root_oid AS target_oid
  FROM roots
  UNION ALL
  SELECT tree.schema_name, tree.table_name, tree.root_oid, inheritance.inhrelid
  FROM relation_tree AS tree
  JOIN pg_inherits AS inheritance ON inheritance.inhparent = tree.target_oid
)
SELECT
  tree.schema_name,
  tree.table_name,
  target_namespace.nspname,
  target.relname
FROM relation_tree AS tree
JOIN pg_class AS target ON target.oid = tree.target_oid
JOIN pg_namespace AS target_namespace ON target_namespace.oid = target.relnamespace
ORDER BY tree.schema_name, tree.table_name, target_namespace.nspname, target.relname
`, pgx.NamedArgs{"schemas": schemas, "tables": tables})
	if err != nil {
		return nil, fmt.Errorf("load registered trigger target inventory: %w", err)
	}
	defer rows.Close()
	targets := make([]registeredTableTriggerTargetInventoryRow, 0, len(registeredTables))
	for rows.Next() {
		var target registeredTableTriggerTargetInventoryRow
		if err := rows.Scan(&target.rootSchema, &target.rootTable, &target.targetSchema, &target.targetTable); err != nil {
			return nil, fmt.Errorf("scan registered trigger target inventory: %w", err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate registered trigger target inventory: %w", err)
	}
	return targets, nil
}

func appendExpectedTriggerFacts(
	facts []managedLayoutFact,
	identity string,
	function string,
	enabled string,
	timing string,
	level string,
	events string,
	arguments []string,
) []managedLayoutFact {
	if arguments == nil {
		arguments = []string{}
	}
	encodedArguments, err := json.Marshal(arguments)
	if err != nil {
		panic(fmt.Sprintf("encode compiled managed trigger arguments: %v", err))
	}
	for attribute, value := range map[string]string{
		"arguments":      string(encodedArguments),
		"constraint":     "",
		"enabled":        enabled,
		"events":         events,
		"function":       function,
		"level":          level,
		"new_transition": "",
		"old_transition": "",
		"timing":         timing,
	} {
		facts = append(facts, managedLayoutFact{
			Kind:      "trigger",
			Identity:  identity,
			Attribute: attribute,
			Value:     value,
		})
	}
	return facts
}

func (s *SyncService) expectedManagedLayoutFacts(
	ctx context.Context,
	q syncCatalogQuerier,
) ([]managedLayoutFact, error) {
	compiled, ok := expectedManagedLayoutFactsByName[syncSchemaLayoutName]
	if !ok {
		return nil, fmt.Errorf("no compiled managed layout manifest for %q", syncSchemaLayoutName)
	}
	facts := append([]managedLayoutFact(nil), compiled...)
	facts = append(facts, retryableWriteManagedLayoutFacts...)
	activeTables := s.activeRegisteredTableDeclarations()
	targets, err := loadRegisteredTableTriggerTargetInventory(ctx, q, activeTables)
	if err != nil {
		return nil, err
	}
	targetsByRoot := make(map[string][]registeredTableTriggerTargetInventoryRow, len(activeTables))
	for _, target := range targets {
		rootKey := Key(target.rootSchema, target.rootTable)
		targetsByRoot[rootKey] = append(targetsByRoot[rootKey], target)
	}
	for _, table := range activeTables {
		rootKey := table.normalizedKey()
		info, ok := s.registeredTableInfo[rootKey]
		if !ok {
			return nil, fmt.Errorf("registered table %s is missing runtime metadata for managed layout manifest", rootKey)
		}
		keyColumns := table.normalizedSyncKeyColumns()
		if len(keyColumns) != 1 {
			return nil, fmt.Errorf("registered table %s requires exactly one sync key column for managed layout manifest", rootKey)
		}
		rootIdentity := table.normalizedSchema() + "." + table.normalizedTable() + "."
		facts = appendExpectedTriggerFacts(
			facts,
			rootIdentity+registeredTableOwnerGuardTrigger,
			"sync.enforce_registered_row_owner()",
			"O",
			"before",
			"row",
			"insert,update,delete",
			nil,
		)
		facts = appendExpectedTriggerFacts(
			facts,
			rootIdentity+registeredTableCaptureTriggerName,
			"sync.capture_registered_row_change()",
			"O",
			"after",
			"row",
			"insert,update,delete",
			append(
				[]string{keyColumns[0], strconv.Itoa(int(info.syncKeyKind)), strconv.Itoa(int(info.tableID))},
				databaseJSONColumnNames(s.columnTypesForTable(table.normalizedSchema(), table.normalizedTable()))...,
			),
		)
		rootTargets := targetsByRoot[rootKey]
		if len(rootTargets) == 0 {
			return nil, fmt.Errorf("registered table %s has no trigger target inventory", rootKey)
		}
		for _, target := range rootTargets {
			facts = appendExpectedTriggerFacts(
				facts,
				target.targetSchema+"."+target.targetTable+"."+registeredTableTruncateGuardTrigger,
				"sync.reject_registered_table_truncate()",
				"O",
				"before",
				"statement",
				"truncate",
				[]string{table.normalizedSchema(), table.normalizedTable()},
			)
		}
	}
	return normalizeManagedLayoutFacts(facts)
}

func (s *SyncService) validateManagedLayout(ctx context.Context, q syncCatalogQuerier) error {
	expected, err := s.expectedManagedLayoutFacts(ctx, q)
	if err != nil {
		return err
	}
	actual, err := loadManagedLayoutFacts(ctx, q)
	if err != nil {
		return err
	}
	return validateManagedLayoutFacts(syncSchemaLayoutName, expected, actual)
}

// validateStaticManagedLayout validates the complete sync-owned manifest before catalog metadata
// or application relations are consulted. Registered-table triggers live outside the sync schema
// and are validated later, after the durable catalog has supplied their identities and arguments.
func validateStaticManagedLayout(ctx context.Context, q syncCatalogQuerier) error {
	compiled, ok := expectedManagedLayoutFactsByName[syncSchemaLayoutName]
	if !ok {
		return fmt.Errorf("no compiled managed layout manifest for %q", syncSchemaLayoutName)
	}
	expected, err := normalizeManagedLayoutFacts(append(append([]managedLayoutFact(nil), compiled...), retryableWriteManagedLayoutFacts...))
	if err != nil {
		return err
	}
	actual, err := loadManagedLayoutFacts(ctx, q)
	if err != nil {
		return err
	}
	staticActual := make([]managedLayoutFact, 0, len(actual))
	for _, fact := range actual {
		if fact.Identity == "sync" || strings.HasPrefix(fact.Identity, "sync.") {
			staticActual = append(staticActual, fact)
		}
	}
	return validateManagedLayoutFacts(syncSchemaLayoutName, expected, staticActual)
}

func lockExistingManagedLayoutRelations(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, `
SELECT namespace.nspname, relation.relname
FROM pg_class AS relation
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'sync'
  AND relation.relkind IN ('r', 'p')
ORDER BY namespace.nspname, relation.relname
`)
	if err != nil {
		return fmt.Errorf("load managed relations for bootstrap locking: %w", err)
	}
	var relations []string
	for rows.Next() {
		var schemaName, relationName string
		if err := rows.Scan(&schemaName, &relationName); err != nil {
			rows.Close()
			return fmt.Errorf("scan managed relation for bootstrap locking: %w", err)
		}
		relations = append(relations, pgx.Identifier{schemaName, relationName}.Sanitize())
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate managed relations for bootstrap locking: %w", err)
	}
	rows.Close()
	for _, relation := range relations {
		if _, err := tx.Exec(ctx, "LOCK TABLE "+relation+" IN SHARE ROW EXCLUSIVE MODE"); err != nil {
			return fmt.Errorf("lock managed relation %s: %w", relation, err)
		}
	}
	return nil
}

func (s *SyncService) validateManagedLayoutVolatileFacts(ctx context.Context, q syncCatalogQuerier) error {
	expected, err := s.expectedManagedLayoutFacts(ctx, q)
	if err != nil {
		return err
	}
	actual, err := loadManagedLayoutVolatileFacts(ctx, q)
	if err != nil {
		return err
	}
	filter := func(facts []managedLayoutFact) []managedLayoutFact {
		filtered := make([]managedLayoutFact, 0)
		for _, fact := range facts {
			if fact.Kind == "function" || fact.Kind == "trigger" {
				filtered = append(filtered, fact)
			}
		}
		return filtered
	}
	return validateManagedLayoutFacts(syncSchemaLayoutName, filter(expected), filter(actual))
}

func managedLayoutFactLess(left, right managedLayoutFact) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.Identity != right.Identity {
		return left.Identity < right.Identity
	}
	if left.Attribute != right.Attribute {
		return left.Attribute < right.Attribute
	}
	return left.Value < right.Value
}

func normalizeManagedLayoutFacts(facts []managedLayoutFact) ([]managedLayoutFact, error) {
	normalized := append([]managedLayoutFact(nil), facts...)
	sort.Slice(normalized, func(i, j int) bool {
		return managedLayoutFactLess(normalized[i], normalized[j])
	})
	for i := 1; i < len(normalized); i++ {
		previous := normalized[i-1]
		current := normalized[i]
		if previous.Kind == current.Kind && previous.Identity == current.Identity && previous.Attribute == current.Attribute {
			return nil, fmt.Errorf(
				"duplicate managed layout fact %s %s attribute %s",
				current.Kind,
				current.Identity,
				current.Attribute,
			)
		}
	}
	return normalized, nil
}

func managedLayoutFingerprint(facts []managedLayoutFact) (string, error) {
	normalized, err := normalizeManagedLayoutFacts(facts)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	var length [8]byte
	for _, fact := range normalized {
		for _, field := range [...]string{fact.Kind, fact.Identity, fact.Attribute, fact.Value} {
			binary.BigEndian.PutUint64(length[:], uint64(len(field)))
			_, _ = hash.Write(length[:])
			_, _ = hash.Write([]byte(field))
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func compareManagedLayoutFacts(expected, actual []managedLayoutFact, limit int) ([]managedLayoutDifference, int, error) {
	expectedNormalized, err := normalizeManagedLayoutFacts(expected)
	if err != nil {
		return nil, 0, fmt.Errorf("normalize expected managed layout facts: %w", err)
	}
	actualNormalized, err := normalizeManagedLayoutFacts(actual)
	if err != nil {
		return nil, 0, fmt.Errorf("normalize actual managed layout facts: %w", err)
	}
	expectedByKey := make(map[managedLayoutFactKey]string, len(expectedNormalized))
	actualByKey := make(map[managedLayoutFactKey]string, len(actualNormalized))
	keys := make(map[managedLayoutFactKey]struct{}, len(expectedNormalized)+len(actualNormalized))
	for _, fact := range expectedNormalized {
		key := managedLayoutFactKey{Kind: fact.Kind, Identity: fact.Identity, Attribute: fact.Attribute}
		expectedByKey[key] = fact.Value
		keys[key] = struct{}{}
	}
	for _, fact := range actualNormalized {
		key := managedLayoutFactKey{Kind: fact.Kind, Identity: fact.Identity, Attribute: fact.Attribute}
		actualByKey[key] = fact.Value
		keys[key] = struct{}{}
	}
	orderedKeys := make([]managedLayoutFactKey, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Slice(orderedKeys, func(i, j int) bool {
		if orderedKeys[i].Kind != orderedKeys[j].Kind {
			return orderedKeys[i].Kind < orderedKeys[j].Kind
		}
		if orderedKeys[i].Identity != orderedKeys[j].Identity {
			return orderedKeys[i].Identity < orderedKeys[j].Identity
		}
		return orderedKeys[i].Attribute < orderedKeys[j].Attribute
	})

	if limit < 0 {
		limit = 0
	}
	differences := make([]managedLayoutDifference, 0, min(limit, len(orderedKeys)))
	total := 0
	for _, key := range orderedKeys {
		expectedValue, expectedOK := expectedByKey[key]
		actualValue, actualOK := actualByKey[key]
		if expectedOK && actualOK && expectedValue == actualValue {
			continue
		}
		total++
		if len(differences) >= limit {
			continue
		}
		difference := managedLayoutDifference{
			Kind:      key.Kind,
			Identity:  key.Identity,
			Attribute: key.Attribute,
			Expected:  expectedValue,
			Actual:    actualValue,
		}
		switch {
		case expectedOK && actualOK:
			difference.Category = "changed"
		case expectedOK:
			difference.Category = "missing"
		default:
			difference.Category = "unexpected"
		}
		differences = append(differences, difference)
	}
	return differences, total, nil
}

func validateManagedLayoutFacts(layoutName string, expected, actual []managedLayoutFact) error {
	expectedFingerprint, err := managedLayoutFingerprint(expected)
	if err != nil {
		return fmt.Errorf("fingerprint expected managed layout %q: %w", layoutName, err)
	}
	actualFingerprint, err := managedLayoutFingerprint(actual)
	if err != nil {
		return fmt.Errorf("fingerprint actual managed layout %q: %w", layoutName, err)
	}
	differences, total, err := compareManagedLayoutFacts(expected, actual, managedLayoutDifferenceLimit)
	if err != nil {
		return err
	}
	if total == 0 {
		return nil
	}
	details := make([]string, 0, len(differences)+1)
	for _, difference := range differences {
		switch difference.Category {
		case "changed":
			details = append(details, fmt.Sprintf(
				"changed %s %s attribute %s (expected=%q actual=%q)",
				difference.Kind,
				difference.Identity,
				difference.Attribute,
				difference.Expected,
				difference.Actual,
			))
		default:
			details = append(details, fmt.Sprintf(
				"%s %s %s attribute %s",
				difference.Category,
				difference.Kind,
				difference.Identity,
				difference.Attribute,
			))
		}
	}
	if omitted := total - len(differences); omitted > 0 {
		details = append(details, fmt.Sprintf("%d additional differences omitted", omitted))
	}
	return unsupportedSchemaf(
		"managed sync layout mismatch for layout %q (expected fingerprint %s, actual fingerprint %s, mismatch count %d): %s; stop all Oversync service instances, restore the exact managed layout or recreate it under the approved policy, then retry bootstrap",
		layoutName,
		expectedFingerprint,
		actualFingerprint,
		total,
		strings.Join(details, "; "),
	)
}

func appendManagedLayoutQueryFacts(
	ctx context.Context,
	q syncCatalogQuerier,
	facts []managedLayoutFact,
	phase string,
	query string,
	args ...any,
) ([]managedLayoutFact, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("load managed layout %s facts: %w", phase, err)
	}
	defer rows.Close()
	for rows.Next() {
		var fact managedLayoutFact
		if err := rows.Scan(&fact.Kind, &fact.Identity, &fact.Attribute, &fact.Value); err != nil {
			return nil, fmt.Errorf("scan managed layout %s fact: %w", phase, err)
		}
		if fact.Kind == "function" && fact.Attribute == "source" {
			sum := sha256.Sum256([]byte(fact.Value))
			fact.Attribute = "source.sha256"
			fact.Value = hex.EncodeToString(sum[:])
		}
		if fact.Kind == "trigger" && fact.Attribute == "arguments.hex" {
			decoded, err := hex.DecodeString(fact.Value)
			if err != nil {
				return nil, fmt.Errorf("decode managed trigger arguments for %s: %w", fact.Identity, err)
			}
			if len(decoded) > 0 && decoded[len(decoded)-1] == 0 {
				decoded = decoded[:len(decoded)-1]
			}
			arguments := []string{}
			if len(decoded) > 0 {
				arguments = strings.Split(string(decoded), "\x00")
			}
			encoded, err := json.Marshal(arguments)
			if err != nil {
				return nil, fmt.Errorf("encode managed trigger arguments for %s: %w", fact.Identity, err)
			}
			fact.Attribute = "arguments"
			fact.Value = string(encoded)
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed layout %s facts: %w", phase, err)
	}
	return facts, nil
}

func loadManagedLayoutFacts(ctx context.Context, q syncCatalogQuerier) ([]managedLayoutFact, error) {
	return loadManagedLayoutFactsSelected(ctx, q, false)
}

func loadManagedLayoutVolatileFacts(ctx context.Context, q syncCatalogQuerier) ([]managedLayoutFact, error) {
	return loadManagedLayoutFactsSelected(ctx, q, true)
}

func loadManagedLayoutFactsSelected(ctx context.Context, q syncCatalogQuerier, volatileOnly bool) ([]managedLayoutFact, error) {
	facts := make([]managedLayoutFact, 0, 512)
	// Catalog paths are semantic fact identities, not executable SQL. Render
	// their exact components without PostgreSQL's version-dependent %I quoting.
	queries := []struct {
		phase string
		sql   string
	}{
		{
			phase: "namespace and relation",
			sql: `
SELECT 'namespace', 'sync', 'present', 'true'
FROM pg_namespace
WHERE nspname = 'sync'
UNION ALL
SELECT 'relation', format('%s.%s', namespace.nspname, relation.relname), attribute.name, attribute.value
FROM pg_class AS relation
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
CROSS JOIN LATERAL (
  VALUES
    ('kind'::text, relation.relkind::text),
    ('persistence', relation.relpersistence::text),
    ('is_partition', relation.relispartition::text),
    ('row_security', relation.relrowsecurity::text),
    ('force_row_security', relation.relforcerowsecurity::text),
    ('replica_identity', relation.relreplident::text)
) AS attribute(name, value)
WHERE namespace.nspname = 'sync'
  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "column",
			sql: `
SELECT
  'column',
  format('%s.%s.%s', namespace.nspname, relation.relname, attribute.attname),
  fact.name,
  fact.value
FROM pg_class AS relation
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
JOIN pg_attribute AS attribute
  ON attribute.attrelid = relation.oid
 AND attribute.attnum > 0
 AND NOT attribute.attisdropped
JOIN pg_type AS type ON type.oid = attribute.atttypid
JOIN pg_namespace AS type_namespace ON type_namespace.oid = type.typnamespace
LEFT JOIN pg_attrdef AS default_value
  ON default_value.adrelid = relation.oid
 AND default_value.adnum = attribute.attnum
LEFT JOIN pg_collation AS collation_entry ON collation_entry.oid = attribute.attcollation
LEFT JOIN pg_namespace AS collation_namespace ON collation_namespace.oid = collation_entry.collnamespace
CROSS JOIN LATERAL (
  VALUES
    ('ordinal'::text, attribute.attnum::text),
    ('type', format('%s.%s', type_namespace.nspname, type.typname)),
    ('typmod', attribute.atttypmod::text),
    ('not_null', attribute.attnotnull::text),
    ('identity', attribute.attidentity::text),
    ('generated', attribute.attgenerated::text),
    ('default', COALESCE(
      CASE WHEN set_config('TimeZone', 'UTC', true) = 'UTC'
        THEN pg_get_expr(default_value.adbin, default_value.adrelid)
        ELSE NULL
      END,
      ''
    )),
    ('collation', CASE WHEN collation_entry.oid IS NULL THEN '' ELSE format('%s.%s', collation_namespace.nspname, collation_entry.collname) END)
) AS fact(name, value)
WHERE namespace.nspname = 'sync'
  AND relation.relkind IN ('r', 'p')
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "constraint",
			sql: `
WITH constraint_details AS (
  SELECT
    constraint_entry.oid,
    constraint_entry.conrelid,
    format('%s.%s.%s', namespace.nspname, relation.relname, constraint_entry.conname) AS identity,
    constraint_entry.contype::text AS type,
    COALESCE((
      SELECT to_json(array_agg(attribute.attname ORDER BY key.ordinality))::text
      FROM unnest(constraint_entry.conkey) WITH ORDINALITY AS key(attnum, ordinality)
      JOIN pg_attribute AS attribute
        ON attribute.attrelid = constraint_entry.conrelid
       AND attribute.attnum = key.attnum
    ), '[]') AS columns,
    CASE WHEN constraint_entry.confrelid = 0 THEN '' ELSE format('%s.%s', foreign_namespace.nspname, foreign_relation.relname) END AS referenced_table,
    COALESCE((
      SELECT to_json(array_agg(attribute.attname ORDER BY key.ordinality))::text
      FROM unnest(constraint_entry.confkey) WITH ORDINALITY AS key(attnum, ordinality)
      JOIN pg_attribute AS attribute
        ON attribute.attrelid = constraint_entry.confrelid
       AND attribute.attnum = key.attnum
    ), '[]') AS referenced_columns,
    constraint_entry.confmatchtype::text AS match_type,
    constraint_entry.confupdtype::text AS update_action,
    constraint_entry.confdeltype::text AS delete_action,
    constraint_entry.condeferrable::text AS deferrable,
    constraint_entry.condeferred::text AS initially_deferred,
    constraint_entry.convalidated::text AS validated,
    CASE
      WHEN constraint_entry.contype = 'c' AND set_config('TimeZone', 'UTC', true) = 'UTC'
        THEN pg_get_expr(constraint_entry.conbin, constraint_entry.conrelid)
      ELSE ''
    END AS expression
  FROM pg_constraint AS constraint_entry
  JOIN pg_class AS relation ON relation.oid = constraint_entry.conrelid
  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
  LEFT JOIN pg_class AS foreign_relation ON foreign_relation.oid = constraint_entry.confrelid
  LEFT JOIN pg_namespace AS foreign_namespace ON foreign_namespace.oid = foreign_relation.relnamespace
  WHERE namespace.nspname = 'sync'
)
SELECT 'constraint', details.identity, fact.name, fact.value
FROM constraint_details AS details
CROSS JOIN LATERAL (
  VALUES
    ('type'::text, details.type),
    ('columns', details.columns),
    ('referenced_table', details.referenced_table),
    ('referenced_columns', details.referenced_columns),
    ('match_type', details.match_type),
    ('update_action', details.update_action),
    ('delete_action', details.delete_action),
    ('deferrable', details.deferrable),
    ('initially_deferred', details.initially_deferred),
    ('validated', details.validated),
    ('expression', details.expression)
) AS fact(name, value)
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "index",
			sql: `
WITH index_details AS (
  SELECT
    index_relation.oid AS index_oid,
    index.indrelid AS table_oid,
    format('%s.%s', index_namespace.nspname, index_relation.relname) AS identity,
    format('%s.%s', table_namespace.nspname, table_relation.relname) AS table_identity,
    access_method.amname AS access_method,
    index.indisunique::text AS is_unique,
    index.indisprimary::text AS is_primary,
    index.indisexclusion::text AS is_exclusion,
    index.indisvalid::text AS is_valid,
    index.indisready::text AS is_ready,
    index.indislive::text AS is_live,
    index.indnkeyatts::text AS key_count,
    index.indnatts::text AS attribute_count,
    COALESCE(
      CASE WHEN set_config('TimeZone', 'UTC', true) = 'UTC'
        THEN pg_get_expr(index.indpred, index.indrelid)
        ELSE NULL
      END,
      ''
    ) AS predicate
  FROM pg_index AS index
  JOIN pg_class AS index_relation ON index_relation.oid = index.indexrelid
  JOIN pg_namespace AS index_namespace ON index_namespace.oid = index_relation.relnamespace
  JOIN pg_class AS table_relation ON table_relation.oid = index.indrelid
  JOIN pg_namespace AS table_namespace ON table_namespace.oid = table_relation.relnamespace
  JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
  WHERE table_namespace.nspname = 'sync'
), index_facts AS (
  SELECT
    details.identity,
    fact.name,
    fact.value
  FROM index_details AS details
  CROSS JOIN LATERAL (
    VALUES
      ('table'::text, details.table_identity),
      ('access_method', details.access_method),
      ('unique', details.is_unique),
      ('primary', details.is_primary),
      ('exclusion', details.is_exclusion),
      ('valid', details.is_valid),
      ('ready', details.is_ready),
      ('live', details.is_live),
      ('key_count', details.key_count),
      ('attribute_count', details.attribute_count),
      ('predicate', details.predicate)
  ) AS fact(name, value)
), index_key_facts AS (
  SELECT
    details.identity,
    format('attribute.%s', position.ordinality) AS name,
    jsonb_build_object(
      'role', CASE WHEN position.ordinality <= index.indnkeyatts THEN 'key' ELSE 'include' END,
      'column', COALESCE(attribute.attname, ''),
	  'expression', CASE
	    WHEN position.attnum = 0 AND set_config('TimeZone', 'UTC', true) = 'UTC'
	      THEN pg_get_indexdef(index.indexrelid, position.ordinality::integer, false)
	    ELSE ''
	  END,
      'collation', CASE WHEN collation_entry.oid IS NULL OR collation_entry.oid = 0 THEN '' ELSE format('%s.%s', collation_namespace.nspname, collation_entry.collname) END,
      'opclass', CASE WHEN operator_class.oid IS NULL OR operator_class.oid = 0 THEN '' ELSE format('%s.%s', operator_namespace.nspname, operator_class.opcname) END,
      'descending', ((position.options & 1) = 1),
      'nulls_first', ((position.options & 2) = 2)
    )::text AS value
  FROM index_details AS details
  JOIN pg_index AS index ON index.indexrelid = details.index_oid
  CROSS JOIN LATERAL unnest(index.indkey, index.indcollation, index.indclass, index.indoption)
    WITH ORDINALITY AS position(attnum, collation_oid, opclass_oid, options, ordinality)
  LEFT JOIN pg_attribute AS attribute
    ON attribute.attrelid = index.indrelid
   AND attribute.attnum = position.attnum
  LEFT JOIN pg_collation AS collation_entry ON collation_entry.oid = position.collation_oid
  LEFT JOIN pg_namespace AS collation_namespace ON collation_namespace.oid = collation_entry.collnamespace
  LEFT JOIN pg_opclass AS operator_class ON operator_class.oid = position.opclass_oid
  LEFT JOIN pg_namespace AS operator_namespace ON operator_namespace.oid = operator_class.opcnamespace
)
SELECT 'index', identity, name, value FROM index_facts
UNION ALL
SELECT 'index', identity, name, value FROM index_key_facts
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "sequence",
			sql: `
WITH sequence_details AS (
  SELECT
    format('%s.%s', namespace.nspname, relation.relname) AS identity,
    format('%s.%s', type_namespace.nspname, type.typname) AS data_type,
    relation.relpersistence::text AS persistence,
    sequence.seqstart::text AS start_value,
    sequence.seqincrement::text AS increment,
    sequence.seqmax::text AS max_value,
    sequence.seqmin::text AS min_value,
    sequence.seqcache::text AS cache,
    sequence.seqcycle::text AS cycle,
    CASE WHEN owner_attribute.attname IS NULL THEN '' ELSE format('%s.%s.%s', owner_namespace.nspname, owner_relation.relname, owner_attribute.attname) END AS owned_by
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
  JOIN pg_sequence AS sequence ON sequence.seqrelid = relation.oid
  JOIN pg_type AS type ON type.oid = sequence.seqtypid
  JOIN pg_namespace AS type_namespace ON type_namespace.oid = type.typnamespace
  LEFT JOIN pg_depend AS dependency
    ON dependency.classid = 'pg_class'::regclass
   AND dependency.objid = relation.oid
   AND dependency.objsubid = 0
   AND dependency.refclassid = 'pg_class'::regclass
   AND dependency.deptype IN ('a', 'i')
  LEFT JOIN pg_class AS owner_relation ON owner_relation.oid = dependency.refobjid
  LEFT JOIN pg_namespace AS owner_namespace ON owner_namespace.oid = owner_relation.relnamespace
  LEFT JOIN pg_attribute AS owner_attribute
    ON owner_attribute.attrelid = dependency.refobjid
   AND owner_attribute.attnum = dependency.refobjsubid
  WHERE namespace.nspname = 'sync'
)
SELECT 'sequence', details.identity, fact.name, fact.value
FROM sequence_details AS details
CROSS JOIN LATERAL (
  VALUES
    ('data_type'::text, details.data_type),
    ('persistence', details.persistence),
    ('start', details.start_value),
    ('increment', details.increment),
    ('maximum', details.max_value),
    ('minimum', details.min_value),
    ('cache', details.cache),
    ('cycle', details.cycle),
    ('owned_by', details.owned_by)
) AS fact(name, value)
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "function",
			sql: `
WITH function_details AS (
  SELECT
    format('%s.%s(%s)', namespace.nspname, procedure.proname, pg_get_function_identity_arguments(procedure.oid)) AS identity,
    format('%s.%s', return_namespace.nspname, return_type.typname) AS return_type,
    language.lanname AS language,
    procedure.provolatile::text AS volatility,
    procedure.proisstrict::text AS strict,
    procedure.prosecdef::text AS security_definer,
    procedure.proleakproof::text AS leakproof,
    procedure.proparallel::text AS parallel,
    COALESCE(to_json(procedure.proconfig)::text, '[]') AS configuration,
    procedure.prosrc AS source
  FROM pg_proc AS procedure
  JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
  JOIN pg_language AS language ON language.oid = procedure.prolang
  JOIN pg_type AS return_type ON return_type.oid = procedure.prorettype
  JOIN pg_namespace AS return_namespace ON return_namespace.oid = return_type.typnamespace
  WHERE namespace.nspname = 'sync'
)
SELECT 'function', details.identity, fact.name, fact.value
FROM function_details AS details
CROSS JOIN LATERAL (
  VALUES
    ('return_type'::text, details.return_type),
    ('language', details.language),
    ('volatility', details.volatility),
    ('strict', details.strict),
    ('security_definer', details.security_definer),
    ('leakproof', details.leakproof),
    ('parallel', details.parallel),
    ('configuration', details.configuration),
    ('source', details.source)
) AS fact(name, value)
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "trigger",
			sql: `
WITH trigger_details AS (
  SELECT
    format('%s.%s.%s', table_namespace.nspname, relation.relname, trigger.tgname) AS identity,
    format('%s.%s(%s)', function_namespace.nspname, procedure.proname, pg_get_function_identity_arguments(procedure.oid)) AS function_identity,
    trigger.tgenabled::text AS enabled,
    CASE
      WHEN (trigger.tgtype & 64) = 64 THEN 'instead_of'
      WHEN (trigger.tgtype & 2) = 2 THEN 'before'
      ELSE 'after'
    END AS timing,
    CASE WHEN (trigger.tgtype & 1) = 1 THEN 'row' ELSE 'statement' END AS level,
    concat_ws(',',
      CASE WHEN (trigger.tgtype & 4) = 4 THEN 'insert' END,
      CASE WHEN (trigger.tgtype & 16) = 16 THEN 'update' END,
      CASE WHEN (trigger.tgtype & 8) = 8 THEN 'delete' END,
      CASE WHEN (trigger.tgtype & 32) = 32 THEN 'truncate' END
    ) AS events,
    COALESCE(trigger.tgoldtable::text, '') AS old_transition,
    COALESCE(trigger.tgnewtable::text, '') AS new_transition,
    CASE WHEN trigger.tgconstraint = 0 THEN '' ELSE format('%s.%s.%s', constraint_namespace.nspname, constraint_relation.relname, constraint_entry.conname) END AS constraint_identity,
    encode(trigger.tgargs, 'hex') AS arguments_hex
  FROM pg_trigger AS trigger
  JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
  JOIN pg_namespace AS table_namespace ON table_namespace.oid = relation.relnamespace
  JOIN pg_proc AS procedure ON procedure.oid = trigger.tgfoid
  JOIN pg_namespace AS function_namespace ON function_namespace.oid = procedure.pronamespace
  LEFT JOIN pg_constraint AS constraint_entry ON constraint_entry.oid = trigger.tgconstraint
  LEFT JOIN pg_class AS constraint_relation ON constraint_relation.oid = constraint_entry.conrelid
  LEFT JOIN pg_namespace AS constraint_namespace ON constraint_namespace.oid = constraint_relation.relnamespace
  WHERE NOT trigger.tgisinternal
	AND trigger.tgparentid = 0
    AND (
      table_namespace.nspname = 'sync'
      OR trigger.tgname IN ('oversync_bundle_capture_row', 'oversync_bundle_owner_guard', 'oversync_registered_truncate_guard')
    )
)
SELECT 'trigger', details.identity, fact.name, fact.value
FROM trigger_details AS details
CROSS JOIN LATERAL (
  VALUES
    ('function'::text, details.function_identity),
    ('enabled', details.enabled),
    ('timing', details.timing),
    ('level', details.level),
    ('events', details.events),
    ('old_transition', details.old_transition),
    ('new_transition', details.new_transition),
    ('constraint', details.constraint_identity),
    ('arguments.hex', details.arguments_hex)
) AS fact(name, value)
ORDER BY 1, 2, 3, 4`,
		},
		{
			phase: "rule",
			sql: `
SELECT
  'rule',
  format('%s.%s.%s', namespace.nspname, relation.relname, rule.rulename),
  'definition',
  pg_get_ruledef(rule.oid, false)
FROM pg_rewrite AS rule
JOIN pg_class AS relation ON relation.oid = rule.ev_class
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'sync'
  AND rule.rulename <> '_RETURN'
ORDER BY 1, 2, 3, 4`,
		},
	}
	var err error
	for _, query := range queries {
		if volatileOnly && query.phase != "function" && query.phase != "trigger" {
			continue
		}
		facts, err = appendManagedLayoutQueryFacts(ctx, q, facts, query.phase, query.sql)
		if err != nil {
			return nil, err
		}
	}
	return normalizeManagedLayoutFacts(facts)
}
