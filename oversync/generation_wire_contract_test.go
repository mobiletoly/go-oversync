package oversync

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerationPushAdmissionRejectsNonCanonicalWireValues(t *testing.T) {
	contract := SchemaGenerationContract{id: 1, tables: []GenerationTableContract{{
		Schema: "business", Table: "records", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{
			{Name: "id", WireType: GenerationWireTypeUUID},
			{Name: "active", WireType: GenerationWireTypeBoolean},
			{Name: "amount", WireType: GenerationWireTypeDecimal},
			{Name: "ratio", WireType: GenerationWireTypeFloat64},
			{Name: "blob", WireType: GenerationWireTypeBytes},
			{Name: "label", WireType: GenerationWireTypeText},
			{Name: "document", WireType: GenerationWireTypeJSON},
		},
	}}}
	base := PushRequestRow{
		Schema: "business", Table: "records", Op: OpInsert,
		Key: SyncKey{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	}
	for name, payload := range map[string]string{
		"decimal exponent":        `{"amount":"1.25e3"}`,
		"string boolean":          `{"active":"1"}`,
		"noncanonical float":      `{"ratio":"1.50"}`,
		"noncanonical base64":     `{"blob":"YQ"}`,
		"noncanonical inner JSON": `{"document":"{\"b\":1,\"a\":2}"}`,
		"text U+0000":             `{"label":"a\u0000b"}`,
		"inner JSON U+0000":       `{"document":"\"a\\u0000b\""}`,
	} {
		t.Run(name, func(t *testing.T) {
			row := base
			row.Payload = json.RawMessage(payload)
			require.Error(t, validateGenerationPushRows(contract, []PushRequestRow{row}))
		})
	}

	row := base
	row.Payload = json.RawMessage(`{"active":true,"amount":"1.250","ratio":"1.2345678806304932","blob":"YQ==","document":"{\"a\":2,\"b\":1}"}`)
	require.NoError(t, validateGenerationPushRows(contract, []PushRequestRow{row}))
}

func TestGenerationPushAdmissionIsExactAboutSurfaceAndNullability(t *testing.T) {
	contract := SchemaGenerationContract{id: 1, tables: []GenerationTableContract{{
		Schema: "business", Table: "records", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{
			{Name: "id", WireType: GenerationWireTypeText},
			{Name: "required", WireType: GenerationWireTypeText},
			{Name: "optional", WireType: GenerationWireTypeText, Nullable: true},
		},
	}}}
	valid := PushRequestRow{Schema: "business", Table: "records", Key: SyncKey{"id": "exact"}, Op: OpUpdate, Payload: json.RawMessage(`{"optional":null}`)}
	require.NoError(t, validateGenerationPushRows(contract, []PushRequestRow{valid}))

	for _, mutate := range []func(*PushRequestRow){
		func(row *PushRequestRow) { row.Schema = " business" },
		func(row *PushRequestRow) { row.Table = "RECORDS" },
		func(row *PushRequestRow) { row.Op = "update" },
		func(row *PushRequestRow) { row.Key["extra"] = "value" },
		func(row *PushRequestRow) { row.Payload = json.RawMessage(`{"unknown":"value"}`) },
		func(row *PushRequestRow) { row.Payload = json.RawMessage(`{"required":null}`) },
	} {
		row := valid
		row.Key = SyncKey{"id": "exact"}
		mutate(&row)
		require.Error(t, validateGenerationPushRows(contract, []PushRequestRow{row}))
	}
}

func TestGenerationPushAdmissionRejectsU0000InTextSyncKey(t *testing.T) {
	t.Parallel()
	contract := SchemaGenerationContract{id: 1, tables: []GenerationTableContract{{
		Schema: "business", Table: "records", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}},
	}}}
	row := PushRequestRow{
		Schema: "business", Table: "records", Key: SyncKey{"id": "a\x00b"}, Op: OpDelete,
	}
	require.Error(t, validateGenerationPushRows(contract, []PushRequestRow{row}))
}

func TestCanonicalizeWirePayloadEncodesJSONAsInnerJCSString(t *testing.T) {
	service := &SyncService{columnTypesByTable: map[string]map[string]string{
		Key("business", "records"): {"document": "jsonb"},
	}}
	got, err := service.canonicalizeWirePayload("business", "records", json.RawMessage(`{"document":"{\"b\":1,\"a\":2}"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"document":"{\"a\":2,\"b\":1}"}`, string(got))
}

func TestCanonicalizeWirePayloadKeepsJSONDocumentNullDistinctFromSQLNull(t *testing.T) {
	t.Parallel()
	service := &SyncService{columnTypesByTable: map[string]map[string]string{
		Key("business", "records"): {"document": "jsonb"},
	}}

	documentNull, err := service.canonicalizeWirePayload("business", "records", json.RawMessage(`{"document":"null"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"document":"null"}`, string(documentNull))

	sqlNull, err := service.canonicalizeWirePayload("business", "records", json.RawMessage(`{"document":null}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"document":null}`, string(sqlNull))
}
