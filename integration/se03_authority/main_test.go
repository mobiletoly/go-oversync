package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestAuthorityContractDeclaresExactNotesEvolution(t *testing.T) {
	registry, err := oversync.CompileSchemaGenerationRegistry(registeredTables(), schemaGenerations())
	require.NoError(t, err)
	one, ok := registry.Generation(1)
	require.True(t, ok)
	two, ok := registry.Generation(2)
	require.True(t, ok)
	require.NotEqual(t, one.Fingerprint(), two.Fingerprint())
	require.Equal(t, []oversync.RegisteredTable{{Schema: "se03", Table: "notes", SyncKeyColumns: []string{"id"}}}, registeredTables())

	generations := schemaGenerations()
	require.Equal(t, oversync.SchemaGenerationID(1), generations[0].ID)
	require.Equal(t, []string{"id", "title"}, []string{generations[0].Tables[0].Columns[0].Name, generations[0].Tables[0].Columns[1].Name})
	require.Equal(t, oversync.SchemaGenerationID(2), generations[1].ID)
	require.Equal(t, "details", generations[1].Tables[0].Columns[2].Name)
	require.True(t, generations[1].Tables[0].Columns[2].Nullable)
}

func TestAuthorityControlIsDisabledWithoutTokenAndAuthenticatedWhenEnabled(t *testing.T) {
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	components := &server.ServerComponents{Handler: fallback}

	disabled := httptest.NewRecorder()
	authorityHandler(components, "").ServeHTTP(
		disabled,
		httptest.NewRequest(http.MethodPost, "/se03/control", bytes.NewBufferString(`{"action":"inspect","user_id":"scope"}`)),
	)
	require.Equal(t, http.StatusTeapot, disabled.Code)

	unauthorized := httptest.NewRecorder()
	authorityHandler(components, "control-secret").ServeHTTP(
		unauthorized,
		httptest.NewRequest(http.MethodPost, "/se03/control", bytes.NewBufferString(`{"action":"inspect","user_id":"scope"}`)),
	)
	require.Equal(t, http.StatusUnauthorized, unauthorized.Code)

	invalidActionRequest := httptest.NewRequest(
		http.MethodPost,
		"/se03/control",
		bytes.NewBufferString(`{"action":"unknown","user_id":"scope"}`),
	)
	invalidActionRequest.Header.Set("X-SE03-Control-Token", "control-secret")
	invalidAction := httptest.NewRecorder()
	authorityHandler(components, "control-secret").ServeHTTP(invalidAction, invalidActionRequest)
	require.Equal(t, http.StatusBadRequest, invalidAction.Code)
}

func TestAuthorityControlRejectsUnboundedOrMalformedCommandsBeforeDatabaseAccess(t *testing.T) {
	_, err := seedScope(t.Context(), &server.ServerComponents{}, controlRequest{Action: "seed"})
	require.ErrorContains(t, err, "requires user_id, id, and title")

	_, err = inspectScope(t.Context(), nil, "")
	require.ErrorContains(t, err, "requires user_id")

	_, err = expireSessions(t.Context(), nil, controlRequest{Action: "expire_sessions", SessionKind: "all"})
	require.ErrorContains(t, err, "requires user_id or session_id")

	_, err = expireSessions(t.Context(), nil, controlRequest{
		Action: "expire_sessions", UserID: "scope", SessionKind: "unknown",
	})
	require.ErrorContains(t, err, "session_kind must be snapshot when supplied")
}
