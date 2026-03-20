package oversqlite

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func errorJSONResponse(status int, v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

type staticResolver struct {
	keepLocal bool
	merged    json.RawMessage
}

func (r *staticResolver) Merge(table string, pk string, server json.RawMessage, local json.RawMessage) (json.RawMessage, bool, error) {
	if r.merged != nil {
		return r.merged, r.keepLocal, nil
	}
	return server, r.keepLocal, nil
}
