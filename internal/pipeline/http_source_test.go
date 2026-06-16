package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestHTTPSource_Read_Success(t *testing.T) {
	data := []map[string]interface{}{
		{"name": "Alice", "age": float64(30)},
		{"name": "Bob", "age": float64(25)},
	}
	body, _ := json.Marshal(data)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(context.Background(), out)
	close(out)

	assert.NoError(t, err)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 2)

	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, float64(30), records[0].Fields["age"])
	assert.Equal(t, "http", records[0].Metadata.SourceType)
	assert.Equal(t, server.URL, records[0].Metadata.SourceID)
	assert.Equal(t, 1, records[0].Metadata.LineNumber)

	assert.Equal(t, "Bob", records[1].Fields["name"])
	assert.Equal(t, float64(25), records[1].Fields["age"])
	assert.Equal(t, "http", records[1].Metadata.SourceType)
	assert.Equal(t, server.URL, records[1].Metadata.SourceID)
	assert.Equal(t, 2, records[1].Metadata.LineNumber)
}

func TestHTTPSource_Read_EmptyArray(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(context.Background(), out)
	close(out)

	assert.NoError(t, err)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}
	assert.Len(t, records, 0)
}

func TestHTTPSource_Read_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(context.Background(), out)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "returned status 500")
}

func TestHTTPSource_Read_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(context.Background(), out)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

func TestHTTPSource_Read_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := make([]map[string]interface{}, 100)
		for i := range data {
			data[i] = map[string]interface{}{"index": float64(i)}
		}
		body, _ := json.Marshal(data)
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(ctx, out)

	// Should get an error (either context cancelled during request or during emit)
	assert.Error(t, err)
}

func TestHTTPSource_Read_DefaultTimeout(t *testing.T) {
	source := NewHTTPSource("http://example.com", 0)
	assert.Equal(t, 0, source.TimeoutSeconds) // Will use DefaultHTTPTimeout in Read
}

func TestHTTPSource_Type(t *testing.T) {
	source := NewHTTPSource("http://example.com/api", 30)
	assert.Equal(t, "http", source.Type())
}

func TestHTTPSource_Identifier(t *testing.T) {
	url := "http://example.com/api/data"
	source := NewHTTPSource(url, 30)
	assert.Equal(t, url, source.Identifier())
}

func TestHTTPSource_ImplementsSourceInterface(t *testing.T) {
	var _ Source = &HTTPSource{}
}

func TestHTTPSource_RecordIDs_AreUnique(t *testing.T) {
	data := []map[string]interface{}{
		{"a": "1"},
		{"a": "2"},
		{"a": "3"},
	}
	body, _ := json.Marshal(data)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, 10)
	out := make(chan *model.Record, 10)

	err := source.Read(context.Background(), out)
	close(out)

	assert.NoError(t, err)

	ids := make(map[string]bool)
	for r := range out {
		assert.NotEmpty(t, r.ID)
		assert.False(t, ids[r.ID], "duplicate record ID found: %s", r.ID)
		ids[r.ID] = true
	}
	assert.Len(t, ids, 3)
}
