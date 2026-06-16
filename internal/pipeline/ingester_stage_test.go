package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
)

// mockSource is a test Source that emits a configurable number of records.
type mockSource struct {
	id      string
	srcType string
	records []*model.Record
	err     error
	delay   time.Duration
}

func (m *mockSource) Read(ctx context.Context, out chan<- *model.Record) error {
	for _, r := range m.records {
		if m.delay > 0 {
			select {
			case <-time.After(m.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.err
}

func (m *mockSource) Type() string       { return m.srcType }
func (m *mockSource) Identifier() string { return m.id }

func makeRecords(n int, sourceType, sourceID string) []*model.Record {
	records := make([]*model.Record, n)
	for i := 0; i < n; i++ {
		records[i] = &model.Record{
			ID:     "rec-" + sourceID + "-" + string(rune('0'+i)),
			Fields: map[string]interface{}{"val": i},
			Metadata: model.RecordMetadata{
				SourceType: sourceType,
				SourceID:   sourceID,
				LineNumber: i + 1,
			},
		}
	}
	return records
}

func TestIngester_Name(t *testing.T) {
	ing := NewIngester(nil, nil, "job-1")
	assert.Equal(t, "ingester", ing.Name())
}

func TestIngester_SingleSource(t *testing.T) {
	records := makeRecords(5, "csv", "file.csv")
	src := &mockSource{id: "file.csv", srcType: "csv", records: records}

	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{src}, errStore, "job-1")

	out := make(chan *model.Record, 100)
	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}
	assert.Len(t, received, 5)
}

func TestIngester_MultipleSources(t *testing.T) {
	records1 := makeRecords(3, "csv", "a.csv")
	records2 := makeRecords(4, "json", "b.json")
	src1 := &mockSource{id: "a.csv", srcType: "csv", records: records1}
	src2 := &mockSource{id: "b.json", srcType: "json", records: records2}

	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{src1, src2}, errStore, "job-1")

	out := make(chan *model.Record, 100)
	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}
	assert.Len(t, received, 7)
}

func TestIngester_SourceFailure_ContinuesRemaining(t *testing.T) {
	records := makeRecords(3, "json", "good.json")
	goodSrc := &mockSource{id: "good.json", srcType: "json", records: records}
	badSrc := &mockSource{id: "bad.csv", srcType: "csv", records: nil, err: errors.New("file not found")}

	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{badSrc, goodSrc}, errStore, "job-1")

	out := make(chan *model.Record, 100)
	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}
	// Good source records should still be received
	assert.Len(t, received, 3)

	// Error from bad source should be logged
	errs, total := errStore.GetByJob("job-1", 0, 50)
	assert.Equal(t, 1, total)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Message, "bad.csv")
	assert.Contains(t, errs[0].Message, "file not found")
	assert.Equal(t, "ingester", errs[0].Stage)
}

func TestIngester_ContextCancellation(t *testing.T) {
	// Source with delay so we can cancel mid-read
	records := makeRecords(10, "csv", "slow.csv")
	slowSrc := &mockSource{id: "slow.csv", srcType: "csv", records: records, delay: 100 * time.Millisecond}

	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{slowSrc}, errStore, "job-1")

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *model.Record, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = ing.Run(ctx, nil, out)
	}()

	// Give it a moment to start, then cancel
	time.Sleep(250 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.ErrorIs(t, runErr, context.Canceled)
}

func TestIngester_NoSources(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{}, errStore, "job-1")

	out := make(chan *model.Record, 100)
	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	// Channel should be closed with no records
	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}
	assert.Empty(t, received)
}

func TestIngester_ConcurrentExecution(t *testing.T) {
	// Verify sources run concurrently by checking total time
	records1 := makeRecords(1, "csv", "a.csv")
	records2 := makeRecords(1, "json", "b.json")
	src1 := &mockSource{id: "a.csv", srcType: "csv", records: records1, delay: 100 * time.Millisecond}
	src2 := &mockSource{id: "b.json", srcType: "json", records: records2, delay: 100 * time.Millisecond}

	errStore := store.NewInMemoryErrorStore()
	ing := NewIngester([]Source{src1, src2}, errStore, "job-1")

	out := make(chan *model.Record, 100)

	start := time.Now()
	err := ing.Run(context.Background(), nil, out)
	elapsed := time.Since(start)

	assert.NoError(t, err)

	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}
	assert.Len(t, received, 2)

	// If sources run concurrently, total time should be ~100ms, not ~200ms
	assert.Less(t, elapsed, 180*time.Millisecond, "Sources should run concurrently")
}
