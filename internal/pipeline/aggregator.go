package pipeline

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// Aggregator implements the Stage interface and computes aggregations over records.
// It is a batch stage: it collects ALL input records before computing results.
type Aggregator struct {
	Config   model.AggregationConfig
	JobID    string
	ErrStore store.ErrorStore
	Progress store.ProgressTracker
}

// NewAggregator creates a new Aggregator stage with the given configuration.
func NewAggregator(config model.AggregationConfig, jobID string, errStore store.ErrorStore, progress store.ProgressTracker) *Aggregator {
	return &Aggregator{
		Config:   config,
		JobID:    jobID,
		ErrStore: errStore,
		Progress: progress,
	}
}

// Name returns the stage name.
func (a *Aggregator) Name() string {
	return "aggregator"
}

// Run executes the aggregation stage. It collects all records from the input channel,
// groups them by the configured group-by fields, computes aggregation functions per group,
// and emits one result record per group to the output channel.
func (a *Aggregator) Run(ctx context.Context, in <-chan *model.Record, out chan<- *model.Record) error {
	defer close(out)

	// Collect all records from input
	var records []*model.Record
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-in:
			if !ok {
				// Input channel closed, proceed to aggregation
				goto aggregate
			}
			records = append(records, record)
		}
	}

aggregate:
	// Handle zero-input case
	if len(records) == 0 {
		log.Printf("[aggregator] warning: zero records received for job %s", a.JobID)
		a.ErrStore.Add(a.JobID, model.ErrorEntry{
			JobID:     a.JobID,
			Stage:     "aggregator",
			Message:   "zero records received for aggregation",
			Record:    nil,
			Timestamp: time.Now().UTC(),
		})
		return nil
	}

	// Group records
	groups := a.groupRecords(records)

	// Compute aggregations per group and emit results
	for groupKey, groupRecords := range groups {
		start := time.Now()

		result, err := a.computeAggregations(groupKey, groupRecords)
		if err != nil {
			// This shouldn't happen since we already filtered records, but just in case
			a.ErrStore.Add(a.JobID, model.ErrorEntry{
				JobID:     a.JobID,
				Stage:     "aggregator",
				Message:   fmt.Sprintf("aggregation computation error for group '%s': %s", groupKey, err.Error()),
				Record:    nil,
				Timestamp: time.Now().UTC(),
			})
			a.Progress.RecordFailed(a.JobID, "aggregator")
			continue
		}

		latency := time.Since(start)
		a.Progress.RecordProcessed(a.JobID, "aggregator", latency)

		// Emit result
		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// groupRecords groups records by configured group-by fields.
// Records with missing/null group-by fields are excluded and errors logged.
// If no group-by fields are configured, all valid records go into a single group.
func (a *Aggregator) groupRecords(records []*model.Record) map[string][]*model.Record {
	groups := make(map[string][]*model.Record)

	for _, record := range records {
		if len(a.Config.GroupBy) == 0 {
			// No group-by: all records in one group
			groups["__all__"] = append(groups["__all__"], record)
			continue
		}

		// Check group-by fields
		groupKey, valid := a.buildGroupKey(record)
		if !valid {
			continue // Error already logged in buildGroupKey
		}
		groups[groupKey] = append(groups[groupKey], record)
	}

	return groups
}

// buildGroupKey builds a group key string from a record's group-by field values.
// Returns the key and true if valid, or empty string and false if any group-by field is missing/null.
func (a *Aggregator) buildGroupKey(record *model.Record) (string, bool) {
	parts := make([]string, 0, len(a.Config.GroupBy))

	for _, field := range a.Config.GroupBy {
		val, exists := record.Fields[field]
		if !exists || val == nil {
			a.ErrStore.Add(a.JobID, model.ErrorEntry{
				JobID:     a.JobID,
				Stage:     "aggregator",
				Message:   fmt.Sprintf("record excluded: missing or null group-by field '%s'", field),
				Record:    record.Fields,
				Timestamp: time.Now().UTC(),
			})
			a.Progress.RecordFailed(a.JobID, "aggregator")
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%v", val))
	}

	return strings.Join(parts, "\x00"), true
}

// computeAggregations computes all configured aggregation functions for a group of records.
func (a *Aggregator) computeAggregations(groupKey string, records []*model.Record) (*model.Record, error) {
	fields := make(map[string]interface{})

	// Add group-by field values from the first record in the group
	if len(a.Config.GroupBy) > 0 && len(records) > 0 {
		for _, field := range a.Config.GroupBy {
			fields[field] = records[0].Fields[field]
		}
	}

	// Compute each aggregation function
	for _, fn := range a.Config.Functions {
		value := a.computeFunction(fn, records)
		fields[fn.Alias] = value
	}

	// Add input record count
	fields["_count"] = float64(len(records))

	// Generate a unique ID for the result record
	id, err := generateAggregatorUUID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate result record ID: %w", err)
	}

	return &model.Record{
		ID:     id,
		Fields: fields,
		Metadata: model.RecordMetadata{
			SourceType: "aggregator",
			SourceID:   groupKey,
		},
	}, nil
}

// computeFunction computes a single aggregation function over the valid records in a group.
// - count: counts all records where the field exists (any value type is valid).
// - sum / average: require numeric field values; non-numeric records are skipped and logged.
func (a *Aggregator) computeFunction(fn model.AggregationFunction, records []*model.Record) float64 {
	// count("*") — count every record in the group unconditionally
	if fn.Name == "count" && fn.Field == "*" {
		return float64(len(records))
	}

	// count(field) — count records where the field exists, regardless of value type
	if fn.Name == "count" {
		var count int
		for _, record := range records {
			val, exists := record.Fields[fn.Field]
			if exists && val != nil {
				count++
			}
		}
		return float64(count)
	}

	// sum / average — require numeric values
	var sum float64
	var numericCount int

	for _, record := range records {
		val, exists := record.Fields[fn.Field]
		if !exists || val == nil {
			a.ErrStore.Add(a.JobID, model.ErrorEntry{
				JobID:     a.JobID,
				Stage:     "aggregator",
				Message:   fmt.Sprintf("record excluded from %s(%s): missing or null field '%s'", fn.Name, fn.Field, fn.Field),
				Record:    record.Fields,
				Timestamp: time.Now().UTC(),
			})
			a.Progress.RecordFailed(a.JobID, "aggregator")
			continue
		}

		numVal, err := aggToFloat64(val)
		if err != nil {
			a.ErrStore.Add(a.JobID, model.ErrorEntry{
				JobID:     a.JobID,
				Stage:     "aggregator",
				Message:   fmt.Sprintf("record excluded from %s(%s): non-numeric value '%v' in field '%s'", fn.Name, fn.Field, val, fn.Field),
				Record:    record.Fields,
				Timestamp: time.Now().UTC(),
			})
			a.Progress.RecordFailed(a.JobID, "aggregator")
			continue
		}

		sum += numVal
		numericCount++
	}

	switch fn.Name {
	case "sum":
		return sum
	case "average":
		if numericCount == 0 {
			return 0
		}
		return sum / float64(numericCount)
	default:
		return 0
	}
}

// aggToFloat64 attempts to convert a value to float64 for aggregation purposes.
func aggToFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to number", v)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to number", val)
	}
}

// generateAggregatorUUID generates a version 4 UUID string for aggregator result records.
func generateAggregatorUUID() (string, error) {
	var uuid [16]byte
	_, err := rand.Read(uuid[:])
	if err != nil {
		return "", err
	}

	// Set version 4 bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant bits
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}
