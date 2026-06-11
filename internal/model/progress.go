package model

// Progress holds real-time metrics for a running job.
type Progress struct {
	RecordsProcessed int64              `json:"records_processed"`
	RecordsPending   int64              `json:"records_pending"`
	PercentComplete  int                `json:"percent_complete"` // 0-100
	ProcessingRate   float64            `json:"processing_rate"`  // records/sec
	StageLatencies   map[string]float64 `json:"stage_latencies"`  // ms per record
	ErrorCounts      map[string]int64   `json:"error_counts"`     // per stage
}
