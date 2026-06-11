package model

import "time"

// ErrorEntry represents a processing error stored in the Error Store.
type ErrorEntry struct {
	ID        string                 `json:"id"`
	JobID     string                 `json:"job_id"`
	Stage     string                 `json:"stage"`
	Message   string                 `json:"message"`   // Max 1000 chars
	Record    map[string]interface{} `json:"record"`    // Failed record data
	Timestamp time.Time              `json:"timestamp"` // ISO 8601 UTC
}
