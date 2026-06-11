package model

// Record represents a data unit flowing through the pipeline.
type Record struct {
	ID       string                 `json:"id"`
	Fields   map[string]interface{} `json:"fields"`
	Metadata RecordMetadata         `json:"metadata"`
}

// RecordMetadata holds source information for a record.
type RecordMetadata struct {
	SourceType string `json:"source_type"` // "csv", "json", "http"
	SourceID   string `json:"source_id"`   // file path or URL
	LineNumber int    `json:"line_number"` // Original line/index in source
}
