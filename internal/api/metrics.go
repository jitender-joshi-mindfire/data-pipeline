package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/jitendraj/data-pipeline/internal/model"
)

// pipelineCollector is a Prometheus Collector that reads live pipeline
// metrics from the job store and progress tracker at each scrape.
type pipelineCollector struct {
	jobStore        JobStore
	progressTracker ProgressTracker

	descRecordsProcessed *prometheus.Desc
	descRecordsPending   *prometheus.Desc
	descPercentComplete  *prometheus.Desc
	descProcessingRate   *prometheus.Desc
	descStageLatency     *prometheus.Desc
	descErrorCount       *prometheus.Desc
	descJobsTotal        *prometheus.Desc
}

// newPipelineCollector creates and registers a pipelineCollector with the
// provided Prometheus registry.
func newPipelineCollector(jobStore JobStore, pt ProgressTracker) *pipelineCollector {
	return &pipelineCollector{
		jobStore:        jobStore,
		progressTracker: pt,

		descRecordsProcessed: prometheus.NewDesc(
			"pipeline_records_processed_total",
			"Total number of records processed by a pipeline job.",
			[]string{"job_id"}, nil,
		),
		descRecordsPending: prometheus.NewDesc(
			"pipeline_records_pending",
			"Number of records still pending in a pipeline job.",
			[]string{"job_id"}, nil,
		),
		descPercentComplete: prometheus.NewDesc(
			"pipeline_percent_complete",
			"Completion percentage (0–100) of a pipeline job.",
			[]string{"job_id"}, nil,
		),
		descProcessingRate: prometheus.NewDesc(
			"pipeline_processing_rate_records_per_sec",
			"Current processing rate in records per second for a pipeline job.",
			[]string{"job_id"}, nil,
		),
		descStageLatency: prometheus.NewDesc(
			"pipeline_stage_latency_ms",
			"Average latency in milliseconds per record for a pipeline stage.",
			[]string{"job_id", "stage"}, nil,
		),
		descErrorCount: prometheus.NewDesc(
			"pipeline_stage_errors_total",
			"Total number of errors produced by a pipeline stage.",
			[]string{"job_id", "stage"}, nil,
		),
		descJobsTotal: prometheus.NewDesc(
			"pipeline_jobs_total",
			"Total number of pipeline jobs grouped by status.",
			[]string{"status"}, nil,
		),
	}
}

// Describe sends all metric descriptors to the channel.
func (c *pipelineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.descRecordsProcessed
	ch <- c.descRecordsPending
	ch <- c.descPercentComplete
	ch <- c.descProcessingRate
	ch <- c.descStageLatency
	ch <- c.descErrorCount
	ch <- c.descJobsTotal
}

// Collect reads current metrics from the stores and emits them.
func (c *pipelineCollector) Collect(ch chan<- prometheus.Metric) {
	jobs := c.jobStore.List()

	// Tally jobs per status for the summary gauge.
	statusCounts := map[model.JobStatus]float64{
		model.StatusQueued:    0,
		model.StatusRunning:   0,
		model.StatusCompleted: 0,
		model.StatusFailed:    0,
		model.StatusCancelled: 0,
	}

	for _, job := range jobs {
		statusCounts[job.Status]++

		progress := c.progressTracker.GetProgress(job.ID)
		if progress == nil {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			c.descRecordsProcessed,
			prometheus.GaugeValue,
			float64(progress.RecordsProcessed),
			job.ID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.descRecordsPending,
			prometheus.GaugeValue,
			float64(progress.RecordsPending),
			job.ID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.descPercentComplete,
			prometheus.GaugeValue,
			float64(progress.PercentComplete),
			job.ID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.descProcessingRate,
			prometheus.GaugeValue,
			progress.ProcessingRate,
			job.ID,
		)

		for stage, latency := range progress.StageLatencies {
			ch <- prometheus.MustNewConstMetric(
				c.descStageLatency,
				prometheus.GaugeValue,
				latency,
				job.ID, stage,
			)
		}

		for stage, count := range progress.ErrorCounts {
			ch <- prometheus.MustNewConstMetric(
				c.descErrorCount,
				prometheus.GaugeValue,
				float64(count),
				job.ID, stage,
			)
		}
	}

	for status, count := range statusCounts {
		ch <- prometheus.MustNewConstMetric(
			c.descJobsTotal,
			prometheus.GaugeValue,
			count,
			string(status),
		)
	}
}
