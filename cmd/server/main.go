package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jitendraj/data-pipeline/internal/api"
	"github.com/jitendraj/data-pipeline/internal/config"
	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/pipeline"
	"github.com/jitendraj/data-pipeline/internal/store"
)

func main() {
	// Determine listen port from environment, default to :8080
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8080"
	} else if port[0] != ':' {
		port = ":" + port
	}

	// Initialize shared stores
	jobStore := store.NewInMemoryJobStore()
	errorStore := store.NewInMemoryErrorStore()
	progressTracker := store.NewProgressTracker()
	resultStore := api.NewInMemoryResultStore()

	// Create API handler with all dependencies
	handler := &api.Handler{
		JobStore:        jobStore,
		ErrorStore:      errorStore,
		ProgressTracker: progressTracker,
		ResultStore:     resultStore,
	}

	// Create pipeline runner that wires pipeline execution
	runner := &pipelineRunner{
		jobStore:        jobStore,
		errorStore:      errorStore,
		progressTracker: progressTracker,
		resultStore:     resultStore,
		handler:         handler,
	}
	handler.Runner = runner

	// Create router with all routes and middleware
	router := api.NewRouter(handler)

	// Create HTTP server
	srv := &http.Server{
		Addr:    port,
		Handler: router,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting server on %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Give outstanding requests 5 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped")
}

// pipelineRunner implements the api.PipelineRunner interface.
// It builds and runs pipelines asynchronously for created jobs.
type pipelineRunner struct {
	jobStore        store.JobStore
	errorStore      store.ErrorStore
	progressTracker store.ProgressTracker
	resultStore     api.ResultStore
	handler         *api.Handler
}

// RunJob starts asynchronous pipeline execution for the given job ID.
func (r *pipelineRunner) RunJob(jobID string) {
	go func() {
		job, err := r.jobStore.Get(jobID)
		if err != nil {
			log.Printf("Failed to get job %s for execution: %v", jobID, err)
			return
		}

		// Apply environment variable overrides to the config
		cfg := job.Config
		config.ApplyEnvOverrides(&cfg)
		job.Config = cfg

		// Build sources from configuration
		sources := buildSources(job, r.errorStore)

		// Build export targets from configuration
		exportTargets, cleanupFns := buildExportTargets(job)
		defer func() {
			for _, fn := range cleanupFns {
				fn()
			}
		}()

		// Create the pipeline
		p := pipeline.NewPipeline(
			job,
			r.jobStore,
			sources,
			exportTargets,
			r.errorStore,
			r.progressTracker,
		)

		// Create a cancellable context and store the cancel func for API cancellation
		ctx, cancel := context.WithCancel(context.Background())

		// If timeout is configured, use a context with deadline instead
		if job.Config.TimeoutSeconds != nil {
			timeout := *job.Config.TimeoutSeconds
			if timeout >= 1 && timeout <= 86400 {
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			}
		}

		// Store cancel function so the API can cancel this job
		r.handler.CancelFuncs.Store(jobID, cancel)

		// Run the pipeline
		if err := p.Run(ctx); err != nil {
			log.Printf("Pipeline job %s finished with error: %v", jobID, err)
		} else {
			log.Printf("Pipeline job %s completed successfully", jobID)
		}

		// Clean up cancel function
		r.handler.CancelFuncs.Delete(jobID)
		cancel()
	}()
}

// buildSources creates pipeline Source instances from the job configuration.
func buildSources(job *model.Job, errorStore store.ErrorStore) []pipeline.Source {
	var sources []pipeline.Source
	for _, src := range job.Config.Sources {
		switch src.Type {
		case "csv":
			sources = append(sources, &pipeline.CSVSource{
				FilePath:   src.Path,
				JobID:      job.ID,
				ErrorStore: errorStore,
			})
		case "json":
			sources = append(sources, pipeline.NewJSONSource(src.Path))
		case "http":
			timeout := src.TimeoutSeconds
			sources = append(sources, pipeline.NewHTTPSource(src.Path, timeout))
		}
	}
	return sources
}

// buildExportTargets creates ExportTarget instances from the job configuration.
// Returns the targets and a slice of cleanup functions to call when done.
func buildExportTargets(job *model.Job) ([]export.ExportTarget, []func()) {
	var targets []export.ExportTarget
	var cleanups []func()

	for _, exp := range job.Config.Exports {
		switch exp.Type {
		case "sqlite":
			tableName := exp.TableName
			if tableName == "" {
				tableName = "results"
			}
			target, err := export.NewSQLiteTarget(exp.Path, tableName)
			if err != nil {
				log.Printf("Failed to create SQLite target %s: %v", exp.Path, err)
				continue
			}
			targets = append(targets, target)
			cleanups = append(cleanups, func() { target.Close() })
		case "csv":
			targets = append(targets, export.NewCSVTarget(exp.Path))
		case "json":
			targets = append(targets, export.NewJSONTarget(exp.Path))
		case "postgres":
			tableName := exp.TableName
			if tableName == "" {
				tableName = "results"
			}
			// exp.Path holds the DSN; fall back to POSTGRES_DSN env var if blank.
			dsn := exp.Path
			if dsn == "" {
				dsn = os.Getenv("POSTGRES_DSN")
			}
			if dsn == "" {
				log.Printf("Skipping postgres export target: no DSN provided (set path or POSTGRES_DSN)")
				continue
			}
			target, err := export.NewPostgresTarget(dsn, tableName)
			if err != nil {
				log.Printf("Failed to create postgres target: %v", err)
				continue
			}
			targets = append(targets, target)
			cleanups = append(cleanups, func() { target.Close() })
		}
	}
	return targets, cleanups
}
