package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"french-admin-etl/internal/extractors"
	"french-admin-etl/internal/infrastructure/config"
	"french-admin-etl/internal/model"
	"french-admin-etl/internal/transformers"
)

// CsvETLProcessor handles the ETL process for CSV files with parallel processing.
type CsvETLProcessor[E any] struct {
	config         *config.Config
	name           string                        // name for logging
	extractor      *extractors.CSVExtractor      // Extractor to read CSV records
	csvTransformer model.CsvRecordTransformer[E] // Transformer to convert CSV records to entities
	entityLoader   model.EntityLoader[E]         // Loader to load entities into the database
}

// NewCsvETLProcessor creates a new CsvETLProcessor with the provided configuration, name, delimiter, filter, mapper, and loader.
func NewCsvETLProcessor[E any](
	config *config.Config,
	name string,
	delimiter rune,
	filter model.CsvRecordFilter,
	mapper model.Mapper[model.CSVRecord, E],
	loader model.EntityLoader[E],
) *CsvETLProcessor[E] {
	return &CsvETLProcessor[E]{
		config:         config,
		name:           name,
		extractor:      extractors.NewCSVExtractorWithDelimiter(filter, delimiter),
		csvTransformer: transformers.NewCsvRecordTransformer(mapper),
		entityLoader:   loader,
	}
}

// Run executes the ETL process for the given CSV file path, extracting records, transforming them into entities, and loading them into the database using parallel workers.
func (l *CsvETLProcessor[E]) Run(ctx context.Context, filePath string) error {
	recordChan, err := l.extractor.Extract(ctx, filePath, l.config.BatchSize)
	if err != nil {
		return fmt.Errorf("error extracting CSV records: %w", err)
	}

	// Load in parallel using streaming channel
	return l.loadParallelStream(ctx, recordChan)
}

func (l *CsvETLProcessor[E]) loadParallelStream(
	ctx context.Context,
	recordChan <-chan model.CSVRecord,
) error {
	start := time.Now()

	// Channel to distribute work batches
	jobs := make(chan []model.CSVRecord, l.config.Workers)

	// WaitGroup to wait for all workers
	var wg sync.WaitGroup

	// Stats
	var processed, failed int
	var mu sync.Mutex

	// Launch workers
	for w := 0; w < l.config.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range jobs {
				batchStart := time.Now()
				n, err := l.loadBatch(ctx, batch)

				mu.Lock()
				processed += n
				failed += len(batch) - n
				if err != nil {
					slog.Error("Batch error", "workerID", workerID, "total", len(batch), "error", err)
				} else {
					if n < len(batch) {
						slog.Warn("Partial batch", "workerID", workerID, "loaded", n, "total", len(batch), "duration", time.Since(batchStart))
					} else {
						slog.Info("Batch success", "workerID", workerID, "loaded", n, "duration", time.Since(batchStart))
					}
				}
				mu.Unlock()
			}
		}(w)
	}

	// Batch records from stream and distribute to workers
	go func() {
		defer close(jobs)

		batch := make([]model.CSVRecord, 0, l.config.BatchSize)
		for record := range recordChan {
			batch = append(batch, record)

			// Send batch when full
			if len(batch) >= l.config.BatchSize {
				select {
				case jobs <- batch:
					batch = make([]model.CSVRecord, 0, l.config.BatchSize)
				case <-ctx.Done():
					return
				}
			}
		}

		// Send remaining records
		if len(batch) > 0 {
			select {
			case jobs <- batch:
			case <-ctx.Done():
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	duration := time.Since(start)
	rate := float64(processed) / duration.Seconds()

	slog.Info("Results Breakdown", "dataset", l.name, "success", processed, "failed", failed, "duration", duration, "throughput", fmt.Sprintf("%.0f records/sec", rate))
	return nil
}

func (l *CsvETLProcessor[E]) loadBatch(ctx context.Context, records []model.CSVRecord) (int, error) {
	entities, err := l.csvTransformer.Transform(records)
	if err != nil {
		return 0, err
	}

	count, err := l.entityLoader.Load(ctx, entities)
	if err != nil {
		return 0, err
	}

	return count, nil
}
