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

// GeoJSONETLProcessor handles the ETL process for GeoJSON files with parallel processing.
type GeoJSONETLProcessor[T any, E any] struct {
	config             *config.Config
	name               string                                   // name for logging
	factory            func() T                                 // Factory function to create empty instances for JSON unmarshalling
	extractor          *extractors.GeoJSONExtractor[T]          // Embedded extractor to read GeoJSON features
	geoJSONTransformer model.GeoJSONTransformer[T, E]           // Transformer to convert GeoJSON features to entities with WKB
	entityLoader       model.EntityWithGeoJSONGeometryLoader[E] // Loader to load entities with WKB into the database
}

// NewGeoJSONETLProcessor creates a new GeoJSONETLProcessor with the provided configuration, name, factory, mapper, and loader.
func NewGeoJSONETLProcessor[T any, E any](
	config *config.Config,
	name string,
	factory func() T,
	mapper model.Mapper[T, E],
	loader model.EntityWithGeoJSONGeometryLoader[E],
) *GeoJSONETLProcessor[T, E] {
	return &GeoJSONETLProcessor[T, E]{
		config:             config,
		name:               name,
		factory:            factory,
		extractor:          extractors.NewGeoJSONExtractor[T](),
		geoJSONTransformer: transformers.NewGeoJSONTransformer(mapper),
		entityLoader:       loader,
	}
}

// Run executes the ETL process for the given GeoJSON file path, extracting features, transforming them into entities, and loading them into the database using parallel workers.
func (l *GeoJSONETLProcessor[T, E]) Run(ctx context.Context, filePath string) error {
	featureChan, err := l.extractor.Extract(ctx, filePath, l.config.BatchSize, l.factory)
	if err != nil {
		return fmt.Errorf("error extracting features: %w", err)
	}

	// Load in parallel using streaming channel
	return l.loadParallelStream(ctx, featureChan)
}

func (l *GeoJSONETLProcessor[T, E]) loadParallelStream(
	ctx context.Context,
	featureChan <-chan model.GeoJSONFeature[T],
) error {
	start := time.Now()

	// Channel to distribute work batches
	jobs := make(chan []model.GeoJSONFeature[T], l.config.Workers)

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

	// Batch features from stream and distribute to workers
	go func() {
		defer close(jobs)

		batch := make([]model.GeoJSONFeature[T], 0, l.config.BatchSize)
		for feature := range featureChan {
			batch = append(batch, feature)

			// Send batch when full
			if len(batch) >= l.config.BatchSize {
				select {
				case jobs <- batch:
					batch = make([]model.GeoJSONFeature[T], 0, l.config.BatchSize)
				case <-ctx.Done():
					return
				}
			}
		}

		// Send remaining features
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

	slog.Info("Results Breakdown", "dataset", l.name, "success", processed, "failed", failed, "duration", duration, "throughput", fmt.Sprintf("%.0f features/sec", rate))
	return nil
}

func (l *GeoJSONETLProcessor[T, E]) loadBatch(ctx context.Context, features []model.GeoJSONFeature[T]) (int, error) {
	entities, err := l.geoJSONTransformer.Transform(features)
	if err != nil {
		return 0, err
	}

	count, err := l.entityLoader.Load(ctx, entities)
	if err != nil {
		return 0, err
	}

	return count, nil
}
