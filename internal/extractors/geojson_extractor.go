package extractors

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"french_admin_etl/internal/model"
)

type GeoJSONExtractor[T any] struct {
}

func NewGeoJSONExtractor[T any]() *GeoJSONExtractor[T] {
	return &GeoJSONExtractor[T]{}
}

func (e *GeoJSONExtractor[T]) loadFile(filePath string) (file *os.File, decoder *json.Decoder, err error) {
	// Open file for streaming
	file, err = os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	// Create JSON decoder for streaming
	decoder = json.NewDecoder(file)
	return file, decoder, nil
}

// parse reads the GeoJSON file and sends features to the channel
func (e *GeoJSONExtractor[T]) parse(ctx context.Context, decoder *json.Decoder, featureChan chan model.GeoJSONFeature[T], factory func() T) {
	for decoder.More() {
		// Read key
		token, err := decoder.Token()
		if err != nil {
			slog.Error("Reading token", "error", err)
			return
		}

		key, ok := token.(string)
		if !ok {
			continue
		}

		// Skip until we find "features" array
		if key == "features" {
			// Read array opening bracket
			if _, err := decoder.Token(); err != nil {
				slog.Error("Reading array start", "error", err)
				return
			}

			// Stream each feature
			for decoder.More() {
				// Use factory to create a new instance with the correct type
				feature := model.GeoJSONFeature[T]{Properties: factory()}
				if err := decoder.Decode(&feature); err != nil {
					slog.Error("Decoding feature", "error", err)
					return
				}

				select {
				case featureChan <- feature:
				case <-ctx.Done():
					return
				}
			}
			return
		} else {
			// Skip other fields (type, etc.)
			var discard any
			if err := decoder.Decode(&discard); err != nil {
				slog.Warn("Skipping field", "field", key, "error", err)
				return
			}
		}
	}
}

func (e *GeoJSONExtractor[T]) Extract(ctx context.Context, filePath string, batchSize int, factory func() T) (chan model.GeoJSONFeature[T], error) {
	file, decoder, err := e.loadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	// Read opening brace
	if _, err := decoder.Token(); err != nil {
		file.Close()
		return nil, fmt.Errorf("error reading opening brace: %w", err)
	}

	// Create channel to stream features
	featureChan := make(chan model.GeoJSONFeature[T], batchSize*2)

	go func() {
		defer func() {
			file.Close() // Close file when goroutine finishes reading
			close(featureChan)
		}()
		e.parse(ctx, decoder, featureChan, factory)
	}()

	return featureChan, nil
}
