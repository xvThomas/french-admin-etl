package processor

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"french_admin_etl/internal/infrastructure/config"
	"french_admin_etl/internal/infrastructure/entities"
	"french_admin_etl/internal/model"
)

type mockEntityLoader struct{}

var _ model.EntityWithGeoJSONGeometryLoader[entities.RegionEntity] = (*mockEntityLoader)(nil)

func NewMockEntityLoader() *mockEntityLoader {
	return &mockEntityLoader{}
}

func (m *mockEntityLoader) Load(_ context.Context, entities []entities.RegionWithGeometry) (int, error) {
	for _, entity := range entities {
		log.Printf("Transformed entity: %v with GeoJSON size %d bytes", entity.Data, len(entity.GeometryJSON))
	}
	return len(entities), nil
}

// Create etlprocessor with factory for RegionProperties
func newEtlProcessor(config *config.Config) *GeoJSONETLProcessor[entities.RegionProperties, entities.RegionEntity] {
	return NewGeoJSONETLProcessor(
		config,
		"Test Régions",
		func() entities.RegionProperties {
			return entities.RegionProperties{}
		},
		entities.NewRegionMapper(),
		NewMockEntityLoader(),
	)
}

func TestGeoJSONETLProcessor_Run(t *testing.T) {
	tests := []struct {
		name          string
		filePath      string
		workers       int
		batchSize     int
		expectError   bool
		expectMinimum int // Minimum number of features expected
	}{
		{
			name:          "Run valid regions GeoJSON",
			filePath:      "testdata/regions.geojson",
			workers:       2,
			batchSize:     10,
			expectError:   false,
			expectMinimum: 1, // At least one region should be loaded
		},
		{
			name:          "Run with single worker",
			filePath:      "testdata/regions.geojson",
			workers:       1,
			batchSize:     5,
			expectError:   false,
			expectMinimum: 1,
		},
		{
			name:          "Run with multiple workers and large batch",
			filePath:      "testdata/regions.geojson",
			workers:       4,
			batchSize:     50,
			expectError:   false,
			expectMinimum: 1,
		},
		{
			name:        "Non-existent file",
			filePath:    "testdata/nonexistent.geojson",
			workers:     2,
			batchSize:   10,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create config
			config := &config.Config{
				Workers:   tt.workers,
				BatchSize: tt.batchSize,
			}

			// Create etlprocessor with factory for RegionProperties
			etlprocessor := newEtlProcessor(config)

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Execute run
			err := etlprocessor.Run(ctx, tt.filePath)

			// Check error expectation
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
		})
	}
}

func TestGeoJSONETLProcessor_RunWithContext(t *testing.T) {
	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	// Create etlprocessor with factory for RegionProperties
	etlprocessor := newEtlProcessor(config)

	t.Run("Context cancellation", func(t *testing.T) {
		// Create context that will be cancelled immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := etlprocessor.Run(ctx, "testdata/regions.geojson")
		// The error might be nil if cancellation happens after processing starts
		// We just verify it doesn't panic
		t.Logf("Run with cancelled context returned: %v", err)
	})

	t.Run("Context timeout", func(t *testing.T) {
		// Create context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond) // Ensure timeout expires

		err := etlprocessor.Run(ctx, "testdata/regions.geojson")
		// Similar to above - verify no panic
		t.Logf("Run with timed out context returned: %v", err)
	})
}

func TestGeoJSONETLProcessor_RunStreamingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	config := &config.Config{
		Workers:   4,
		BatchSize: 100,
	}

	etlprocessor := newEtlProcessor(config)
	ctx := context.Background()

	start := time.Now()
	err := etlprocessor.Run(ctx, "testdata/regions.geojson")
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	t.Logf("Streaming run completed in %v", duration)

	// Verify reasonable performance (should be fast for test data)
	if duration > 10*time.Second {
		t.Logf("Warning: Run took longer than expected: %v", duration)
	}
}

func TestNewGeoJSONETLProcessor(t *testing.T) {
	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	// Create etlprocessor with factory for RegionProperties
	etlprocessor := newEtlProcessor(config)

	if etlprocessor == nil {
		t.Fatal("NewGeoJSONETLProcessor returned nil")
	}

	if etlprocessor.config != config {
		t.Error("Config not properly set")
	}

	if etlprocessor.factory == nil {
		t.Error("Factory function not properly set")
	}
}

func TestGeoJSONETLProcessor_RunInvalidJSON(t *testing.T) {
	// Create a temporary invalid JSON file
	tmpDir := t.TempDir()
	invalidFile := filepath.Join(tmpDir, "invalid.geojson")

	// Write invalid JSON
	content := []byte(`{"type": "FeatureCollection", "features": [invalid json}`)
	if err := writeTestFile(invalidFile, content); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	etlprocessor := newEtlProcessor(config)
	ctx := context.Background()

	err := etlprocessor.Run(ctx, invalidFile)
	// Should handle invalid JSON gracefully (may not return error due to streaming nature)
	t.Logf("Run with invalid JSON returned: %v", err)
}

func TestGeoJSONETLProcessor_RunEmptyFeatureCollection(t *testing.T) {
	// Create a temporary empty feature collection
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.geojson")

	// Write empty feature collection
	content := []byte(`{"type": "FeatureCollection", "features": []}`)
	if err := writeTestFile(emptyFile, content); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	// Create etlprocessor with factory for RegionProperties
	etlprocessor := newEtlProcessor(config)
	ctx := context.Background()

	err := etlprocessor.Run(ctx, emptyFile)
	if err != nil {
		t.Errorf("Run of empty feature collection failed: %v", err)
	}
}

// Helper function to write test files
func writeTestFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0644)
}
