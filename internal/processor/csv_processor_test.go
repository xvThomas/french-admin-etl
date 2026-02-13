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

type mockPopulationEntityLoader struct{}

var _ model.EntityLoader[entities.CommunePopulationPrincEntity] = (*mockPopulationEntityLoader)(nil)

func NewMockPopulationEntityLoader() *mockPopulationEntityLoader {
	return &mockPopulationEntityLoader{}
}

func (m *mockPopulationEntityLoader) Load(_ context.Context, entities []entities.CommunePopulationPrincEntity) (int, error) {
	for _, entity := range entities {
		log.Printf("Loaded entity: Age=%s, CodeCommune=%s, Sexe=%s, Annee=%d, Population=%d",
			entity.Age, entity.CodeCommune, entity.Sexe, entity.Annee, entity.Population)
	}
	return len(entities), nil
}

// Create CSV ETL processor for population data
func newCsvEtlProcessor(config *config.Config) *CsvETLProcessor[entities.CommunePopulationPrincEntity] {
	return NewCsvETLProcessor(
		config,
		"Test Population",
		';', // semicolon delimiter for French CSV
		nil, // no filter
		entities.NewCommunePopulationMapper(),
		NewMockPopulationEntityLoader(),
	)
}

func TestCsvETLProcessor_Run(t *testing.T) {
	tests := []struct {
		name          string
		filePath      string
		workers       int
		batchSize     int
		expectError   bool
		expectMinimum int // Minimum number of records expected
	}{
		{
			name:          "Run valid population CSV",
			filePath:      "testdata/population.csv",
			workers:       2,
			batchSize:     10,
			expectError:   false,
			expectMinimum: 1, // At least one record should be loaded
		},
		{
			name:          "Run with single worker",
			filePath:      "testdata/population.csv",
			workers:       1,
			batchSize:     5,
			expectError:   false,
			expectMinimum: 1,
		},
		{
			name:          "Run with multiple workers and large batch",
			filePath:      "testdata/population.csv",
			workers:       4,
			batchSize:     50,
			expectError:   false,
			expectMinimum: 1,
		},
		{
			name:        "Non-existent file",
			filePath:    "testdata/nonexistent.csv",
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

			// Create CSV ETL processor
			processor := newCsvEtlProcessor(config)

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Execute run
			err := processor.Run(ctx, tt.filePath)

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

func TestCsvETLProcessor_RunWithContext(t *testing.T) {
	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	processor := newCsvEtlProcessor(config)

	t.Run("Context cancellation", func(t *testing.T) {
		// Create context that will be cancelled immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := processor.Run(ctx, "testdata/population.csv")
		// The error might be nil if cancellation happens after processing starts
		// We just verify it doesn't panic
		t.Logf("Run with cancelled context returned: %v", err)
	})

	t.Run("Context timeout", func(t *testing.T) {
		// Create context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond) // Ensure timeout expires

		err := processor.Run(ctx, "testdata/population.csv")
		// Similar to above - verify no panic
		t.Logf("Run with timed out context returned: %v", err)
	})
}

func TestCsvETLProcessor_RunStreamingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	config := &config.Config{
		Workers:   4,
		BatchSize: 100,
	}

	processor := newCsvEtlProcessor(config)
	ctx := context.Background()

	start := time.Now()
	err := processor.Run(ctx, "testdata/population.csv")
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

func TestNewCsvETLProcessor(t *testing.T) {
	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	processor := newCsvEtlProcessor(config)

	if processor == nil {
		t.Fatal("NewCsvETLProcessor returned nil")
	}

	if processor.config != config {
		t.Error("Config not properly set")
	}

	if processor.extractor == nil {
		t.Error("Extractor not properly set")
	}

	if processor.csvTransformer == nil {
		t.Error("CSV transformer not properly set")
	}

	if processor.entityLoader == nil {
		t.Error("Entity loader not properly set")
	}
}

func TestCsvETLProcessor_RunInvalidCSV(t *testing.T) {
	// Create a temporary invalid CSV file
	tmpDir := t.TempDir()
	invalidFile := filepath.Join(tmpDir, "invalid.csv")

	// Write invalid CSV (unmatched quotes, wrong number of columns)
	content := []byte("col1;col2;col3\nvalue1;value2\nvalue1;value2;value3;value4\n")
	if err := writeTestFile(invalidFile, content); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	processor := newCsvEtlProcessor(config)
	ctx := context.Background()

	err := processor.Run(ctx, invalidFile)
	// Should handle invalid CSV gracefully (may not return error due to streaming nature)
	t.Logf("Run with invalid CSV returned: %v", err)
}

func TestCsvETLProcessor_RunEmptyCSV(t *testing.T) {
	// Create a temporary empty CSV (only header)
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.csv")

	// Write only header
	content := []byte("AGE;GEO;GEO_OBJECT;RP_MEASURE;SEX;TIME_PERIOD;OBS_VALUE\n")
	if err := writeTestFile(emptyFile, content); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	config := &config.Config{
		Workers:   2,
		BatchSize: 10,
	}

	processor := newCsvEtlProcessor(config)
	ctx := context.Background()

	err := processor.Run(ctx, emptyFile)
	if err != nil {
		t.Errorf("Run of empty CSV failed: %v", err)
	}
}

func TestCsvETLProcessor_RunWithDifferentDelimiters(t *testing.T) {
	// Create a CSV with comma delimiter
	tmpDir := t.TempDir()
	commaFile := filepath.Join(tmpDir, "comma.csv")

	content := []byte("col1,col2,col3\nvalue1,value2,value3\n")
	if err := writeTestFile(commaFile, content); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	config := &config.Config{
		Workers:   1,
		BatchSize: 10,
	}

	// Create processor with comma delimiter
	processor := NewCsvETLProcessor(
		config,
		"Test Comma CSV",
		',', // comma delimiter
		nil, // no filter
		entities.NewCommunePopulationMapper(),
		NewMockPopulationEntityLoader(),
	)

	ctx := context.Background()
	err := processor.Run(ctx, commaFile)

	// Will likely fail to map properly (wrong columns), but should not panic
	t.Logf("Run with comma delimiter returned: %v", err)
}

func TestCsvETLProcessor_RunLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	// Create a temporary large CSV file
	tmpDir := t.TempDir()
	largeFile := filepath.Join(tmpDir, "large.csv")

	// Create file with multiple records
	f, err := os.Create(largeFile)
	if err != nil {
		t.Fatalf("Failed to create large file: %v", err)
	}
	defer f.Close()

	// Write header
	f.WriteString("AGE;GEO;GEO_OBJECT;RP_MEASURE;SEX;TIME_PERIOD;OBS_VALUE\n")

	// Write 1000 records
	for i := 0; i < 1000; i++ {
		f.WriteString("Y_GE80;75101;COM;POP;_T;2022;100\n")
	}

	config := &config.Config{
		Workers:   4,
		BatchSize: 50,
	}

	processor := newCsvEtlProcessor(config)
	ctx := context.Background()

	start := time.Now()
	err = processor.Run(ctx, largeFile)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	t.Logf("Processed 1000 records in %v", duration)
}

// Helper function to write test files
func writeTestFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0644)
}
