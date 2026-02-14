package extractors

import (
	"context"
	"french_admin_etl/internal/model"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCSVExtractor_Extract(t *testing.T) {
	tests := []struct {
		name           string
		filePath       string
		delimiter      rune
		batchSize      int
		expectError    bool
		expectedCount  int // Expected number of records
		expectedColumn string
		expectedValue  string // Expected value for first record
	}{
		{
			name:           "Extract population CSV with semicolon delimiter",
			filePath:       "testdata/population.csv",
			delimiter:      ';',
			batchSize:      10,
			expectError:    false,
			expectedCount:  5, // Based on population.csv content
			expectedColumn: "AGE",
			expectedValue:  "Y_GE80",
		},
		{
			name:          "Non-existent file",
			filePath:      "testdata/nonexistent.csv",
			delimiter:     ',',
			batchSize:     10,
			expectError:   true,
			expectedCount: 0,
		},
		{
			name:           "Small batch size",
			filePath:       "testdata/population.csv",
			delimiter:      ';',
			batchSize:      1,
			expectError:    false,
			expectedCount:  5,
			expectedColumn: "TIME_PERIOD",
			expectedValue:  "2022",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create extractor
			extractor := NewCSVExtractorWithDelimiter(nil, tt.delimiter)

			// Extract records
			recordChan, err := extractor.Extract(ctx, tt.filePath, tt.batchSize)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Read all records
			var records []model.CSVRecord
			for record := range recordChan {
				records = append(records, record)
			}

			// Verify count
			if len(records) != tt.expectedCount {
				t.Errorf("Expected %d records, got %d", tt.expectedCount, len(records))
			}

			// Verify first record if we have records
			if len(records) > 0 && tt.expectedColumn != "" {
				firstRecord := records[0]
				if val, exists := firstRecord[tt.expectedColumn]; !exists {
					t.Errorf("Expected column %q not found in record", tt.expectedColumn)
				} else if tt.expectedValue != "" && val != tt.expectedValue {
					t.Errorf("Expected value %q for column %q, got %q", tt.expectedValue, tt.expectedColumn, val)
				}
			}
		})
	}
}

func TestCSVExtractor_Headers(t *testing.T) {
	ctx := context.Background()

	extractor := NewCSVExtractorWithDelimiter(nil, ';')
	recordChan, err := extractor.Extract(ctx, "testdata/population.csv", 10)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Get first record
	firstRecord := <-recordChan

	// Verify expected headers exist
	expectedHeaders := []string{"AGE", "GEO", "GEO_OBJECT", "RP_MEASURE", "SEX", "TIME_PERIOD", "OBS_VALUE"}
	for _, header := range expectedHeaders {
		if _, exists := firstRecord[header]; !exists {
			t.Errorf("Expected header %q not found in record", header)
		}
	}

	// Verify we have exactly the right number of columns
	if len(firstRecord) != len(expectedHeaders) {
		t.Errorf("Expected %d columns, got %d", len(expectedHeaders), len(firstRecord))
	}

	// Drain the channel
	for record := range recordChan {
		_ = record // Consume remaining records
	}
}

func TestCSVExtractor_DefaultDelimiter(t *testing.T) {
	// Create a temporary CSV file with comma delimiter
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.csv")

	content := "name,age,city\nJohn,30,Paris\nJane,25,Lyon\n"
	if err := os.WriteFile(tmpFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()

	// Use default extractor (comma delimiter)
	extractor := NewCSVExtractor(nil)
	recordChan, err := extractor.Extract(ctx, tmpFile, 10)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	var records []model.CSVRecord
	for record := range recordChan {
		records = append(records, record)
	}

	if len(records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(records))
	}

	// Verify first record
	if records[0]["name"] != "John" {
		t.Errorf("Expected name 'John', got %q", records[0]["name"])
	}
	if records[0]["age"] != "30" {
		t.Errorf("Expected age '30', got %q", records[0]["age"])
	}
	if records[0]["city"] != "Paris" {
		t.Errorf("Expected city 'Paris', got %q", records[0]["city"])
	}
}

func TestCSVExtractor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	extractor := NewCSVExtractorWithDelimiter(nil, ';')
	recordChan, err := extractor.Extract(ctx, "testdata/population.csv", 1)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Read first record
	<-recordChan

	// Cancel context
	cancel()

	// Channel should close shortly
	timeout := time.After(2 * time.Second)
	select {
	case _, ok := <-recordChan:
		if ok {
			// Still receiving, this is ok - drain until closed
			for record := range recordChan {
				_ = record // Consume remaining records
			}
		}
	case <-timeout:
		t.Error("Channel did not close after context cancellation")
	}
}

func TestCSVExtractor_MismatchedColumns(t *testing.T) {
	// Create a temporary CSV file with mismatched columns
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "mismatched.csv")

	content := "col1,col2,col3\nvalue1,value2,value3\nvalue1,value2\nvalue1,value2,value3,value4\n"
	if err := os.WriteFile(tmpFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()

	extractor := NewCSVExtractor(nil)
	recordChan, err := extractor.Extract(ctx, tmpFile, 10)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	var records []model.CSVRecord
	for record := range recordChan {
		records = append(records, record)
	}

	// Should only have valid records (first one with 3 columns)
	// The extractor skips records with mismatched column counts
	if len(records) != 1 {
		t.Errorf("Expected 1 valid record, got %d", len(records))
	}
}
