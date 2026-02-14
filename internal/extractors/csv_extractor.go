// Package extractors provides utilities for extracting data from various file formats.
package extractors

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"

	"french-admin-etl/internal/model"
)

// CSVExtractor extracts records from CSV files with configurable delimiters and filters.
type CSVExtractor struct {
	Delimiter rune
	filter    model.CsvRecordFilter
}

// NewCSVExtractor creates a new CSV extractor with comma as the default delimiter.
func NewCSVExtractor(filter model.CsvRecordFilter) *CSVExtractor {
	return &CSVExtractor{
		Delimiter: ',', // Default delimiter
		filter:    filter,
	}
}

// NewCSVExtractorWithDelimiter creates a new CSV extractor with a custom delimiter.
func NewCSVExtractorWithDelimiter(filter model.CsvRecordFilter, delimiter rune) *CSVExtractor {
	return &CSVExtractor{
		Delimiter: delimiter,
		filter:    filter,
	}
}

func (e *CSVExtractor) loadFile(filePath string) (file *os.File, reader *csv.Reader, headers []string, err error) {
	// Open file for reading
	// #nosec G304 -- filePath is controlled by the application, not user input
	file, err = os.Open(filePath)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create CSV reader
	reader = csv.NewReader(file)
	reader.Comma = e.Delimiter
	reader.TrimLeadingSpace = true

	// Read header line to get column names
	headers, err = reader.Read()
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, fmt.Errorf("error reading CSV header: %w", err)
	}

	return file, reader, headers, nil
}

// parse reads the CSV file and sends records to the channel
func (e *CSVExtractor) parse(ctx context.Context, reader *csv.Reader, headers []string, recordChan chan model.CSVRecord) {
	lineNumber := 1 // Start at 1 because header is line 0

	for {
		// Read next record
		values, err := reader.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			slog.Error("Reading CSV record", "line", lineNumber, "error", err)
			return
		}

		lineNumber++

		// Check that the number of values matches the number of headers
		if len(values) != len(headers) {
			slog.Warn("CSV record has different number of columns than header",
				"line", lineNumber,
				"expected", len(headers),
				"got", len(values))
			continue
		}

		// Create CSVRecord (map[string]string) with column names as keys
		record := make(model.CSVRecord)
		for i, header := range headers {
			record[header] = values[i]
		}

		if e.filter != nil && !e.filter.Filter(record) {
			// slog.Debug("Record filtered out", "line", lineNumber, "record", record)
			continue
		}

		select {
		case recordChan <- record:
		case <-ctx.Done():
			return
		}
	}
}

// Extract reads a CSV file and streams records through a channel with optional filtering.
func (e *CSVExtractor) Extract(ctx context.Context, filePath string, batchSize int) (chan model.CSVRecord, error) {
	file, reader, headers, err := e.loadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: %w", err)
	}

	slog.Info("CSV file opened", "file", filePath, "columns", len(headers), "headers", headers)

	// Create channel to stream records
	recordChan := make(chan model.CSVRecord, batchSize*2)

	go func() {
		defer func() {
			_ = file.Close() // Close file when goroutine finishes reading
			close(recordChan)
		}()
		e.parse(ctx, reader, headers, recordChan)
	}()

	return recordChan, nil
}
