package extractors

import (
	"context"
	"french-admin-etl/internal/infrastructure/entities"
	"french-admin-etl/internal/model"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestGeoJSONExtractor_Extract tests the main extraction functionality
func TestGeoJSONExtractor_Extract(t *testing.T) {
	tests := []struct {
		name          string
		filePath      string
		batchSize     int
		expectError   bool
		expectedCount int
		expectedCode  string
		expectedName  string
	}{
		{
			name:          "Extract regions GeoJSON",
			filePath:      "testdata/regions.geojson",
			batchSize:     10,
			expectError:   false,
			expectedCount: 1, // regions.geojson has 1 feature (Guadeloupe)
			expectedCode:  "01",
			expectedName:  "Guadeloupe",
		},
		{
			name:          "Non-existent file",
			filePath:      "testdata/nonexistent.geojson",
			batchSize:     10,
			expectError:   true,
			expectedCount: 0,
		},
		{
			name:          "Small batch size",
			filePath:      "testdata/regions.geojson",
			batchSize:     1,
			expectError:   false,
			expectedCount: 1,
			expectedCode:  "01",
			expectedName:  "Guadeloupe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create extractor
			extractor := NewGeoJSONExtractor[entities.RegionProperties]()

			// Factory function for RegionProperties
			factory := func() entities.RegionProperties {
				return entities.RegionProperties{}
			}

			// Extract features
			featureChan, err := extractor.Extract(ctx, tt.filePath, tt.batchSize, factory)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Read all features
			var features []model.GeoJSONFeature[entities.RegionProperties]
			for feature := range featureChan {
				features = append(features, feature)
			}

			// Verify count
			if len(features) != tt.expectedCount {
				t.Errorf("Expected %d features, got %d", tt.expectedCount, len(features))
			}

			// Verify first feature if we have features
			if len(features) > 0 && tt.expectedCode != "" {
				firstFeature := features[0]
				if firstFeature.Properties.Code != tt.expectedCode {
					t.Errorf("Expected code %q, got %q", tt.expectedCode, firstFeature.Properties.Code)
				}
				if tt.expectedName != "" && firstFeature.Properties.Nom != tt.expectedName {
					t.Errorf("Expected name %q, got %q", tt.expectedName, firstFeature.Properties.Nom)
				}
			}
		})
	}
}

// TestGeoJSONExtractor_FeatureProperties tests that feature properties are correctly parsed
func TestGeoJSONExtractor_FeatureProperties(t *testing.T) {
	ctx := context.Background()

	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, "testdata/regions.geojson", 10, factory)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Get first feature
	firstFeature := <-featureChan

	// Verify properties
	if firstFeature.Properties.Code != "01" {
		t.Errorf("Expected code '01', got %q", firstFeature.Properties.Code)
	}
	if firstFeature.Properties.Nom != "Guadeloupe" {
		t.Errorf("Expected name 'Guadeloupe', got %q", firstFeature.Properties.Nom)
	}

	// Verify feature type
	if firstFeature.Type != "Feature" {
		t.Errorf("Expected type 'Feature', got %q", firstFeature.Type)
	}

	// Verify geometry type is not empty
	if firstFeature.Geometry.Type == "" {
		t.Error("Expected geometry to have a type")
	}

	// Drain the channel
	for feature := range featureChan {
		_ = feature // Consume remaining features
	}
}

// TestGeoJSONExtractor_Geometry tests that geometry is correctly parsed
func TestGeoJSONExtractor_Geometry(t *testing.T) {
	ctx := context.Background()

	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, "testdata/regions.geojson", 10, factory)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Get first feature
	firstFeature := <-featureChan

	// Verify geometry structure
	if firstFeature.Geometry.Type != "MultiPolygon" {
		t.Errorf("Expected geometry type 'MultiPolygon', got %v", firstFeature.Geometry.Type)
	}

	// Verify geometry has coordinates
	if firstFeature.Geometry.Coordinates == nil {
		t.Error("Expected geometry to have coordinates")
	}

	// Drain the channel
	for feature := range featureChan {
		_ = feature
	}
}

// TestGeoJSONExtractor_ContextCancellation tests that extraction stops on context cancellation
func TestGeoJSONExtractor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, "testdata/regions.geojson", 10, factory)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Cancel context immediately
	cancel()

	// Try to read from channel (should close soon)
	timeout := time.After(2 * time.Second)
	channelClosed := false

	for !channelClosed {
		select {
		case _, ok := <-featureChan:
			if !ok {
				channelClosed = true
			}
		case <-timeout:
			t.Error("Channel did not close after context cancellation")
			return
		}
	}

	if !channelClosed {
		t.Error("Expected channel to be closed after context cancellation")
	}
}

// TestGeoJSONExtractor_InvalidJSON tests handling of malformed JSON
func TestGeoJSONExtractor_InvalidJSON(t *testing.T) {
	// Create temporary file with invalid JSON
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "invalid.geojson")

	invalidJSON := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": { "code": "01"
				// Missing closing brace
			}
		]
	}`

	if err := os.WriteFile(tmpFile, []byte(invalidJSON), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()
	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, tmpFile, 10, factory)
	if err != nil {
		// It's acceptable to fail immediately on invalid JSON
		return
	}

	// If extraction doesn't fail immediately, channel should close without sending valid features
	featuresReceived := 0
	for range featureChan {
		featuresReceived++
	}

	// We expect 0 features from invalid JSON
	if featuresReceived > 0 {
		t.Errorf("Expected 0 features from invalid JSON, got %d", featuresReceived)
	}
}

// TestGeoJSONExtractor_EmptyFeatureCollection tests extraction of empty feature collection
func TestGeoJSONExtractor_EmptyFeatureCollection(t *testing.T) {
	// Create temporary file with empty features array
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "empty.geojson")

	emptyJSON := `{
		"type": "FeatureCollection",
		"features": []
	}`

	if err := os.WriteFile(tmpFile, []byte(emptyJSON), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()
	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, tmpFile, 10, factory)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Read all features
	var features []model.GeoJSONFeature[entities.RegionProperties]
	for feature := range featureChan {
		features = append(features, feature)
	}

	// Verify we got 0 features
	if len(features) != 0 {
		t.Errorf("Expected 0 features from empty collection, got %d", len(features))
	}
}

// TestGeoJSONExtractor_FactoryFunction tests that the factory function is used correctly
func TestGeoJSONExtractor_FactoryFunction(t *testing.T) {
	ctx := context.Background()

	extractor := NewGeoJSONExtractor[entities.RegionProperties]()

	factoryCalled := false
	factory := func() entities.RegionProperties {
		factoryCalled = true
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, "testdata/regions.geojson", 10, factory)
	if err != nil {
		t.Fatalf("Failed to extract: %v", err)
	}

	// Read at least one feature to ensure factory is called
	<-featureChan

	if !factoryCalled {
		t.Error("Factory function was not called")
	}

	// Drain the channel
	for f := range featureChan {
		_ = f
	}
}

// TestGeoJSONExtractor_MultipleFeatures tests handling of multiple features
func TestGeoJSONExtractor_MultipleFeatures(t *testing.T) {
	// Create temporary file with multiple features
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "multiple.geojson")

	multipleJSON := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": { "code": "01", "nom": "Region 1" },
				"geometry": { "type": "Point", "coordinates": [0, 0] }
			},
			{
				"type": "Feature",
				"properties": { "code": "02", "nom": "Region 2" },
				"geometry": { "type": "Point", "coordinates": [1, 1] }
			},
			{
				"type": "Feature",
				"properties": { "code": "03", "nom": "Region 3" },
				"geometry": { "type": "Point", "coordinates": [2, 2] }
			}
		]
	}`

	if err := os.WriteFile(tmpFile, []byte(multipleJSON), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()
	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	featureChan, err := extractor.Extract(ctx, tmpFile, 10, factory)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Read all features
	var features []model.GeoJSONFeature[entities.RegionProperties]
	for feature := range featureChan {
		features = append(features, feature)
	}

	// Verify count
	if len(features) != 3 {
		t.Errorf("Expected 3 features, got %d", len(features))
	}

	// Verify each feature has correct properties
	expectedCodes := []string{"01", "02", "03"}
	expectedNames := []string{"Region 1", "Region 2", "Region 3"}

	for i, feature := range features {
		if feature.Properties.Code != expectedCodes[i] {
			t.Errorf("Feature %d: expected code %q, got %q", i, expectedCodes[i], feature.Properties.Code)
		}
		if feature.Properties.Nom != expectedNames[i] {
			t.Errorf("Feature %d: expected name %q, got %q", i, expectedNames[i], feature.Properties.Nom)
		}
	}
}

// TestGeoJSONExtractor_ChannelBufferSize tests that batch size affects channel buffer
func TestGeoJSONExtractor_ChannelBufferSize(t *testing.T) {
	ctx := context.Background()

	extractor := NewGeoJSONExtractor[entities.RegionProperties]()
	factory := func() entities.RegionProperties {
		return entities.RegionProperties{}
	}

	// Extract with different batch sizes
	batchSizes := []int{1, 5, 10, 50}
	for _, batchSize := range batchSizes {
		t.Run(string(rune(batchSize+'0')), func(t *testing.T) {
			featureChan, err := extractor.Extract(ctx, "testdata/regions.geojson", batchSize, factory)
			if err != nil {
				t.Fatalf("Failed to extract with batch size %d: %v", batchSize, err)
			}

			// Just verify we can read from the channel
			featuresRead := 0
			for range featureChan {
				featuresRead++
			}

			if featuresRead != 1 {
				t.Errorf("Expected 1 feature with batch size %d, got %d", batchSize, featuresRead)
			}
		})
	}
}
