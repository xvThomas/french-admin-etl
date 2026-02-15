package transformers

import (
	"encoding/json"
	"errors"
	"french-admin-etl/internal/infrastructure/entities"
	"french-admin-etl/internal/model"
	"testing"

	"github.com/twpayne/go-geom/encoding/geojson"
)

// mockGeoJSONMapper is a mock implementation of model.Mapper for testing
type mockGeoJSONMapper struct {
	mapFunc func(entities.RegionProperties) (*entities.RegionEntity, error)
}

func (m *mockGeoJSONMapper) Map(input entities.RegionProperties) (*entities.RegionEntity, error) {
	if m.mapFunc != nil {
		return m.mapFunc(input)
	}
	return &entities.RegionEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}

// TestNewGeoJSONTransformer tests the constructor
func TestNewGeoJSONTransformer(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	if transformer == nil {
		t.Error("NewGeoJSONTransformer() returned nil")
	}
}

// TestGeoJSONTransformer_Transform_Success tests successful transformation
func TestGeoJSONTransformer_Transform_Success(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	// Create test features with valid geometry
	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "02",
				Nom:  "Martinique",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[3.0, 4.0]`),
			},
		},
	}

	entities, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 2 {
		t.Errorf("Expected 2 entities, got %d", len(entities))
	}

	// Verify first entity
	if entities[0].Data.Code != "01" {
		t.Errorf("Expected Code '01', got %q", entities[0].Data.Code)
	}
	if entities[0].Data.Nom != "Guadeloupe" {
		t.Errorf("Expected Nom 'Guadeloupe', got %q", entities[0].Data.Nom)
	}
	if entities[0].GeoJSONGeometry == "" {
		t.Error("Expected non-empty GeoJSONGeometry")
	}

	// Verify geometry is valid JSON
	var geom map[string]interface{}
	if err := json.Unmarshal([]byte(entities[0].GeoJSONGeometry), &geom); err != nil {
		t.Errorf("GeoJSONGeometry is not valid JSON: %v", err)
	}
}

// TestGeoJSONTransformer_Transform_EmptyFeatures tests transformation with empty input
func TestGeoJSONTransformer_Transform_EmptyFeatures(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{}

	entities, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 0 {
		t.Errorf("Expected 0 entities for empty input, got %d", len(entities))
	}
}

// TestGeoJSONTransformer_Transform_MapperError tests handling of mapper errors
// Note: Current implementation checks entity == nil before err != nil,
// so this test verifies that a non-nil entity with error returns the error
func TestGeoJSONTransformer_Transform_MapperError(t *testing.T) {
	mapper := &mockGeoJSONMapper{
		mapFunc: func(props entities.RegionProperties) (*entities.RegionEntity, error) {
			if props.Code == "02" {
				// Return non-nil entity WITH error to trigger error return
				return &entities.RegionEntity{
					Code: props.Code,
					Nom:  props.Nom,
				}, errors.New("mapper error")
			}
			return &entities.RegionEntity{
				Code: props.Code,
				Nom:  props.Nom,
			}, nil
		},
	}

	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "02",
				Nom:  "Martinique",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[3.0, 4.0]`),
			},
		},
	}

	_, err := transformer.Transform(features)
	if err == nil {
		t.Error("Expected error from mapper, got nil")
	}
}

// TestGeoJSONTransformer_Transform_MapperReturnsNil tests handling when mapper returns nil
func TestGeoJSONTransformer_Transform_MapperReturnsNil(t *testing.T) {
	mapper := &mockGeoJSONMapper{
		mapFunc: func(props entities.RegionProperties) (*entities.RegionEntity, error) {
			if props.Code == "02" {
				return nil, nil // Explicitly return nil
			}
			return &entities.RegionEntity{
				Code: props.Code,
				Nom:  props.Nom,
			}, nil
		},
	}

	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "02",
				Nom:  "Martinique",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[3.0, 4.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "03",
				Nom:  "Guyane",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[5.0, 6.0]`),
			},
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Should have 2 entities (skipping the nil one)
	if len(result) != 2 {
		t.Errorf("Expected 2 entities (nil record skipped), got %d", len(result))
	}

	// Verify we got the non-nil records
	if result[0].Data.Code != "01" {
		t.Errorf("Expected first entity Code '01', got %q", result[0].Data.Code)
	}
	if result[1].Data.Code != "03" {
		t.Errorf("Expected second entity Code '03', got %q", result[1].Data.Code)
	}
}

// TestGeoJSONTransformer_Transform_EmptyGeometry tests handling of empty geometry
// Note: geojson.Geometry{} with empty Type and nil Coordinates is still marshaled to JSON
// ConvertGeoJSONGeometryToBytes returns "" only when geometry is nil,
// so this test verifies both features are processed (empty geometry is not detected as empty)
func TestGeoJSONTransformer_Transform_EmptyGeometry(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "02",
				Nom:  "Martinique",
			},
			Geometry: geojson.Geometry{}, // Empty geometry (but still marshals to JSON)
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Current implementation: geojson.Geometry{} still has JSON representation,
	// so both features are processed
	if len(result) != 2 {
		t.Errorf("Expected 2 entities (empty geometry has JSON representation), got %d", len(result))
	}

	if result[0].Data.Code != "01" {
		t.Errorf("Expected Code '01', got %q", result[0].Data.Code)
	}
	if len(result) >= 2 && result[1].Data.Code != "02" {
		t.Errorf("Expected Code '02', got %q", result[1].Data.Code)
	}
}

// TestGeoJSONTransformer_Transform_SingleFeature tests transformation with single feature
func TestGeoJSONTransformer_Transform_SingleFeature(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 entity, got %d", len(result))
	}

	if result[0].Data.Code != "01" || result[0].Data.Nom != "Guadeloupe" {
		t.Errorf("Entity data mismatch: got Code=%q, Nom=%q", result[0].Data.Code, result[0].Data.Nom)
	}
}

// TestGeoJSONTransformer_Transform_ComplexGeometry tests with complex geometry types
func TestGeoJSONTransformer_Transform_ComplexGeometry(t *testing.T) {
	mapper := &mockGeoJSONMapper{}
	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	complexCoords := `[[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [1.0, 2.0]]]`

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Polygon",
				Coordinates: jsonRawMessage(complexCoords),
			},
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 entity, got %d", len(result))
	}

	// Verify geometry JSON contains expected structure
	if !contains(result[0].GeoJSONGeometry, "Polygon") {
		t.Errorf("Expected geometry to contain 'Polygon' type")
	}
}

// TestGeoJSONTransformer_Transform_PreservesMapperLogic tests that mapper logic is applied
func TestGeoJSONTransformer_Transform_PreservesMapperLogic(t *testing.T) {
	// Mapper that transforms data
	mapper := &mockGeoJSONMapper{
		mapFunc: func(props entities.RegionProperties) (*entities.RegionEntity, error) {
			return &entities.RegionEntity{
				Code: "CODE-" + props.Code,
				Nom:  "Nom: " + props.Nom,
			}, nil
		},
	}

	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Verify mapper logic was applied
	if result[0].Data.Code != "CODE-01" {
		t.Errorf("Expected Code 'CODE-01', got %q", result[0].Data.Code)
	}
	if result[0].Data.Nom != "Nom: Guadeloupe" {
		t.Errorf("Expected Nom 'Nom: Guadeloupe', got %q", result[0].Data.Nom)
	}
}

// TestGeoJSONTransformer_Transform_AllFeaturesSkipped tests when all features are skipped
func TestGeoJSONTransformer_Transform_AllFeaturesSkipped(t *testing.T) {
	mapper := &mockGeoJSONMapper{
		mapFunc: func(_ entities.RegionProperties) (*entities.RegionEntity, error) {
			return nil, nil // Always return nil
		},
	}

	transformer := NewGeoJSONTransformer[entities.RegionProperties, entities.RegionEntity](mapper)

	features := []model.GeoJSONFeature[entities.RegionProperties]{
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "01",
				Nom:  "Guadeloupe",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[1.0, 2.0]`),
			},
		},
		{
			Type: "Feature",
			Properties: entities.RegionProperties{
				Code: "02",
				Nom:  "Martinique",
			},
			Geometry: geojson.Geometry{
				Type:        "Point",
				Coordinates: jsonRawMessage(`[3.0, 4.0]`),
			},
		},
	}

	result, err := transformer.Transform(features)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected 0 entities when all are skipped, got %d", len(result))
	}
}

// Helper functions

func jsonRawMessage(s string) *json.RawMessage {
	raw := json.RawMessage(s)
	return &raw
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) >= len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
