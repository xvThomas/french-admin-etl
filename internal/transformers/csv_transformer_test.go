package transformers

import (
	"errors"
	"french-admin-etl/internal/model"
	"testing"
)

// mockCSVMapper is a mock implementation of model.Mapper for testing
type mockCSVMapper struct {
	mapFunc func(model.CSVRecord) (*testEntity, error)
}

type testEntity struct {
	ID   string
	Name string
}

func (m *mockCSVMapper) Map(input model.CSVRecord) (*testEntity, error) {
	if m.mapFunc != nil {
		return m.mapFunc(input)
	}
	return &testEntity{
		ID:   input["id"],
		Name: input["name"],
	}, nil
}

// TestNewCsvRecordTransformer tests the constructor
func TestNewCsvRecordTransformer(t *testing.T) {
	mapper := &mockCSVMapper{}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	if transformer == nil {
		t.Error("NewCsvRecordTransformer() returned nil")
	}
}

// TestCsvTransformer_Transform_Success tests successful transformation
func TestCsvTransformer_Transform_Success(t *testing.T) {
	mapper := &mockCSVMapper{
		mapFunc: func(record model.CSVRecord) (*testEntity, error) {
			return &testEntity{
				ID:   record["id"],
				Name: record["name"],
			}, nil
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},
		{"id": "3", "name": "Charlie"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 3 {
		t.Errorf("Expected 3 entities, got %d", len(entities))
	}

	// Verify first entity
	if entities[0].ID != "1" {
		t.Errorf("Expected ID '1', got %q", entities[0].ID)
	}
	if entities[0].Name != "Alice" {
		t.Errorf("Expected Name 'Alice', got %q", entities[0].Name)
	}

	// Verify last entity
	if entities[2].ID != "3" {
		t.Errorf("Expected ID '3', got %q", entities[2].ID)
	}
	if entities[2].Name != "Charlie" {
		t.Errorf("Expected Name 'Charlie', got %q", entities[2].Name)
	}
}

// TestCsvTransformer_Transform_EmptyRecords tests transformation with empty input
func TestCsvTransformer_Transform_EmptyRecords(t *testing.T) {
	mapper := &mockCSVMapper{}
	transformer := NewCsvRecordTransformer[testEntity](mapper)
	records := []model.CSVRecord{}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 0 {
		t.Errorf("Expected 0 entities for empty input, got %d", len(entities))
	}
}

// TestCsvTransformer_Transform_MapperError tests handling of mapper errors
func TestCsvTransformer_Transform_MapperError(t *testing.T) {
	callCount := 0
	mapper := &mockCSVMapper{
		mapFunc: func(record model.CSVRecord) (*testEntity, error) {
			callCount++
			if record["id"] == "2" {
				return nil, errors.New("mapper error")
			}
			return &testEntity{
				ID:   record["id"],
				Name: record["name"],
			}, nil
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"}, // This will cause an error
		{"id": "3", "name": "Charlie"},
	}

	// Transform should not return error, just skip the problematic record
	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() unexpected error = %v", err)
	}

	// Should have 2 entities (skipping the one with error)
	if len(entities) != 2 {
		t.Errorf("Expected 2 entities (error record skipped), got %d", len(entities))
	}

	// Verify we got the non-error records
	if entities[0].ID != "1" {
		t.Errorf("Expected first entity ID '1', got %q", entities[0].ID)
	}
	if entities[1].ID != "3" {
		t.Errorf("Expected second entity ID '3', got %q", entities[1].ID)
	}

	// Verify mapper was called for all records
	if callCount != 3 {
		t.Errorf("Expected mapper to be called 3 times, got %d", callCount)
	}
}

// TestCsvTransformer_Transform_MapperReturnsNil tests handling when mapper returns nil
func TestCsvTransformer_Transform_MapperReturnsNil(t *testing.T) {
	mapper := &mockCSVMapper{
		mapFunc: func(record model.CSVRecord) (*testEntity, error) {
			if record["id"] == "2" {
				return nil, nil // Explicitly return nil
			}
			return &testEntity{
				ID:   record["id"],
				Name: record["name"],
			}, nil
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"}, // This will return nil
		{"id": "3", "name": "Charlie"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Should have 2 entities (skipping the nil one)
	if len(entities) != 2 {
		t.Errorf("Expected 2 entities (nil record skipped), got %d", len(entities))
	}

	// Verify we got the non-nil records
	if entities[0].ID != "1" {
		t.Errorf("Expected first entity ID '1', got %q", entities[0].ID)
	}
	if entities[1].ID != "3" {
		t.Errorf("Expected second entity ID '3', got %q", entities[1].ID)
	}
}

// TestCsvTransformer_Transform_MixedErrorsAndNils tests mixed scenarios
func TestCsvTransformer_Transform_MixedErrorsAndNils(t *testing.T) {
	mapper := &mockCSVMapper{
		mapFunc: func(record model.CSVRecord) (*testEntity, error) {
			switch record["id"] {
			case "2":
				return nil, errors.New("error for id 2")
			case "4":
				return nil, nil // Explicitly nil
			default:
				return &testEntity{
					ID:   record["id"],
					Name: record["name"],
				}, nil
			}
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},     // Error
		{"id": "3", "name": "Charlie"},
		{"id": "4", "name": "David"},   // Nil
		{"id": "5", "name": "Eve"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Should have 3 entities (skipping error and nil)
	if len(entities) != 3 {
		t.Errorf("Expected 3 entities, got %d", len(entities))
	}

	// Verify we got the correct records
	expectedIDs := []string{"1", "3", "5"}
	for i, entity := range entities {
		if entity.ID != expectedIDs[i] {
			t.Errorf("Entity %d: expected ID %q, got %q", i, expectedIDs[i], entity.ID)
		}
	}
}

// TestCsvTransformer_Transform_AllRecordsSkipped tests when all records are skipped
func TestCsvTransformer_Transform_AllRecordsSkipped(t *testing.T) {
	mapper := &mockCSVMapper{
		mapFunc: func(_ model.CSVRecord) (*testEntity, error) {
			return nil, errors.New("always error")
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 0 {
		t.Errorf("Expected 0 entities when all are skipped, got %d", len(entities))
	}
}

// TestCsvTransformer_Transform_SingleRecord tests transformation with single record
func TestCsvTransformer_Transform_SingleRecord(t *testing.T) {
	mapper := &mockCSVMapper{}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 1 {
		t.Errorf("Expected 1 entity, got %d", len(entities))
	}

	if entities[0].ID != "1" || entities[0].Name != "Alice" {
		t.Errorf("Entity data mismatch: got ID=%q, Name=%q", entities[0].ID, entities[0].Name)
	}
}

// TestCsvTransformer_Transform_EmptyFieldValues tests records with empty field values
func TestCsvTransformer_Transform_EmptyFieldValues(t *testing.T) {
	mapper := &mockCSVMapper{}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "", "name": ""},
		{"id": "2", "name": ""},
		{"id": "", "name": "Bob"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	if len(entities) != 3 {
		t.Errorf("Expected 3 entities, got %d", len(entities))
	}

	// Verify empty values are preserved
	if entities[0].ID != "" || entities[0].Name != "" {
		t.Errorf("Expected empty values for first entity")
	}
	if entities[1].ID != "2" || entities[1].Name != "" {
		t.Errorf("Expected ID='2', Name='' for second entity")
	}
	if entities[2].ID != "" || entities[2].Name != "Bob" {
		t.Errorf("Expected ID='', Name='Bob' for third entity")
	}
}

// TestCsvTransformer_Transform_PreservesMapperLogic tests that mapper logic is preserved
func TestCsvTransformer_Transform_PreservesMapperLogic(t *testing.T) {
	// Mapper that transforms data
	mapper := &mockCSVMapper{
		mapFunc: func(record model.CSVRecord) (*testEntity, error) {
			return &testEntity{
				ID:   "ID-" + record["id"],
				Name: "Name: " + record["name"],
			}, nil
		},
	}
	transformer := NewCsvRecordTransformer[testEntity](mapper)

	records := []model.CSVRecord{
		{"id": "1", "name": "Alice"},
	}

	entities, err := transformer.Transform(records)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	// Verify mapper logic was applied
	if entities[0].ID != "ID-1" {
		t.Errorf("Expected ID 'ID-1', got %q", entities[0].ID)
	}
	if entities[0].Name != "Name: Alice" {
		t.Errorf("Expected Name 'Name: Alice', got %q", entities[0].Name)
	}
}
