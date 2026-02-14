package transformers

import (
	"french_admin_etl/internal/model"
	"log/slog"
)

type csvTransformer[T any] struct {
	mapper model.Mapper[model.CSVRecord, T]
}

// NewCsvRecordTransformer creates a new CsvRecordTransformer with the provided mapper.
func NewCsvRecordTransformer[T any](mapper model.Mapper[model.CSVRecord, T]) model.CsvRecordTransformer[T] {
	return &csvTransformer[T]{mapper: mapper}
}

func (t *csvTransformer[T]) Transform(records []model.CSVRecord) ([]T, error) {
	entities := make([]T, 0, len(records))
	for _, record := range records {
		entity, err := t.mapper.Map(record)
		if err != nil {
			slog.Error("Error mapping record", "error", err, "record", record)
			continue
		}
		if entity == nil {
			slog.Debug("Skip, mapper returned nil", "record", record)
			continue
		}

		entities = append(entities, *entity)
	}
	return entities, nil
}
