package model

// Mapper defines the interface for mapping one type to another.
type Mapper[TInput any, TOutput any] interface {
	Map(input TInput) (*TOutput, error)
}

// CsvRecordTransformer defines the interface for transforming CSV records into typed entities.
type CsvRecordTransformer[T any] interface {
	Transform(records []CSVRecord) ([]T, error)
}

// GeoJSONTransformer defines the interface for transforming GeoJSON features into entities with geometry.
type GeoJSONTransformer[TInput any, TOutput any] interface {
	Transform(features []GeoJSONFeature[TInput]) ([]EntityWithGeoJSONGeometry[TOutput], error)
}
