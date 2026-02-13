package model

type Mapper[TInput any, TOutput any] interface {
	Map(input TInput) (*TOutput, error)
}

type CsvRecordTransformer[T any] interface {
	Transform(records []CSVRecord) ([]T, error)
}

type GeoJSONTransformer[TInput any, TOutput any] interface {
	Transform(features []GeoJSONFeature[TInput]) ([]EntityWithGeoJSONGeometry[TOutput], error)
}



