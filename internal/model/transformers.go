package model

type Mapper[TInput any, TOutput any] interface {
	Map(input TInput) (TOutput, error)
}

type GeoJSONTransformer[TInput any, TOutput any] interface {
	Transform(features []GeoJSONFeature[TInput]) ([]EntityWithGeoJSONGeometry[TOutput], error)
}

type geojsonTransformer[TInput any, TOutput any] struct{
	mapper Mapper[TInput, TOutput]
}

func NewGeoJSONTransformer[TInput any, TOutput any](mapper Mapper[TInput, TOutput]) GeoJSONTransformer[TInput, TOutput] {
	return &geojsonTransformer[TInput, TOutput]{mapper: mapper}
}

func (t *geojsonTransformer[TInput, TOutput]) Transform(features []GeoJSONFeature[TInput]) ([]EntityWithGeoJSONGeometry[TOutput], error) {
	entities := make([]EntityWithGeoJSONGeometry[TOutput], 0, len(features))
	for _, feature := range features {
		geomJSON, err := ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			continue
		}

		entity, err := t.mapper.Map(feature.Properties)
		if err != nil {
			return nil, err
		}

		entityWithGeom := EntityWithGeoJSONGeometry[TOutput]{
			Data: entity,
			GeometryJSON: geomJSON,
		}
		entities = append(entities, entityWithGeom)
	}

	return entities, nil
}	

