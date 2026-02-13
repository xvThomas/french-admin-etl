package transformers

import (
	"french_admin_etl/internal/model"
	"log/slog"
)

type geojsonTransformer[TInput any, TOutput any] struct {
	mapper model.Mapper[TInput, TOutput]
}

func NewGeoJSONTransformer[TInput any, TOutput any](mapper model.Mapper[TInput, TOutput]) model.GeoJSONTransformer[TInput, TOutput] {
	return &geojsonTransformer[TInput, TOutput]{mapper: mapper}
}

func (t *geojsonTransformer[TInput, TOutput]) Transform(features []model.GeoJSONFeature[TInput]) ([]model.EntityWithGeoJSONGeometry[TOutput], error) {
	entities := make([]model.EntityWithGeoJSONGeometry[TOutput], 0, len(features))
	for _, feature := range features {
		geomJSON, err := model.ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			slog.Warn("Skip feature with empty geometry", "feature", feature)
			continue
		}

		entity, err := t.mapper.Map(feature.Properties)
		if entity == nil {
			slog.Debug("Skip, mapper returned nil", "feature", feature)
			continue
		}
		if err != nil {
			return nil, err
		}

		entityWithGeom := model.EntityWithGeoJSONGeometry[TOutput]{
			Data:            *entity,
			GeoJSONGeometry: geomJSON,
		}
		entities = append(entities, entityWithGeom)
	}

	return entities, nil
}
