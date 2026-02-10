package entities

import "french_admin_etl/internal/model"

type EPCIProperties struct {
	Code string `json:"code"`
	Nom  string `json:"nom"`
}

type GeoJsonEpciFeature = model.GeoJSONFeature[EPCIProperties]

type EPCIEntity struct {
	Code string `json:"code_insee_epci"`
	Nom  string `json:"nom_epci"`
}

type EPCIWithGeometry = model.EntityWithGeoJSONGeometry[EPCIEntity]

type epciMapper struct{}

func NewEPCIMapper() *epciMapper {
	return &epciMapper{}
}

var _ model.Mapper[EPCIProperties, EPCIEntity] = (*epciMapper)(nil)

func (m *epciMapper) Map(input EPCIProperties) (EPCIEntity, error) {
	return EPCIEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}

/*

type epciExtractor = model.GeoJSONExtractor[EPCIProperties]

func NewEpciExtractor() epciExtractor {
	return *model.NewGeoJSONExtractor[EPCIProperties]()
}

type epciTransformer struct{}

func NewEpciTransformer() *epciTransformer {
	return &epciTransformer{}
}

var _ model.GeoJSONTransformer[EPCIProperties, EPCIEntity] = (*epciTransformer)(nil)

func (t *epciTransformer) Transform(features []GeoJsonEpciFeature) ([]EPCIWithGeometry, error) {
	var entities []EPCIWithGeometry
	for _, feature := range features {
		geomJSON, err := model.ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			continue
		}

		entity := EPCIWithGeometry{
			Data: EPCIEntity{
				Code: feature.Properties.Code,
				Nom:  feature.Properties.Nom,
			},
			GeometryJSON: geomJSON,
		}
		entities = append(entities, entity)
	}

	return entities, nil
}
*/
