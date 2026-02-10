package entities

import "french_admin_etl/internal/model"

type DepartementProperties struct {
	Code   string `json:"code"`
	Nom    string `json:"nom"`
	Region string `json:"region"`
}

type GeoJsonDepartementFeature = model.GeoJSONFeature[DepartementProperties]

type DepartementEntity struct {
	Code       string `json:"code_insee_departement"`
	Nom        string `json:"nom_departement"`
	CodeRegion string `json:"code_insee_region"`
}

type DepartementWithGeometry = model.EntityWithGeoJSONGeometry[DepartementEntity]

type departementMapper struct{}

func NewDepartementMapper() *departementMapper {
	return &departementMapper{}
}

var _ model.Mapper[DepartementProperties, DepartementEntity] = (*departementMapper)(nil)

func (m *departementMapper) Map(input DepartementProperties) (DepartementEntity, error) {
	return DepartementEntity{
		Code:       input.Code,
		Nom:        input.Nom,
		CodeRegion: input.Region,
	}, nil
}

/*
type departementExtractor = model.GeoJSONExtractor[DepartementProperties]

func NewDepartementExtractor() departementExtractor {
	return *model.NewGeoJSONExtractor[DepartementProperties]()
}

type departementTransformer struct{}

func NewDepartementTransformer() *departementTransformer {
	return &departementTransformer{}
}

var _ model.GeoJSONTransformer[DepartementProperties, DepartementEntity] = (*departementTransformer)(nil)

func (t *departementTransformer) Transform(features []GeoJsonDepartementFeature) ([]DepartementWithGeometry, error) {
	var entities []DepartementWithGeometry
	for _, feature := range features {
		geomJSON, err := model.ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			continue
		}

		entity := DepartementWithGeometry{
			Data: DepartementEntity{
				Code:       feature.Properties.Code,
				Nom:        feature.Properties.Nom,
				CodeRegion: feature.Properties.Region,
			},
			GeometryJSON: geomJSON,
		}
		entities = append(entities, entity)
	}

	return entities, nil
}
*/