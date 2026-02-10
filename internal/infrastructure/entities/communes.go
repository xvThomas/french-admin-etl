package entities

import "french_admin_etl/internal/model"

type CommuneProperties struct {
	Code        string `json:"code"`
	Nom         string `json:"nom"`
	EPCI        string `json:"epci"`
	Departement string `json:"departement"`
	Region      string `json:"region"`
}

type GeoJsonCommuneFeature = model.GeoJSONFeature[CommuneProperties]

type CommuneEntity struct {
	Code            string `json:"code_insee_commune"`
	Nom             string `json:"nom_commune"`
	CodeEPCI        string `json:"code_insee_epci"`
	CodeDepartement string `json:"code_insee_departement"`
	CodeRegion      string `json:"code_insee_region"`
}

type CommuneWithGeometry = model.EntityWithGeoJSONGeometry[CommuneEntity]

/*
type communeExtractor = model.GeoJSONExtractor[CommuneProperties]

func NewCommuneExtractor() communeExtractor {
	return *model.NewGeoJSONExtractor[CommuneProperties]()
}
*/

type communeMapper struct{}

func NewCommuneMapper() *communeMapper {
	return &communeMapper{}
}

var _ model.Mapper[CommuneProperties, CommuneEntity] = (*communeMapper)(nil)

func (m *communeMapper) Map(input CommuneProperties) (CommuneEntity, error) {
	return CommuneEntity{
		Code:            input.Code,
		Nom:             input.Nom,
		CodeEPCI:        input.EPCI,
		CodeDepartement: input.Departement,
		CodeRegion:      input.Region,
	}, nil
}

/*
type communesTransformer struct{}

func NewCommunesTransformer() *communesTransformer {
	return &communesTransformer{}
}

var _ model.GeoJSONTransformer[CommunesProperties, CommunesEntity] = (*communesTransformer)(nil)

func (t *communesTransformer) Transform(features []GeoJsonCommunesFeature) ([]CommunesWithGeometry, error) {
	var entities []CommunesWithGeometry
	for _, feature := range features {
		geomJSON, err := model.ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			continue
		}

		entity := CommunesWithGeometry{
			Data: CommunesEntity{
				Code:            feature.Properties.Code,
				Nom:             feature.Properties.Nom,
				CodeEPCI:        feature.Properties.EPCI,
				CodeDepartement: feature.Properties.Departement,
				CodeRegion:      feature.Properties.Region,
			},
			GeometryJSON: geomJSON,
		}
		entities = append(entities, entity)
	}

	return entities, nil
}
	*/
