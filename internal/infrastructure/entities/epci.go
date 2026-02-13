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

func (m *epciMapper) Map(input EPCIProperties) (*EPCIEntity, error) {
	return &EPCIEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}