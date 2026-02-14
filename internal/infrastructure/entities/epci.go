package entities

import "french_admin_etl/internal/model"

// EPCIProperties represents the properties of an EPCI in the GeoJSON file.
type EPCIProperties struct {
	Code string `json:"code"`
	Nom  string `json:"nom"`
}

// GeoJSONEpciFeature is a type alias for a GeoJSON feature with EPCIProperties.
type GeoJSONEpciFeature = model.GeoJSONFeature[EPCIProperties]

// EPCIEntity represents the EPCI entity to be stored in the database.
type EPCIEntity struct {
	Code string `json:"code_insee_epci"`
	Nom  string `json:"nom_epci"`
}

// EPCIWithGeometry combines the EPCI entity with its GeoJSON geometry for database insertion.
type EPCIWithGeometry = model.EntityWithGeoJSONGeometry[EPCIEntity]

// EPCIMapper is responsible for mapping EPCIProperties to EPCIEntity.
type EPCIMapper struct{}

// NewEPCIMapper creates a new mapper for EPCI data.
func NewEPCIMapper() *EPCIMapper {
	return &EPCIMapper{}
}

var _ model.Mapper[EPCIProperties, EPCIEntity] = (*EPCIMapper)(nil)

// Map converts EPCIProperties to an EPCIEntity for database insertion.
func (m *EPCIMapper) Map(input EPCIProperties) (*EPCIEntity, error) {
	return &EPCIEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}
