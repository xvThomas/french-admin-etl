package entities

import "french_admin_etl/internal/model"

// CommuneProperties represents the properties of a commune in the GeoJSON file.
type CommuneProperties struct {
	Code        string `json:"code"`
	Nom         string `json:"nom"`
	EPCI        string `json:"epci"`
	Departement string `json:"departement"`
	Region      string `json:"region"`
}

// GeoJSONCommuneFeature is a type alias for a GeoJSON feature with CommuneProperties.
type GeoJSONCommuneFeature = model.GeoJSONFeature[CommuneProperties]

// CommuneEntity represents the commune entity to be stored in the database.
type CommuneEntity struct {
	Code            string `json:"code_insee_commune"`
	Nom             string `json:"nom_commune"`
	CodeEPCI        string `json:"code_insee_epci"`
	CodeDepartement string `json:"code_insee_departement"`
	CodeRegion      string `json:"code_insee_region"`
}

// CommuneWithGeometry combines the commune entity with its GeoJSON geometry for database insertion.
type CommuneWithGeometry = model.EntityWithGeoJSONGeometry[CommuneEntity]

// CommuneMapper is responsible for mapping CommuneProperties to CommuneEntity.
type CommuneMapper struct{}

// NewCommuneMapper creates a new mapper for commune data.
func NewCommuneMapper() *CommuneMapper {
	return &CommuneMapper{}
}

var _ model.Mapper[CommuneProperties, CommuneEntity] = (*CommuneMapper)(nil)

// Map converts CommuneProperties to a CommuneEntity for database insertion.
func (m *CommuneMapper) Map(input CommuneProperties) (*CommuneEntity, error) {
	return &CommuneEntity{
		Code:            input.Code,
		Nom:             input.Nom,
		CodeEPCI:        input.EPCI,
		CodeDepartement: input.Departement,
		CodeRegion:      input.Region,
	}, nil
}
