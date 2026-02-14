package entities

import "french-admin-etl/internal/model"

// DepartementProperties represents the properties of a department in the GeoJSON file
type DepartementProperties struct {
	Code   string `json:"code"`
	Nom    string `json:"nom"`
	Region string `json:"region"`
}

// GeoJSONDepartementFeature is a type alias for a GeoJSON feature with DepartementProperties.
type GeoJSONDepartementFeature = model.GeoJSONFeature[DepartementProperties]

// DepartementEntity represents the department entity to be stored in the database
type DepartementEntity struct {
	Code       string `json:"code_insee_departement"`
	Nom        string `json:"nom_departement"`
	CodeRegion string `json:"code_insee_region"`
}

// DepartementWithGeometry combines the department entity with its GeoJSON geometry for database insertion
type DepartementWithGeometry = model.EntityWithGeoJSONGeometry[DepartementEntity]

// DepartementMapper is responsible for mapping DepartementProperties to DepartementEntity.
type DepartementMapper struct{}

// NewDepartementMapper creates a new mapper for department data.
func NewDepartementMapper() *DepartementMapper {
	return &DepartementMapper{}
}

var _ model.Mapper[DepartementProperties, DepartementEntity] = (*DepartementMapper)(nil)

// Map converts DepartementProperties to a DepartementEntity for database insertion.
func (m *DepartementMapper) Map(input DepartementProperties) (*DepartementEntity, error) {
	return &DepartementEntity{
		Code:       input.Code,
		Nom:        input.Nom,
		CodeRegion: input.Region,
	}, nil
}
