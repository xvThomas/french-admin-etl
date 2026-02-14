package entities

import "french_admin_etl/internal/model"

// RegionProperties represents the properties of a region in the GeoJSON file.
type RegionProperties struct {
	Code string `json:"code"`
	Nom  string `json:"nom"`
}

// GeoJSONRegionFeature is a type alias for a GeoJSON feature with RegionProperties.
type GeoJSONRegionFeature = model.GeoJSONFeature[RegionProperties]

// RegionEntity represents the region entity to be stored in the database.
type RegionEntity struct {
	Code string `json:"code_insee_region"`
	Nom  string `json:"nom_region"`
}

// RegionWithGeometry combines the region entity with its GeoJSON geometry for database insertion.
type RegionWithGeometry = model.EntityWithGeoJSONGeometry[RegionEntity]

// RegionMapper is responsible for mapping RegionProperties to RegionEntity.
type RegionMapper struct{}

// NewRegionMapper creates a new mapper for region data.
func NewRegionMapper() *RegionMapper {
	return &RegionMapper{}
}

var _ model.Mapper[RegionProperties, RegionEntity] = (*RegionMapper)(nil)

// Map converts RegionProperties to a RegionEntity for database insertion.
func (m *RegionMapper) Map(input RegionProperties) (*RegionEntity, error) {
	return &RegionEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}
