package entities

import "french_admin_etl/internal/model"

type RegionProperties struct {
	Code string `json:"code"`
	Nom  string `json:"nom"`
}

type GeoJsonRegionFeature = model.GeoJSONFeature[RegionProperties]

type RegionEntity struct {
	Code string `json:"code_insee_region"`
	Nom  string `json:"nom_region"`
}

type RegionWithGeometry = model.EntityWithGeoJSONGeometry[RegionEntity]

type regionMapper struct{}

func NewRegionMapper() *regionMapper {
	return &regionMapper{}
}

var _ model.Mapper[RegionProperties, RegionEntity] = (*regionMapper)(nil)

func (m *regionMapper) Map(input RegionProperties) (*RegionEntity, error) {
	return &RegionEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}