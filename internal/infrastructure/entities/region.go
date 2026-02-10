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

func (m *regionMapper) Map(input RegionProperties) (RegionEntity, error) {
	return RegionEntity{
		Code: input.Code,
		Nom:  input.Nom,
	}, nil
}

/*	

type regionExtractor = model.GeoJSONExtractor[RegionProperties]

func NewRegionExtractor() regionExtractor {
	return *model.NewGeoJSONExtractor[RegionProperties]()
}

type regionTransformer struct{}

func NewRegionTransformer() *regionTransformer {
	return &regionTransformer{}
}

var _ model.GeoJSONTransformer[RegionProperties, RegionEntity] = (*regionTransformer)(nil)

func (t *regionTransformer) Transform(features []GeoJsonRegionFeature) ([]RegionWithGeometry, error) {
	var entities []RegionWithGeometry
	for _, feature := range features {
		geomJSON, err := model.ConvertGeoJSONGeometryToBytes(&feature.Geometry)
		if err != nil {
			return nil, err
		}
		if geomJSON == "" {
			continue
		}

		entity := RegionWithGeometry{
			Data: RegionEntity{
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