// Package entities defines domain entities for French administrative data.
package entities

import (
	"fmt"
	filters "french_admin_etl/internal/Filters"
	"french_admin_etl/internal/model"
	"math"
	"strconv"
	"strings"
)

// CommunePopulationPrincEntity represents commune population data by age and gender.
type CommunePopulationPrincEntity struct {
	Age         string // _T, Y15T24, Y20T64, Y25T39, Y40T54, Y55T64, Y65T79, Y_GE65, Y_GE80, Y_LT15, Y_LT20
	CodeCommune string // 5-character code for communes
	Sexe        string // _T for total, M for men, F for women
	Annee       int
	Population  int
}

// CommunePopulationPrincFilter is a predefined filter that keeps only commune and arrondissement records.
var CommunePopulationPrincFilter = filters.NewCsvRecordFilterFromAllowList(map[string][]string{
	"GEO_OBJECT": {"COM", "ARM"},
})

// CommunePopulationMapper maps CSV records to CommunePopulationPrincEntity.
type CommunePopulationMapper struct{}

// NewCommunePopulationMapper creates a new mapper for commune population data.
func NewCommunePopulationMapper() *CommunePopulationMapper {
	return &CommunePopulationMapper{}
}

var _ model.Mapper[model.CSVRecord, CommunePopulationPrincEntity] = (*CommunePopulationMapper)(nil)

// Map converts a CSV record to a CommunePopulationPrincEntity, with validation and error handling.
func (m *CommunePopulationMapper) Map(record model.CSVRecord) (*CommunePopulationPrincEntity, error) {
	age := record["AGE"]
	if age != "_T" && age != "Y15T24" && age != "Y20T64" && age != "Y25T39" && age != "Y40T54" && age != "Y55T64" && age != "Y65T79" && age != "Y_GE65" && age != "Y_GE80" && age != "Y_LT15" && age != "Y_LT20" {
		return nil, fmt.Errorf("invalid AGE, must be one of _T, Y15T24, Y20T64, Y25T39, Y40T54, Y55T64, Y65T79, Y_GE65, Y_GE80, Y_LT15, Y_LT20")
	}

	codeCommune := record["GEO"]
	if len(codeCommune) != 5 {
		return nil, fmt.Errorf("invalid GEO code, must be 5 characters for communes")
	}

	// geoObject := record["GEO_OBJECT"] // COM
	// if geoObject != "COM" && geoObject != "ARM" {
	//     slog.Warn("Skip record, ignored GEO_OBJECT", "record", record)
	// 	return nil, nil // Only keep code commune and arrondissment for population_princ
	// }

	sexe := record["SEX"]
	// Validate sexe: must be _T (total), M (hommes), or F (femmes)
	if sexe != "_T" && sexe != "M" && sexe != "F" {
		return nil, fmt.Errorf("invalid SEX, must be _T, M, or F") // Skip invalid sexe
	}

	annee, err := strconv.Atoi(record["TIME_PERIOD"])
	if err != nil {
		return nil, fmt.Errorf("invalid TIME_PERIOD: %s, %w", record["TIME_PERIOD"], err)
	}

	// Parse population: handle both decimal separators ("." and ",")
	populationStr := strings.ReplaceAll(record["OBS_VALUE"], ",", ".")
	populationFloat, err := strconv.ParseFloat(populationStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid OBS_VALUE: %s, %w", record["OBS_VALUE"], err)
	}
	// Round to nearest integer
	population := int(math.Round(populationFloat))

	return &CommunePopulationPrincEntity{
		Age:         age,
		CodeCommune: codeCommune,
		Sexe:        sexe,
		Annee:       annee,
		Population:  population,
	}, nil
}
