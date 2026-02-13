package repository

import (
	"context"
	"fmt"
	"french_admin_etl/internal/infrastructure/entities"
	"french_admin_etl/internal/model"
	"log/slog"
	"sort"
)

type communePopulationRepository struct {
	databaseManager *DatabaseManager
}

var _ model.EntityLoader[entities.CommunePopulationPrincEntity] = (*communePopulationRepository)(nil)

func NewCommunePopulationRepository(dbManager *DatabaseManager) *communePopulationRepository {
	return &communePopulationRepository{
		databaseManager: dbManager,
	}
}

// populationRecord aggregates all population data for a single commune/year
type populationRecord struct {
	codeCommune string
	annee       int
	entityCount int // Number of original entities that contributed to this record
	// Total population
	pop   *int
	pop_h *int
	pop_f *int
	// Population by age groups
	pop_LT15    *int
	pop_LT15_h  *int
	pop_LT15_f  *int
	pop_LT20    *int
	pop_LT20_h  *int
	pop_LT20_f  *int
	pop_15T24   *int
	pop_15T24_h *int
	pop_15T24_f *int
	pop_20T64   *int
	pop_20T64_h *int
	pop_20T64_f *int
	pop_25T39   *int
	pop_25T39_h *int
	pop_25T39_f *int
	pop_40T54   *int
	pop_40T54_h *int
	pop_40T54_f *int
	pop_55T64   *int
	pop_55T64_h *int
	pop_55T64_f *int
	pop_65T79   *int
	pop_65T79_h *int
	pop_65T79_f *int
	pop_GE65    *int
	pop_GE65_h  *int
	pop_GE65_f  *int
	pop_GE80    *int
	pop_GE80_h  *int
	pop_GE80_f  *int
}

// aggregatePopulationData groups entities by commune/year and aggregates by age/sex
func aggregatePopulationData(entities []entities.CommunePopulationPrincEntity) map[string]*populationRecord {
	records := make(map[string]*populationRecord)

	for _, entity := range entities {
		key := fmt.Sprintf("%s_%d", entity.CodeCommune, entity.Annee)

		record, exists := records[key]
		if !exists {
			record = &populationRecord{
				codeCommune: entity.CodeCommune,
				annee:       entity.Annee,
				entityCount: 0,
			}
			records[key] = record
		}

		// Increment entity count for this record
		record.entityCount++

		// Map age and sex to appropriate field
		setPopulationValue(record, entity.Age, entity.Sexe, entity.Population)
	}

	return records
}

// setPopulationValue assigns population to the correct field based on age and sex
func setPopulationValue(record *populationRecord, age, sex string, population int) {
	// Helper to set value
	setValue := func(field **int) {
		*field = &population
	}

	switch age {
	case "Y_LT15":
		switch sex {
		case "_T":
			setValue(&record.pop_LT15)
		case "M":
			setValue(&record.pop_LT15_h)
		case "F":
			setValue(&record.pop_LT15_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y_LT20":
		switch sex {
		case "_T":
			setValue(&record.pop_LT20)
		case "M":
			setValue(&record.pop_LT20_h)
		case "F":
			setValue(&record.pop_LT20_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y15T24":
		switch sex {
		case "_T":
			setValue(&record.pop_15T24)
		case "M":
			setValue(&record.pop_15T24_h)
		case "F":
			setValue(&record.pop_15T24_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y20T64":
		switch sex {
		case "_T":
			setValue(&record.pop_20T64)
		case "M":
			setValue(&record.pop_20T64_h)
		case "F":
			setValue(&record.pop_20T64_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y25T39":
		switch sex {
		case "_T":
			setValue(&record.pop_25T39)
		case "M":
			setValue(&record.pop_25T39_h)
		case "F":
			setValue(&record.pop_25T39_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y40T54":
		switch sex {
		case "_T":
			setValue(&record.pop_40T54)
		case "M":
			setValue(&record.pop_40T54_h)
		case "F":
			setValue(&record.pop_40T54_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y55T64":
		switch sex {
		case "_T":
			setValue(&record.pop_55T64)
		case "M":
			setValue(&record.pop_55T64_h)
		case "F":
			setValue(&record.pop_55T64_f)
		default:
			panic(fmt.Sprintf("invalid sex: %v", *record))
		}
	case "Y65T79":
		switch sex {
		case "_T":
			setValue(&record.pop_65T79)
		case "M":
			setValue(&record.pop_65T79_h)
		case "F":
			setValue(&record.pop_65T79_f)

		}
	case "Y_GE65":
		switch sex {
		case "_T":
			setValue(&record.pop_GE65)
		case "M":
			setValue(&record.pop_GE65_h)
		case "F":
			setValue(&record.pop_GE65_f)
		}
	case "Y_GE80":
		switch sex {
		case "_T":
			setValue(&record.pop_GE80)
		case "M":
			setValue(&record.pop_GE80_h)
		case "F":
			setValue(&record.pop_GE80_f)
		}
	case "_T":
		// Total population (all ages)
		switch sex {
		case "_T":
			setValue(&record.pop)
		case "M":
			setValue(&record.pop_h)
		case "F":
			setValue(&record.pop_f)
		}
	default:
		panic(fmt.Sprintf("invalid age group: %v", *record))
	}
}

func (l *communePopulationRepository) Load(
	ctx context.Context,
	entities []entities.CommunePopulationPrincEntity) (int, error) {

	// Aggregate entities by commune/year
	records := aggregatePopulationData(entities)

	// Convert map to sorted slice to avoid deadlocks
	// Sort by (code_commune, annee) to ensure deterministic lock acquisition order
	sortedRecords := make([]*populationRecord, 0, len(records))
	for _, record := range records {
		sortedRecords = append(sortedRecords, record)
	}
	// Sort by code_commune first, then by annee
	sort.Slice(sortedRecords, func(i, j int) bool {
		if sortedRecords[i].codeCommune != sortedRecords[j].codeCommune {
			return sortedRecords[i].codeCommune < sortedRecords[j].codeCommune
		}
		return sortedRecords[i].annee < sortedRecords[j].annee
	})

	// Begin transaction
	tx, err := l.databaseManager.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Prepare statement with all columns
	stmt := `
		INSERT INTO demography.population_commune(
			code_insee_commune, annee, 
			pop, pop_h, pop_f,
			pop_LT15, pop_LT15_h, pop_LT15_f,
			pop_LT20, pop_LT20_h, pop_LT20_f,
			pop_15T24, pop_15T24_h, pop_15T24_f,
			pop_20T64, pop_20T64_h, pop_20T64_f,
			pop_25T39, pop_25T39_h, pop_25T39_f,
			pop_40T54, pop_40T54_h, pop_40T54_f,
			pop_55T64, pop_55T64_h, pop_55T64_f,
			pop_65T79, pop_65T79_h, pop_65T79_f,
			pop_GE65, pop_GE65_h, pop_GE65_f,
			pop_GE80, pop_GE80_h, pop_GE80_f
		)
		VALUES (
			$1, $2,
			$3, $4, $5,
			$6, $7, $8,
			$9, $10, $11,
			$12, $13, $14,
			$15, $16, $17,
			$18, $19, $20,
			$21, $22, $23,
			$24, $25, $26,
			$27, $28, $29,
			$30, $31, $32,
			$33, $34, $35
		)
		ON CONFLICT (code_insee_commune, annee) DO UPDATE SET
			pop = COALESCE(EXCLUDED.pop, population_commune.pop),
			pop_h = COALESCE(EXCLUDED.pop_h, population_commune.pop_h),
			pop_f = COALESCE(EXCLUDED.pop_f, population_commune.pop_f),
			pop_LT15 = COALESCE(EXCLUDED.pop_LT15, population_commune.pop_LT15),
			pop_LT15_h = COALESCE(EXCLUDED.pop_LT15_h, population_commune.pop_LT15_h),
			pop_LT15_f = COALESCE(EXCLUDED.pop_LT15_f, population_commune.pop_LT15_f),
			pop_LT20 = COALESCE(EXCLUDED.pop_LT20, population_commune.pop_LT20),
			pop_LT20_h = COALESCE(EXCLUDED.pop_LT20_h, population_commune.pop_LT20_h),
			pop_LT20_f = COALESCE(EXCLUDED.pop_LT20_f, population_commune.pop_LT20_f),
			pop_15T24 = COALESCE(EXCLUDED.pop_15T24, population_commune.pop_15T24),
			pop_15T24_h = COALESCE(EXCLUDED.pop_15T24_h, population_commune.pop_15T24_h),
			pop_15T24_f = COALESCE(EXCLUDED.pop_15T24_f, population_commune.pop_15T24_f),
			pop_20T64 = COALESCE(EXCLUDED.pop_20T64, population_commune.pop_20T64),
			pop_20T64_h = COALESCE(EXCLUDED.pop_20T64_h, population_commune.pop_20T64_h),
			pop_20T64_f = COALESCE(EXCLUDED.pop_20T64_f, population_commune.pop_20T64_f),
			pop_25T39 = COALESCE(EXCLUDED.pop_25T39, population_commune.pop_25T39),
			pop_25T39_h = COALESCE(EXCLUDED.pop_25T39_h, population_commune.pop_25T39_h),
			pop_25T39_f = COALESCE(EXCLUDED.pop_25T39_f, population_commune.pop_25T39_f),
			pop_40T54 = COALESCE(EXCLUDED.pop_40T54, population_commune.pop_40T54),
			pop_40T54_h = COALESCE(EXCLUDED.pop_40T54_h, population_commune.pop_40T54_h),
			pop_40T54_f = COALESCE(EXCLUDED.pop_40T54_f, population_commune.pop_40T54_f),
			pop_55T64 = COALESCE(EXCLUDED.pop_55T64, population_commune.pop_55T64),
			pop_55T64_h = COALESCE(EXCLUDED.pop_55T64_h, population_commune.pop_55T64_h),
			pop_55T64_f = COALESCE(EXCLUDED.pop_55T64_f, population_commune.pop_55T64_f),
			pop_65T79 = COALESCE(EXCLUDED.pop_65T79, population_commune.pop_65T79),
			pop_65T79_h = COALESCE(EXCLUDED.pop_65T79_h, population_commune.pop_65T79_h),
			pop_65T79_f = COALESCE(EXCLUDED.pop_65T79_f, population_commune.pop_65T79_f),
			pop_GE65 = COALESCE(EXCLUDED.pop_GE65, population_commune.pop_GE65),
			pop_GE65_h = COALESCE(EXCLUDED.pop_GE65_h, population_commune.pop_GE65_h),
			pop_GE65_f = COALESCE(EXCLUDED.pop_GE65_f, population_commune.pop_GE65_f),
			pop_GE80 = COALESCE(EXCLUDED.pop_GE80, population_commune.pop_GE80),
			pop_GE80_h = COALESCE(EXCLUDED.pop_GE80_h, population_commune.pop_GE80_h),
			pop_GE80_f = COALESCE(EXCLUDED.pop_GE80_f, population_commune.pop_GE80_f)
	`

	count := 0
	failed := 0
	failedEntityCount := 0

	// Insert records in sorted order with savepoints
	// Sorting prevents deadlocks when multiple workers access same keys
	for i, record := range sortedRecords {
		// Create savepoint before each insert to isolate errors
		savepoint := fmt.Sprintf("sp_%d", i)
		if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", savepoint)); err != nil {
			slog.Error("Error creating savepoint", "error", err)
			failed++
			failedEntityCount += record.entityCount
			continue
		}

		// Insert record
		_, err = tx.Exec(ctx, stmt,
			record.codeCommune, record.annee,
			record.pop, record.pop_h, record.pop_f,
			record.pop_LT15, record.pop_LT15_h, record.pop_LT15_f,
			record.pop_LT20, record.pop_LT20_h, record.pop_LT20_f,
			record.pop_15T24, record.pop_15T24_h, record.pop_15T24_f,
			record.pop_20T64, record.pop_20T64_h, record.pop_20T64_f,
			record.pop_25T39, record.pop_25T39_h, record.pop_25T39_f,
			record.pop_40T54, record.pop_40T54_h, record.pop_40T54_f,
			record.pop_55T64, record.pop_55T64_h, record.pop_55T64_f,
			record.pop_65T79, record.pop_65T79_h, record.pop_65T79_f,
			record.pop_GE65, record.pop_GE65_h, record.pop_GE65_f,
			record.pop_GE80, record.pop_GE80_h, record.pop_GE80_f,
		)
		if err != nil {
			slog.Error("Insert error", "entity", "population", "commune", record.codeCommune, "year", record.annee, "error", err)
			// Rollback to savepoint to continue with other inserts
			if _, rbErr := tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepoint)); rbErr != nil {
				slog.Error("Rollback to savepoint failed", "error", rbErr)
			}
			failed++
			failedEntityCount += record.entityCount
			continue
		}

		// Release savepoint on success
		if _, err := tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepoint)); err != nil {
			slog.Warn("Release savepoint warning", "error", err)
		}

		// Count all entities that contributed to this successfully inserted record
		count += record.entityCount
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	slog.Debug("Population data loaded",
		"input_entities", len(entities),
		"aggregated_records", len(records),
		"records_inserted", len(records)-failed,
		"records_failed", failed,
		"entities_loaded", count,
		"entities_failed", failedEntityCount)

	return count, nil
}
