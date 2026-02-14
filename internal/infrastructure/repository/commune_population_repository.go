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

// NewCommunePopulationRepository creates a new repository for loading commune population data.
func NewCommunePopulationRepository(dbManager *DatabaseManager) model.EntityLoader[entities.CommunePopulationPrincEntity] {
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
	pop  *int
	popH *int
	popF *int
	// Population by age groups
	popLT15   *int
	popLT15H  *int
	popLT15F  *int
	popLT20   *int
	popLT20H  *int
	popLT20F  *int
	pop15T24  *int
	pop15T24H *int
	pop15T24F *int
	pop20T64  *int
	pop20T64H *int
	pop20T64F *int
	pop25T39  *int
	pop25T39H *int
	pop25T39F *int
	pop40T54  *int
	pop40T54H *int
	pop40T54F *int
	pop55T64  *int
	pop55T64H *int
	pop55T64F *int
	pop65T79  *int
	pop65T79H *int
	pop65T79F *int
	popGE65   *int
	popGE65H  *int
	popGE65F  *int
	popGE80   *int
	popGE80H  *int
	popGE80F  *int
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

// fieldSelector returns a pointer to the appropriate field based on age and sex
type fieldSelector func(*populationRecord) **int

// populationFieldMap maps (age, sex) combinations to record fields
var populationFieldMap = map[string]map[string]fieldSelector{
	"Y_LT15": {
		"_T": func(r *populationRecord) **int { return &r.popLT15 },
		"M":  func(r *populationRecord) **int { return &r.popLT15H },
		"F":  func(r *populationRecord) **int { return &r.popLT15F },
	},
	"Y_LT20": {
		"_T": func(r *populationRecord) **int { return &r.popLT20 },
		"M":  func(r *populationRecord) **int { return &r.popLT20H },
		"F":  func(r *populationRecord) **int { return &r.popLT20F },
	},
	"Y15T24": {
		"_T": func(r *populationRecord) **int { return &r.pop15T24 },
		"M":  func(r *populationRecord) **int { return &r.pop15T24H },
		"F":  func(r *populationRecord) **int { return &r.pop15T24F },
	},
	"Y20T64": {
		"_T": func(r *populationRecord) **int { return &r.pop20T64 },
		"M":  func(r *populationRecord) **int { return &r.pop20T64H },
		"F":  func(r *populationRecord) **int { return &r.pop20T64F },
	},
	"Y25T39": {
		"_T": func(r *populationRecord) **int { return &r.pop25T39 },
		"M":  func(r *populationRecord) **int { return &r.pop25T39H },
		"F":  func(r *populationRecord) **int { return &r.pop25T39F },
	},
	"Y40T54": {
		"_T": func(r *populationRecord) **int { return &r.pop40T54 },
		"M":  func(r *populationRecord) **int { return &r.pop40T54H },
		"F":  func(r *populationRecord) **int { return &r.pop40T54F },
	},
	"Y55T64": {
		"_T": func(r *populationRecord) **int { return &r.pop55T64 },
		"M":  func(r *populationRecord) **int { return &r.pop55T64H },
		"F":  func(r *populationRecord) **int { return &r.pop55T64F },
	},
	"Y65T79": {
		"_T": func(r *populationRecord) **int { return &r.pop65T79 },
		"M":  func(r *populationRecord) **int { return &r.pop65T79H },
		"F":  func(r *populationRecord) **int { return &r.pop65T79F },
	},
	"Y_GE65": {
		"_T": func(r *populationRecord) **int { return &r.popGE65 },
		"M":  func(r *populationRecord) **int { return &r.popGE65H },
		"F":  func(r *populationRecord) **int { return &r.popGE65F },
	},
	"Y_GE80": {
		"_T": func(r *populationRecord) **int { return &r.popGE80 },
		"M":  func(r *populationRecord) **int { return &r.popGE80H },
		"F":  func(r *populationRecord) **int { return &r.popGE80F },
	},
	"_T": {
		"_T": func(r *populationRecord) **int { return &r.pop },
		"M":  func(r *populationRecord) **int { return &r.popH },
		"F":  func(r *populationRecord) **int { return &r.popF },
	},
}

// setPopulationValue assigns population to the correct field based on age and sex
func setPopulationValue(record *populationRecord, age, sex string, population int) {
	ageMap, ageExists := populationFieldMap[age]
	if !ageExists {
		panic(fmt.Sprintf("invalid age group %q for record: %v", age, *record))
	}

	selector, sexExists := ageMap[sex]
	if !sexExists {
		panic(fmt.Sprintf("invalid sex %q for age %q in record: %v", sex, age, *record))
	}

	field := selector(record)
	*field = &population
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
	defer func() { _ = tx.Rollback(ctx) }()

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
			record.pop, record.popH, record.popF,
			record.popLT15, record.popLT15H, record.popLT15F,
			record.popLT20, record.popLT20H, record.popLT20F,
			record.pop15T24, record.pop15T24H, record.pop15T24F,
			record.pop20T64, record.pop20T64H, record.pop20T64F,
			record.pop25T39, record.pop25T39H, record.pop25T39F,
			record.pop40T54, record.pop40T54H, record.pop40T54F,
			record.pop55T64, record.pop55T64H, record.pop55T64F,
			record.pop65T79, record.pop65T79H, record.pop65T79F,
			record.popGE65, record.popGE65H, record.popGE65F,
			record.popGE80, record.popGE80H, record.popGE80F,
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
