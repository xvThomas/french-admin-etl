package repository

import (
	"context"
	"fmt"
	"french_admin_etl/internal/infrastructure/entities"
	"french_admin_etl/internal/model"
	"log/slog"
)

type communeRepository struct {
	databaseManager *DatabaseManager
}

var _ model.EntityWithGeoJSONGeometryLoader[entities.CommuneEntity] = (*communeRepository)(nil)

// NewCommuneRepository creates a new instance of communeRepository with the provided DatabaseManager.
func NewCommuneRepository(dbManager *DatabaseManager) model.EntityWithGeoJSONGeometryLoader[entities.CommuneEntity] {
	return &communeRepository{
		databaseManager: dbManager,
	}
}

func (l *communeRepository) Load(
	ctx context.Context,
	entities []entities.CommuneWithGeometry) (int, error) {
	// batch transaction
	tx, err := l.databaseManager.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// see ../../../migrations/003_create_base_tables_reg_admin.sql for table structure and indexes

	// Prepare statement
	// Generate an error if EPCI code doesn't exist
	/*
		stmt := `
			INSERT INTO ref_admin.communes(code_insee_commune, nom_commune, code_insee_epci, code_insee_departement, code_insee_region, geom)
			VALUES ($1, $2, $3, $4, $5, ST_SetSRID(ST_GeomFromGeoJSON($6), 4326))
			ON CONFLICT (code_insee_commune) DO UPDATE SET
				nom_commune = EXCLUDED.nom_commune,
				code_insee_epci = EXCLUDED.code_insee_epci,
				code_insee_departement = EXCLUDED.code_insee_departement,
				code_insee_region = EXCLUDED.code_insee_region,
				geom = EXCLUDED.geom
		`
	*/

	// Prepare statement
	// Use NULLIF to insert NULL if EPCI doesn't exist (avoids FK constraint violation)
	stmt := `
		INSERT INTO ref_admin.communes(code_insee_commune, nom_commune, code_insee_epci, code_insee_departement, code_insee_region, geom)
		VALUES ($1, $2,
			CASE WHEN EXISTS(SELECT 1 FROM ref_admin.epci WHERE code_insee_epci = $3) THEN $3 ELSE NULL END,
			$4, $5, ST_SetSRID(ST_GeomFromGeoJSON($6), 4326))
		ON CONFLICT (code_insee_commune) DO UPDATE SET
			nom_commune = EXCLUDED.nom_commune,
			code_insee_epci = EXCLUDED.code_insee_epci,
			code_insee_departement = EXCLUDED.code_insee_departement,
			code_insee_region = EXCLUDED.code_insee_region,
			geom = EXCLUDED.geom
	`

	count := 0
	failed := 0

	for i, entity := range entities {
		// Retrieve geometry
		if entity.GeoJSONGeometry == "" {
			slog.Warn("Missing geometry", "entity", "commune", "code", entity.Data.Code)
			failed++
			continue
		}

		// Create savepoint before each insert to allow rollback on error
		savepoint := fmt.Sprintf("sp_%d", i)
		if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", savepoint)); err != nil {
			slog.Error("Error creating savepoint", "error", err)
			failed++
			continue
		}

		// Insert into DB
		_, err = tx.Exec(ctx, stmt,
			entity.Data.Code,
			entity.Data.Nom,
			entity.Data.CodeEPCI,
			entity.Data.CodeDepartement,
			entity.Data.CodeRegion,
			entity.GeoJSONGeometry,
		)
		if err != nil {
			slog.Error("Insert error", "entity", "commune", "code", entity.Data.Code, "error", err)
			// Rollback to savepoint to continue with other inserts
			if _, rbErr := tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepoint)); rbErr != nil {
				slog.Error("Rollback to savepoint", "error", rbErr)
			}
			failed++
			continue
		}

		// Release savepoint on success
		if _, err := tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepoint)); err != nil {
			slog.Warn("Release savepoint", "error", err)
		}

		count++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return count, nil
}
