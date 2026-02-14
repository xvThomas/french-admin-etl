package repository

import (
	"context"
	"fmt"
	"french_admin_etl/internal/infrastructure/entities"
	"french_admin_etl/internal/model"
	"log/slog"
)

type epciRepository struct {
	databaseManager *DatabaseManager
}

var _ model.EntityWithGeoJSONGeometryLoader[entities.EPCIEntity] = (*epciRepository)(nil)

// NewEPCIRepository creates a new instance of epciRepository with the provided DatabaseManager.
func NewEPCIRepository(dbManager *DatabaseManager) model.EntityWithGeoJSONGeometryLoader[entities.EPCIEntity] {
	return &epciRepository{
		databaseManager: dbManager,
	}
}

func (l *epciRepository) Load(ctx context.Context, entities []entities.EPCIWithGeometry) (int, error) {
	// batch transaction
	tx, err := l.databaseManager.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// see ../../../migrations/003_create_base_tables_reg_admin.sql for table structure and indexes

	// Prepare statement
	stmt := `
		INSERT INTO ref_admin.epci (code_insee_epci, nom_epci, geom)
		VALUES ($1, $2, ST_SetSRID(ST_GeomFromGeoJSON($3), 4326))
		ON CONFLICT (code_insee_epci) DO UPDATE SET
			nom_epci = EXCLUDED.nom_epci,
			code_insee_epci = EXCLUDED.code_insee_epci,
			geom = EXCLUDED.geom
	`

	count := 0
	failed := 0

	for i, entity := range entities {
		// Retrieve geometry
		if entity.GeoJSONGeometry == "" {
			slog.Warn("Missing geometry", "entity", "epci", "code", entity.Data.Code)
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
			entity.GeoJSONGeometry,
		)
		if err != nil {
			slog.Error("Insert error", "entity", "epci", "code", entity.Data.Code, "error", err)
			// Rollback to savepoint to continue with other inserts
			if _, rbErr := tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepoint)); rbErr != nil {
				slog.Error("Rollback to savepoint", "error", rbErr)
			}
			failed++
			continue
		}

		// Release savepoint on success
		if _, err := tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepoint)); err != nil {
			slog.Warn("Error releasing savepoint", "error", err)
		}

		count++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return count, nil
}
