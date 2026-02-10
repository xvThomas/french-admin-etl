package repository

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultMigrationsSchema = "etl_migrations"
const defaultMigrationsTableName = "schema_migrations"

// RunMigrations executes SQL migrations at startup
func RunMigrations(db *sql.DB, migrationsPath string) error {
	// Create schema for migrations if it doesn't exist
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", defaultMigrationsSchema))
	if err != nil {
		return fmt.Errorf("unable to create migration schema %s: %w", defaultMigrationsSchema, err)
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: defaultMigrationsTableName,
		SchemaName:      defaultMigrationsSchema,
	})
	if err != nil {
		return fmt.Errorf("unable to create migration driver: %w", err)
	}

	// Ensure we have an absolute path
	absPath, err := filepath.Abs(migrationsPath)
	if err != nil {
		return fmt.Errorf("unable to get absolute path for migrations: %w", err)
	}

	// Convert to forward slashes for cross-platform compatibility
	normalizedPath := filepath.ToSlash(absPath)

	// Create file URL
	// For Windows: file://C:/path/to/migrations
	// For Unix: file:///path/to/migrations
	sourceURL := "file://" + normalizedPath

	m, err := migrate.NewWithDatabaseInstance(
		sourceURL,
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("unable to create migration instance: %w", err)
	}

	// Apply all migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", err)
	}

	// Get current version
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return fmt.Errorf("unable to get migration version: %w", err)
	}

	if err == migrate.ErrNilVersion {
		slog.Debug("No migrations applied yet")
	} else {
		slog.Debug("Migrations applied successfully",
			"version", version,
			"dirty", dirty,
			"tracking_table", fmt.Sprintf("%s.%s", defaultMigrationsSchema, defaultMigrationsTableName),
		)
	}

	return nil
}
