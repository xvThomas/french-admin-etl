package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/joho/godotenv"

	"french_admin_etl/internal/infrastructure/config"
	"french_admin_etl/internal/infrastructure/entities"
	_ "french_admin_etl/internal/infrastructure/logger"
	"french_admin_etl/internal/infrastructure/repository"
	"french_admin_etl/internal/processor"
)

func main() {

	// Charger les variables d'environnement
	if err := godotenv.Load(); err != nil {
		slog.Warn(".env file not found", "warning", err)
	}

	config, err := config.Load()
	if err != nil {
		slog.Error("❌ Failed to load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	const migrationsPath = "./migrations"
	databaseManager, err := repository.NewDatabaseManager(
		config,
		repository.WithMigrations(migrationsPath),
	)

	if err != nil {
		slog.Error("❌ Failed to create database manager or migrate database", "error", err)
		os.Exit(1)
	}

	regionProcess := processor.NewGeoJSONETLProcessor(
		config,
		"Régions",
		func() entities.RegionProperties {
			return entities.RegionProperties{}
		},
		entities.NewRegionMapper(),
		repository.NewRegionRepository(databaseManager),
	)

	err = regionProcess.Run(ctx, "./data/regions-1000m.geojson")
	if err != nil {
		slog.Error("❌ Failed to run region process", "error", err)
		os.Exit(1)
	}

	departementProcess := processor.NewGeoJSONETLProcessor(
		config,
		"Departements",
		func() entities.DepartementProperties {
			return entities.DepartementProperties{}
		},
		entities.NewDepartementMapper(),
		repository.NewDepartementRepository(databaseManager),
	)

	err = departementProcess.Run(ctx, "./data/departements-1000m.geojson")
	if err != nil {
		slog.Error("❌ Failed to run departement process", "error", err)
		os.Exit(1)
	}

	epciProcess := processor.NewGeoJSONETLProcessor(
		config,
		"EPCI",
		func() entities.EPCIProperties {
			return entities.EPCIProperties{}
		},
		entities.NewEPCIMapper(),
		repository.NewEPCIRepository(databaseManager),
	)

	err = epciProcess.Run(ctx, "./data/epci-1000m.geojson")
	if err != nil {
		slog.Error("❌ Failed to run epci process", "error", err)
		os.Exit(1)
	}

	communesProcess := processor.NewGeoJSONETLProcessor(
		config,
		"Communes",
		func() entities.CommuneProperties {
			return entities.CommuneProperties{}
		},
		entities.NewCommuneMapper(),
		repository.NewCommuneRepository(databaseManager),
	)

	err = communesProcess.Run(ctx, "./data/communes-1000m.geojson")
	if err != nil {
		slog.Error("❌ Failed to run communes process", "error", err)
		os.Exit(1)
	}

	slog.Info("ETL completed")
}
