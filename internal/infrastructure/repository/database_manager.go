package repository

import (
	"context"
	"database/sql"
	"fmt"
	"french-admin-etl/internal/infrastructure/config"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	// Import the pgx driver for database/sql
	_ "github.com/jackc/pgx/v5/stdlib"
)

// DatabaseManager manage connections to the database and provides utility methods for health checks and stats
type DatabaseManager struct {
	db     *sql.DB
	config *config.PostgresDatabase
	pool   *pgxpool.Pool
}

// DatabaseManagerOption is a configuration function
type DatabaseManagerOption func(*DatabaseManager) error

// WithMigrations is an option to automatically run migrations
func WithMigrations(migrationsPath string) DatabaseManagerOption {
	return func(dm *DatabaseManager) error {
		return RunMigrations(dm.db, migrationsPath)
	}
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager(config *config.Config, opts ...DatabaseManagerOption) (*DatabaseManager, error) {
	if config == nil {
		return nil, fmt.Errorf("database config cannot be nil")
	}

	// Validation of required fields
	if config.PostgresDatabase.Host == "" {
		return nil, fmt.Errorf("host cannot be empty")
	}
	if config.PostgresDatabase.Database == "" {
		return nil, fmt.Errorf("database name cannot be empty")
	}
	if config.PostgresDatabase.User == "" {
		return nil, fmt.Errorf("user cannot be empty")
	}

	connectionString := config.PostgresDatabase.ConnectionString()
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// Configure the connection pool
	db.SetMaxOpenConns(config.PostgresDatabase.MaxOpenConns)
	db.SetMaxIdleConns(config.PostgresDatabase.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(config.PostgresDatabase.ConnMaxLifetime) * time.Minute)
	db.SetConnMaxIdleTime(time.Duration(config.PostgresDatabase.ConnMaxIdleTime) * time.Second)

	// Test the connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.PostgresDatabase.PingTimeout)*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to connect to database %s within %v: %w",
			config.PostgresDatabase.ConnectionString(), config.PostgresDatabase.PingTimeout, err)
	}

	poolConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	// Optimized configuration for ETL
	// Validate workers count is within reasonable range for int32
	if config.Workers < 1 || config.Workers > 10000 {
		return nil, fmt.Errorf("workers count must be between 1 and 10000, got %d", config.Workers)
	}
	poolConfig.MaxConns = int32(config.Workers)     // #nosec G115 -- validated range ensures no overflow
	poolConfig.MinConns = int32(config.Workers / 2) // #nosec G115 -- validated range ensures no overflow

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to DB: %w", err)
	}

	dm := &DatabaseManager{
		db:     db,
		config: &config.PostgresDatabase,
		pool:   pool,
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(dm); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to apply database option: %w", err)
		}
	}

	return dm, nil
}

// GetDB retourne l'instance *sql.DB
func (dm *DatabaseManager) GetDB() *sql.DB {
	return dm.db
}

// GetConfig retourne la configuration de la base de données
func (dm *DatabaseManager) GetConfig() *config.PostgresDatabase {
	return dm.config
}

// Ping teste la connexion à la base de données
func (dm *DatabaseManager) Ping(ctx context.Context) error {
	return dm.db.PingContext(ctx)
}

// Stats returns the connection pool statistics
func (dm *DatabaseManager) Stats() sql.DBStats {
	return dm.db.Stats()
}

// Close closes all connections
func (dm *DatabaseManager) Close() error {
	if dm.db != nil {
		return dm.db.Close()
	}
	return nil
}

// Health checks the health status of the database
func (dm *DatabaseManager) Health(ctx context.Context) error {
	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	stats := dm.Stats()
	if stats.OpenConnections >= dm.config.MaxOpenConns {
		return fmt.Errorf("database connection pool exhausted: %d/%d connections",
			stats.OpenConnections, dm.config.MaxOpenConns)
	}

	return nil
}

// PublicConnectionString returns the configuration information as a string without the password
func (dm *DatabaseManager) PublicConnectionString() string {
	password := "****"
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		dm.config.User, password, dm.config.Host, dm.config.Port, dm.config.Database, dm.config.SSLMode)
}
