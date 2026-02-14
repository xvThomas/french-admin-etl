// Package config handles loading and managing configuration from environment variables.
package config

import (
	"fmt"

	"github.com/caarlos0/env/v10"
	"github.com/joho/godotenv"
)

// Config holds the application configuration.
type Config struct {
	PostgresDatabase PostgresDatabase
	Workers          int `env:"ETL_WORKERS" envDefault:"4"`
	BatchSize        int `env:"ETL_BATCH_SIZE" envDefault:"1000"`
}

// PostgresDatabase holds PostgreSQL database configuration.
type PostgresDatabase struct {
	Host            string `env:"POSTGRES_HOST" envDefault:"localhost"`
	Port            int    `env:"POSTGRES_PORT" envDefault:"5432"`
	User            string `env:"POSTGRES_USER" envDefault:"admin"`
	Password        string `env:"POSTGRES_PASSWORD" envDefault:"password"`
	Database        string `env:"POSTGRES_DATABASE" envDefault:"mapbot"`
	SSLMode         string `env:"POSTGRES_SSLMODE" envDefault:"disable"`
	MaxOpenConns    int    `env:"POSTGRES_MAX_OPEN_CONNS" envDefault:"25"`
	MaxIdleConns    int    `env:"POSTGRES_MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime int    `env:"POSTGRES_CONN_MAX_LIFETIME_M" envDefault:"5"`
	ConnMaxIdleTime int    `env:"POSTGRES_CONN_MAX_IDLE_TIME_S" envDefault:"30"`
	PingTimeout     int    `env:"POSTGRES_PING_TIMEOUT_S" envDefault:"5"`
}

// NewPostgresDatabase creates a new PostgresDatabase configuration with default values.
func NewPostgresDatabase(host string, port int, database, user, password string) *PostgresDatabase {
	return &PostgresDatabase{
		Host:            host,
		Port:            port,
		Database:        database,
		User:            user,
		Password:        password,
		SSLMode:         "disable",
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: 5,
		ConnMaxIdleTime: 30,
		PingTimeout:     5,
	}
}

// ConnectionString returns the PostgreSQL connection string.
func (dc *PostgresDatabase) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		dc.User, dc.Password, dc.Host, dc.Port, dc.Database, dc.SSLMode)
}

// Load reads configuration from environment variables and .env file.
func Load() (*Config, error) {
	// Load .env (silently ignored if it doesn't exist)
	_ = godotenv.Load()

	config := &Config{}

	// Automatically parse environment variables with priority:
	// 1. System environment variables
	// 2. .env variables
	// 3. Default values (envDefault)
	if err := env.Parse(config); err != nil {
		return nil, err
	}

	return config, nil
}
