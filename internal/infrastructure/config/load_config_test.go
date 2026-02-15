package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoad_DefaultValues tests loading config with default values (no environment variables set)
func TestLoad_DefaultValues(t *testing.T) {
	cleanEnv(t)

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify default values
	if config.Workers != 4 {
		t.Errorf("Workers = %d, want 4", config.Workers)
	}
	if config.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", config.BatchSize)
	}

	// Verify PostgresDatabase defaults
	db := config.PostgresDatabase
	if db.Host != "localhost" {
		t.Errorf("Host = %s, want localhost", db.Host)
	}
	if db.Port != 5432 {
		t.Errorf("Port = %d, want 5432", db.Port)
	}
	if db.User != "admin" {
		t.Errorf("User = %s, want admin", db.User)
	}
	if db.Password != "password" {
		t.Errorf("Password = %s, want password", db.Password)
	}
	if db.Database != "mapbot" {
		t.Errorf("Database = %s, want mapbot", db.Database)
	}
	if db.SSLMode != "disable" {
		t.Errorf("SSLMode = %s, want disable", db.SSLMode)
	}
	if db.MaxOpenConns != 25 {
		t.Errorf("MaxOpenConns = %d, want 25", db.MaxOpenConns)
	}
	if db.MaxIdleConns != 10 {
		t.Errorf("MaxIdleConns = %d, want 10", db.MaxIdleConns)
	}
	if db.ConnMaxLifetime != 5 {
		t.Errorf("ConnMaxLifetime = %d, want 5", db.ConnMaxLifetime)
	}
	if db.ConnMaxIdleTime != 30 {
		t.Errorf("ConnMaxIdleTime = %d, want 30", db.ConnMaxIdleTime)
	}
	if db.PingTimeout != 5 {
		t.Errorf("PingTimeout = %d, want 5", db.PingTimeout)
	}
}

// TestLoad_CustomValues tests loading config with custom environment variables
func TestLoad_CustomValues(t *testing.T) {
	cleanEnv(t)

	// Set custom environment variables
	setEnv(t, map[string]string{
		"ETL_WORKERS":                   "8",
		"ETL_BATCH_SIZE":                "500",
		"POSTGRES_HOST":                 "db.example.com",
		"POSTGRES_PORT":                 "5433",
		"POSTGRES_USER":                 "testuser",
		"POSTGRES_PASSWORD":             "testpass",
		"POSTGRES_DATABASE":             "testdb",
		"POSTGRES_SSLMODE":              "require",
		"POSTGRES_MAX_OPEN_CONNS":       "50",
		"POSTGRES_MAX_IDLE_CONNS":       "20",
		"POSTGRES_CONN_MAX_LIFETIME_M":  "10",
		"POSTGRES_CONN_MAX_IDLE_TIME_S": "60",
		"POSTGRES_PING_TIMEOUT_S":       "10",
	})

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify custom values
	if config.Workers != 8 {
		t.Errorf("Workers = %d, want 8", config.Workers)
	}
	if config.BatchSize != 500 {
		t.Errorf("BatchSize = %d, want 500", config.BatchSize)
	}

	db := config.PostgresDatabase
	if db.Host != "db.example.com" {
		t.Errorf("Host = %s, want db.example.com", db.Host)
	}
	if db.Port != 5433 {
		t.Errorf("Port = %d, want 5433", db.Port)
	}
	if db.User != "testuser" {
		t.Errorf("User = %s, want testuser", db.User)
	}
	if db.Password != "testpass" {
		t.Errorf("Password = %s, want testpass", db.Password)
	}
	if db.Database != "testdb" {
		t.Errorf("Database = %s, want testdb", db.Database)
	}
	if db.SSLMode != "require" {
		t.Errorf("SSLMode = %s, want require", db.SSLMode)
	}
	if db.MaxOpenConns != 50 {
		t.Errorf("MaxOpenConns = %d, want 50", db.MaxOpenConns)
	}
	if db.MaxIdleConns != 20 {
		t.Errorf("MaxIdleConns = %d, want 20", db.MaxIdleConns)
	}
	if db.ConnMaxLifetime != 10 {
		t.Errorf("ConnMaxLifetime = %d, want 10", db.ConnMaxLifetime)
	}
	if db.ConnMaxIdleTime != 60 {
		t.Errorf("ConnMaxIdleTime = %d, want 60", db.ConnMaxIdleTime)
	}
	if db.PingTimeout != 10 {
		t.Errorf("PingTimeout = %d, want 10", db.PingTimeout)
	}
}

// TestLoad_PartialOverride tests loading config with partial environment variable override
func TestLoad_PartialOverride(t *testing.T) {
	cleanEnv(t)

	// Set only some environment variables
	setEnv(t, map[string]string{
		"ETL_WORKERS":   "16",
		"POSTGRES_HOST": "custom.host",
		"POSTGRES_PORT": "6543",
	})

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify overridden values
	if config.Workers != 16 {
		t.Errorf("Workers = %d, want 16", config.Workers)
	}
	if config.PostgresDatabase.Host != "custom.host" {
		t.Errorf("Host = %s, want custom.host", config.PostgresDatabase.Host)
	}
	if config.PostgresDatabase.Port != 6543 {
		t.Errorf("Port = %d, want 6543", config.PostgresDatabase.Port)
	}

	// Verify non-overridden values still have defaults
	if config.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000 (default)", config.BatchSize)
	}
	if config.PostgresDatabase.User != "admin" {
		t.Errorf("User = %s, want admin (default)", config.PostgresDatabase.User)
	}
	if config.PostgresDatabase.Database != "mapbot" {
		t.Errorf("Database = %s, want mapbot (default)", config.PostgresDatabase.Database)
	}
}

// TestLoad_InvalidIntValue tests loading config with invalid integer value
func TestLoad_InvalidIntValue(t *testing.T) {
	cleanEnv(t)

	setEnv(t, map[string]string{
		"ETL_WORKERS": "not_a_number",
	})

	_, err := Load()
	if err == nil {
		t.Error("Load() should return error for invalid int value")
	}
}

// TestLoad_FromDotEnvFile tests loading configuration from .env file
func TestLoad_FromDotEnvFile(t *testing.T) {
	cleanEnv(t)

	// Create temporary .env file
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")
	envContent := `ETL_WORKERS=12
ETL_BATCH_SIZE=2000
POSTGRES_HOST=envfile.host
POSTGRES_PORT=5555
`
	if err := os.WriteFile(envFile, []byte(envContent), 0600); err != nil {
		t.Fatalf("Failed to create .env file: %v", err)
	}

	// Change to temp directory so Load() finds the .env file
	originalWd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}
	defer func() {
		_ = os.Chdir(originalWd)
	}()

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify values from .env file
	if config.Workers != 12 {
		t.Errorf("Workers = %d, want 12", config.Workers)
	}
	if config.BatchSize != 2000 {
		t.Errorf("BatchSize = %d, want 2000", config.BatchSize)
	}
	if config.PostgresDatabase.Host != "envfile.host" {
		t.Errorf("Host = %s, want envfile.host", config.PostgresDatabase.Host)
	}
	if config.PostgresDatabase.Port != 5555 {
		t.Errorf("Port = %d, want 5555", config.PostgresDatabase.Port)
	}
}

// TestConnectionString_DefaultValues tests connection string format with default values
func TestConnectionString_DefaultValues(t *testing.T) {
	db := &PostgresDatabase{
		Host:     "localhost",
		Port:     5432,
		User:     "admin",
		Password: "password",
		Database: "mapbot",
		SSLMode:  "disable",
	}

	got := db.ConnectionString()
	want := "postgres://admin:password@localhost:5432/mapbot?sslmode=disable"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}

// TestConnectionString_CustomValues tests connection string with custom values
func TestConnectionString_CustomValues(t *testing.T) {
	db := &PostgresDatabase{
		Host:     "db.example.com",
		Port:     5433,
		User:     "customuser",
		Password: "custompass",
		Database: "customdb",
		SSLMode:  "require",
	}

	got := db.ConnectionString()
	want := "postgres://customuser:custompass@db.example.com:5433/customdb?sslmode=require"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}

// TestConnectionString_SpecialCharacters tests connection string with special characters in password
func TestConnectionString_SpecialCharacters(t *testing.T) {
	db := &PostgresDatabase{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "p@ss:w0rd!",
		Database: "db",
		SSLMode:  "disable",
	}

	got := db.ConnectionString()
	// Should contain the special characters (URL encoding is handled by database driver)
	if !strings.Contains(got, "p@ss:w0rd!") {
		t.Errorf("ConnectionString() should contain special characters in password")
	}
}

// TestConnectionString_SSLModes tests different SSL modes
func TestConnectionString_SSLModes(t *testing.T) {
	tests := []struct {
		sslMode string
		want    string
	}{
		{"disable", "postgres://user:pass@host:5432/db?sslmode=disable"},
		{"require", "postgres://user:pass@host:5432/db?sslmode=require"},
		{"verify-ca", "postgres://user:pass@host:5432/db?sslmode=verify-ca"},
		{"verify-full", "postgres://user:pass@host:5432/db?sslmode=verify-full"},
	}

	for _, tt := range tests {
		t.Run(tt.sslMode, func(t *testing.T) {
			db := &PostgresDatabase{
				Host:     "host",
				Port:     5432,
				User:     "user",
				Password: "pass",
				Database: "db",
				SSLMode:  tt.sslMode,
			}
			got := db.ConnectionString()
			if got != tt.want {
				t.Errorf("ConnectionString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNewPostgresDatabase tests creation of PostgresDatabase with constructor
func TestNewPostgresDatabase(t *testing.T) {
	db := NewPostgresDatabase("testhost", 6789, "testdb", "testuser", "testpass")

	// Verify provided values
	if db.Host != "testhost" {
		t.Errorf("Host = %s, want testhost", db.Host)
	}
	if db.Port != 6789 {
		t.Errorf("Port = %d, want 6789", db.Port)
	}
	if db.Database != "testdb" {
		t.Errorf("Database = %s, want testdb", db.Database)
	}
	if db.User != "testuser" {
		t.Errorf("User = %s, want testuser", db.User)
	}
	if db.Password != "testpass" {
		t.Errorf("Password = %s, want testpass", db.Password)
	}

	// Verify default values
	if db.SSLMode != "disable" {
		t.Errorf("SSLMode = %s, want disable (default)", db.SSLMode)
	}
	if db.MaxOpenConns != 25 {
		t.Errorf("MaxOpenConns = %d, want 25 (default)", db.MaxOpenConns)
	}
	if db.MaxIdleConns != 10 {
		t.Errorf("MaxIdleConns = %d, want 10 (default)", db.MaxIdleConns)
	}
	if db.ConnMaxLifetime != 5 {
		t.Errorf("ConnMaxLifetime = %d, want 5 (default)", db.ConnMaxLifetime)
	}
	if db.ConnMaxIdleTime != 30 {
		t.Errorf("ConnMaxIdleTime = %d, want 30 (default)", db.ConnMaxIdleTime)
	}
	if db.PingTimeout != 5 {
		t.Errorf("PingTimeout = %d, want 5 (default)", db.PingTimeout)
	}
}

// Helper functions

// cleanEnv removes all config-related environment variables
func cleanEnv(t *testing.T) {
	t.Helper()
	envVars := []string{
		"ETL_WORKERS",
		"ETL_BATCH_SIZE",
		"POSTGRES_HOST",
		"POSTGRES_PORT",
		"POSTGRES_USER",
		"POSTGRES_PASSWORD",
		"POSTGRES_DATABASE",
		"POSTGRES_SSLMODE",
		"POSTGRES_MAX_OPEN_CONNS",
		"POSTGRES_MAX_IDLE_CONNS",
		"POSTGRES_CONN_MAX_LIFETIME_M",
		"POSTGRES_CONN_MAX_IDLE_TIME_S",
		"POSTGRES_PING_TIMEOUT_S",
	}
	for _, key := range envVars {
		os.Unsetenv(key)
	}
}

// setEnv sets environment variables and schedules cleanup
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for key, value := range vars {
		os.Setenv(key, value)
		t.Cleanup(func() {
			os.Unsetenv(key)
		})
	}
}
