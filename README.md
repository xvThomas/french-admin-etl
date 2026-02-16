# ETL - French Administrative Reference Data Loader

[![CI Pipeline](https://github.com/xvThomas/french-admin-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/xvThomas/french-admin-etl/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/xvThomas/french-admin-etl/branch/main/graph/badge.svg)](https://codecov.io/gh/xvThomas/french-admin-etl)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/xvThomas/french-admin-etl)](https://goreportcard.com/report/github.com/xvThomas/french-admin-etl)
[![Trivy Security Scan](https://github.com/xvThomas/french-admin-etl/actions/workflows/ci.yml/badge.svg?event=push&job=security)](https://github.com/xvThomas/french-admin-etl/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ETL tool in Go for loading French administrative reference data (communes, départements, régions, EPCI) and demographic population data into PostGIS.

**Data sources:**

- https://adresse.data.gouv.fr/data/contours-administratifs/2023/geojson

- https://catalogue-donnees.insee.fr/fr/catalogue/recherche/DS_RP_POPULATION_PRINC

## Prerequisites

- **Go 1.21+**
- **PostgreSQL 17+ with PostGIS extension enabled**

## Quick Start

### 1. Configuration

Copy the example environment file and edit it:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```bash
# ETL Configuration
ETL_WORKERS=4              # Number of parallel workers (default: 4)
ETL_BATCH_SIZE=100         # Batch size for bulk inserts (default: 100)

# PostgreSQL Connection
POSTGRES_HOST=localhost    # Database host
POSTGRES_PORT=5432         # Database port
POSTGRES_USER=admin        # Database user
POSTGRES_PASSWORD=password # Database password
POSTGRES_DB=mapbot         # Database name

# Optional PostgreSQL Settings
# POSTGRES_SSLMODE=disable           # SSL mode: disable, require, verify-ca, verify-full
# POSTGRES_MAX_OPEN_CONNS=25         # Max open connections (default: 25)
# POSTGRES_MAX_IDLE_CONNS=10         # Max idle connections (default: 10)
# POSTGRES_CONN_MAX_LIFETIME_MIN=5   # Connection max lifetime in minutes
# POSTGRES_CONN_MAX_IDLE_TIME_MIN=30 # Connection max idle time in minutes
# POSTGRES_PING_TIMEOUT_S=5          # Ping timeout in seconds
```

### 2. Install Dependencies

```bash
make deps
```

## Main Commands

### Download Data

```bash
make download-regions       # Download régions data
make download-departements  # Download départements data
make download-epci          # Download EPCI data
make download-communes      # Download communes data (1000m, 100m, 5m precision)
make download-population:   # Download population data
```

### Build & Run

```bash
make build      # Build the ETL binary to bin/french-admin-etl
make run        # Run the ETL directly with go run
make run-binary # Build and run the compiled binary
```

### Development

```bash
make test       # Run tests
make benchmark  # Run benchmarks
make clean      # Clean generated files and downloaded data
```

### Help

```bash
make help       # Show all available commands
```

## CI/CD & Quality Assurance

This project includes a comprehensive GitHub Actions pipeline that runs on every push and pull request:

- **Tests**: Automatic test execution with coverage reporting
- **Security Scan**: Trivy vulnerability scanning for dependencies
- **Linting**: Code quality checks with golangci-lint
- **Build**: Binary compilation and artifact storage

### View Results in GitHub

- CI status: `Actions` tab in GitHub
- Security alerts: `Security` → `Code scanning` tab
- Coverage reports: Click the codecov badge

### Local Quality Checks

```bash
make test              # Run tests locally
make test-coverage     # Generate coverage report
golangci-lint run      # Run linter (requires golangci-lint installation)
```

## Features

- Parallel processing with configurable workers
- Batch insert transactions for performance
- Native PostGIS support (WKB geometries)
- Automatic spatial indexes
- Upsert support (INSERT ... ON CONFLICT)
- Error handling and retry logic
- Demographic population data processing (by age and gender)

## Database Structure

The ETL automatically creates the necessary tables with PostGIS geometry columns and spatial indexes. Tables created include:

- **Administrative data**: `communes`, `departements`, `regions`, `epci` with their respective administrative and geometric properties (`ref_admin` schema)
- **Demographic data**: `commune_population` with population statistics by age groups and gender for each commune (`demography` schema)

## Performance Tuning

For large datasets, adjust the configuration in `.env`:

- **ETL_WORKERS**: Increase to 8-16 for faster parallel processing (requires good CPU)
- **ETL_BATCH_SIZE**: Increase to 500-1000 to reduce transaction overhead
- **POSTGRES_MAX_OPEN_CONNS**: Adjust based on your PostgreSQL `max_connections` setting

Example for high-performance import:

```bash
ETL_WORKERS=16
ETL_BATCH_SIZE=1000
POSTGRES_MAX_OPEN_CONNS=50
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

Copyright (c) 2026 Xavier Thomas
