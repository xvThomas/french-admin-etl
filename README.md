# ETL - French Administrative Reference Data Loader

ETL tool in Go for loading French administrative reference data (communes, départements, régions, EPCI) into PostGIS.

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
POSTGRES_DATA_SCHEMA=public # Target schema (default: public)

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
make download-communes      # Download communes data (1000m, 100m, 5m precision)
make download-departements  # Download départements data
make download-regions       # Download régions data
make download-epci          # Download EPCI data
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

## Features

- Parallel processing with configurable workers
- Batch insert transactions for performance
- Native PostGIS support (WKB geometries)
- Automatic spatial indexes
- Upsert support (INSERT ... ON CONFLICT)
- Error handling and retry logic

## Database Structure

The ETL automatically creates the necessary tables with PostGIS geometry columns and spatial indexes. Tables created include `communes`, `departements`, `regions`, and `epci` with their respective administrative and geometric properties.

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

MIT
