# Makefile pour l'ETL Référentiel Administratif Français

.PHONY: help download-data build run clean test

# Couleurs pour l'output
COLOR_RESET = \033[0m
COLOR_BOLD = \033[1m
COLOR_GREEN = \033[32m
COLOR_YELLOW = \033[33m
COLOR_BLUE = \033[34m

help: ## Show help
	@echo "$(COLOR_BOLD)French Administrative Reference Data Loader$(COLOR_RESET)"
	@echo ""
	@echo "$(COLOR_GREEN)Available commands:$(COLOR_RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(COLOR_BLUE)%-24s$(COLOR_RESET) %s\n", $$1, $$2}'

download-communes: ## Download communes data
	@echo "$(COLOR_YELLOW)Downloading communes...$(COLOR_RESET)"
	@curl -o data/communes-1000m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/communes-1000m.geojson'
	@curl -o data/communes-100m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/communes-100m.geojson'
	@curl -o data/communes-5m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/communes-5m.geojson'
	@echo "$(COLOR_GREEN)✓ Communes downloaded$(COLOR_RESET)"

download-departements: ## Download départements data
	@echo "$(COLOR_YELLOW)Downloading départements...$(COLOR_RESET)"
	@mkdir -p data
	@curl -o data/departements-1000m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/departements-1000m.geojson'
	@curl -o data/departements-100m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/departements-100m.geojson'
	@curl -o data/departements-5m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/departements-5m.geojson'
	@echo "$(COLOR_GREEN)✓ Départements downloaded$(COLOR_RESET)"

download-regions: ## Download régions data
	@echo "$(COLOR_YELLOW)Downloading régions...$(COLOR_RESET)"
	@mkdir -p data
	@curl -o data/regions-1000m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/regions-1000m.geojson'
	@curl -o data/regions-100m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/regions-100m.geojson'
	@curl -o data/regions-5m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/regions-5m.geojson'
	@echo "$(COLOR_GREEN)✓ Régions downloaded$(COLOR_RESET)"

download-epci: ## Download EPCI data
	@echo "$(COLOR_YELLOW)Downloading EPCI...$(COLOR_RESET)"
	@mkdir -p data
	@curl -o data/epci-1000m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/epci-1000m.geojson'
	@curl -o data/epci-100m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/epci-100m.geojson'
	@curl -o data/epci-5m.geojson 'https://adresse.data.gouv.fr/data/contours-administratifs/2024/geojson/epci-5m.geojson'
	@echo "$(COLOR_GREEN)✓ EPCI downloaded$(COLOR_RESET)"

download-population: ## Download population data
	@echo "$(COLOR_YELLOW)Downloading population data...$(COLOR_RESET)"
	@curl -o data/population.csv 'https://api.insee.fr/melodi/data/DS_RP_POPULATION_PRINC'
	@echo "$(COLOR_GREEN)✓ Population data downloaded$(COLOR_RESET)"

download-data: ## Download all data
	download-communes download-departements download-regions download-epci download-population

build: ## Build the binary
	@echo "$(COLOR_YELLOW)Building...$(COLOR_RESET)"
	@go build -o bin/french-admin-etl ./cmd/main.go
	@echo "$(COLOR_GREEN)✓ Built: ./bin/french-admin-etl$(COLOR_RESET)"

run: ## Run the ETL
	@echo "$(COLOR_YELLOW)Running the ETL...$(COLOR_RESET)"
	@go run cmd/main.go

run-binary: build ## Run the compiled binary
	@echo "$(COLOR_YELLOW)Running the binary...$(COLOR_RESET)"
	@DATABASE_URL="$(DATABASE_URL)" GEOJSON_FILE="data/communes.geojson" ./bin/french-admin-etl

deps: ## Install dependencies
	@echo "$(COLOR_YELLOW)Installing Go dependencies...$(COLOR_RESET)"
	@go mod download
	@go mod tidy
	@echo "$(COLOR_GREEN)✓ Dependencies installed$(COLOR_RESET)"

test: ## Run tests
	@echo "$(COLOR_YELLOW)Running tests...$(COLOR_RESET)"
	@go test -v ./...

benchmark: ## Run benchmarks
	@echo "$(COLOR_YELLOW)Running benchmarks...$(COLOR_RESET)"
	@go test -bench=. -benchmem ./...

clean: ## Clean generated files
	@echo "$(COLOR_YELLOW)Cleaning...$(COLOR_RESET)"
	@rm -f bin/french-admin-etl
	@rm -rf data/*.geojson
	@echo "$(COLOR_GREEN)✓ Cleaned$(COLOR_RESET)"

all: setup download-data deps build run stats ## Do everything: setup, download, build and run

.DEFAULT_GOAL := help
