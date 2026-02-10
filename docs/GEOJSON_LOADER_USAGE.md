# GeoJSONLoader Usage Guide

## Overview

`GeoJSONLoader` is now a generic type that can load GeoJSON files with any property type. This makes it reusable across different GeoJSON schemas without code duplication.

## Basic Usage

### 1. With Region Properties (Original Use Case)

```go
import (
    "context"
    "etl/internal/infrastructure/config"
    "etl/internal/infrastructure/input"
    "etl/internal/infrastructure/loader"
)

func loadRegions() {
    cfg := &config.Config{
        Workers:   4,
        BatchSize: 100,
    }

    // Create loader with RegionProperties factory
    loader := loader.NewGeoJSONLoader(cfg, func() input.RegionProperties {
        return input.RegionProperties{}
    })

    ctx := context.Background()
    err := loader.Load(ctx, "path/to/regions.geojson")
    if err != nil {
        log.Fatal(err)
    }
}
```

### 2. With Custom Properties

First, define your custom properties type:

```go
package input

import "etl/internal/model"

// Custom properties for city features
type CityProperties struct {
    Name       string  `json:"name"`
    Population int     `json:"population"`
    Country    string  `json:"country"`
    Capital    bool    `json:"capital"`
}

// Type alias for convenience
type GeoJsonCityFeature = model.GeoJSONFeature[CityProperties]
```

Then use it with the loader:

```go
func loadCities() {
    cfg := &config.Config{
        Workers:   4,
        BatchSize: 50,
    }

    // Create loader with CityProperties factory
    loader := loader.NewGeoJSONLoader(cfg, func() input.CityProperties {
        return input.CityProperties{}
    })

    ctx := context.Background()
    err := loader.Load(ctx, "path/to/cities.geojson")
    if err != nil {
        log.Fatal(err)
    }
}
```

### 3. With Complex Nested Properties

```go
package input

type BuildingDetails struct {
    Type      string  `json:"type"`
    Height    float64 `json:"height"`
    FloorCount int    `json:"floors"`
}

type BuildingProperties struct {
    ID      string          `json:"id"`
    Address string          `json:"address"`
    Details BuildingDetails `json:"details"`
    Tags    []string        `json:"tags"`
}

type GeoJsonBuildingFeature = model.GeoJSONFeature[BuildingProperties]
```

```go
func loadBuildings() {
    cfg := &config.Config{
        Workers:   8,
        BatchSize: 200,
    }

    // Create loader with BuildingProperties factory
    loader := loader.NewGeoJSONLoader(cfg, func() input.BuildingProperties {
        return input.BuildingProperties{}
    })

    ctx := context.Background()
    err := loader.Load(ctx, "path/to/buildings.geojson")
    if err != nil {
        log.Fatal(err)
    }
}
```

## Why the Factory Function?

The factory function `func() T` is required because of how Go's `encoding/json` package works with generic types:

1. **Type Erasure**: At runtime, Go needs to create concrete instances to unmarshal JSON data
2. **Generic Limitation**: We cannot directly instantiate a generic type `T` without knowing its concrete type
3. **Solution**: The factory function provides a way to create empty instances of the concrete type

### Example Without Factory (Won't Work)

```go
// ❌ This won't compile - can't instantiate generic type directly
feature := model.GeoJSONFeature[T]{}
```

### With Factory (Works)

```go
// ✅ This works - factory creates the concrete type
feature := model.GeoJSONFeature[T]{Properties: l.factory()}
decoder.Decode(&feature)
```

## Performance Considerations

- **Workers**: Number of parallel workers (default: 4). Increase for larger files
- **BatchSize**: Features per batch (default: 100). Adjust based on feature size
- **Memory**: The loader uses streaming to minimize memory usage
- **Concurrency**: Features are processed in parallel batches for optimal throughput

## Complete Example: Multiple Feature Types

```go
package main

import (
    "context"
    "etl/internal/infrastructure/config"
    "etl/internal/infrastructure/input"
    "etl/internal/infrastructure/loader"
    "log"
)

func main() {
    cfg := &config.Config{
        Workers:   4,
        BatchSize: 100,
    }

    ctx := context.Background()

    // Load regions
    regionLoader := loader.NewGeoJSONLoader(cfg, func() input.RegionProperties {
        return input.RegionProperties{}
    })
    if err := regionLoader.Load(ctx, "data/regions.geojson"); err != nil {
        log.Fatalf("Failed to load regions: %v", err)
    }

    // Load cities with different properties
    cityLoader := loader.NewGeoJSONLoader(cfg, func() input.CityProperties {
        return input.CityProperties{}
    })
    if err := cityLoader.Load(ctx, "data/cities.geojson"); err != nil {
        log.Fatalf("Failed to load cities: %v", err)
    }

    log.Println("All GeoJSON files loaded successfully!")
}
```

## Architecture Benefits

1. **Type Safety**: Compile-time type checking for properties
2. **Reusability**: One loader implementation for all GeoJSON schemas
3. **Flexibility**: Easy to add new property types without modifying the loader
4. **Performance**: Maintains streaming and parallel processing capabilities
5. **Maintainability**: Single source of truth for GeoJSON loading logic

## Migration from Non-Generic Version

**Before:**

```go
loader := loader.NewGeoJSONLoader(cfg)
```

**After:**

```go
loader := loader.NewGeoJSONLoader(cfg, func() input.RegionProperties {
    return input.RegionProperties{}
})
```

Just add the factory function as the second parameter!
