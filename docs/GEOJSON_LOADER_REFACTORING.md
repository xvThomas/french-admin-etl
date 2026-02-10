# GeoJSONLoader Refactoring: Generic Implementation

## Summary

The `GeoJSONLoader` has been refactored from a type-specific implementation to a generic implementation that can work with any GeoJSON property type.

## Changes Made

### 1. Core Type Definition

**Before:**

```go
type GeoJSONLoader struct {
    config *config.Config
}

func NewGeoJSONLoader(config *config.Config) *GeoJSONLoader {
    return &GeoJSONLoader{config: config}
}
```

**After:**

```go
type GeoJSONLoader[T any] struct {
    config  *config.Config
    factory func() T // Factory function to create empty instances for JSON unmarshalling
}

func NewGeoJSONLoader[T any](config *config.Config, factory func() T) *GeoJSONLoader[T] {
    return &GeoJSONLoader[T]{
        config:  config,
        factory: factory,
    }
}
```

### 2. Dependency Changes

**Before:**

- Imported `etl/internal/infrastructure/input` for `GeoJsonRegionFeature`
- Hardcoded to work only with region features

**After:**

- Imports `etl/internal/model` for generic `GeoJSONFeature[T]`
- Works with any type that can be used as GeoJSON properties

### 3. Method Signatures

All methods now use generic type parameter `T`:

```go
// Load method
func (l *GeoJSONLoader[T]) Load(ctx context.Context, filePath string) error

// loadParallelStream method
func (l *GeoJSONLoader[T]) loadParallelStream(
    ctx context.Context,
    featureChan <-chan model.GeoJSONFeature[T],
) error

// loadBatch method
func (l *GeoJSONLoader[T]) loadBatch(
    ctx context.Context,
    features []model.GeoJSONFeature[T],
) (int, error)
```

### 4. JSON Decoding Solution

The key technical challenge was JSON unmarshalling with generic types. Go's `encoding/json` package requires concrete type instances at runtime.

**Problem:**

```go
// ❌ Can't instantiate generic type directly
var feature model.GeoJSONFeature[T]  // T is unknown at runtime
decoder.Decode(&feature)
```

**Solution:**

```go
// ✅ Use factory to create concrete type instance
feature := model.GeoJSONFeature[T]{Properties: l.factory()}
decoder.Decode(&feature)
```

### 5. Channel and Slice Type Updates

All internal channels and slices now use the generic type:

```go
// Channels
featureChan := make(chan model.GeoJSONFeature[T], l.config.BatchSize*2)
jobs := make(chan []model.GeoJSONFeature[T], l.config.Workers)

// Slices
batch := make([]model.GeoJSONFeature[T], 0, l.config.BatchSize)
```

## Technical Details

### Generic Type Constraints

- **Type Parameter**: `T any` - accepts any type for properties
- **Factory Function**: `func() T` - must return a zero-value instance of T
- **Model Dependency**: Uses `model.GeoJSONFeature[T]` as the container type

### Type Relationships

```go
model.GeoJSONFeature[T]                    // Generic base type
    ↓
input.GeoJsonRegionFeature                 // Type alias
    = model.GeoJSONFeature[RegionProperties]
    ↓
loader.NewGeoJSONLoader(cfg, func() RegionProperties {
    return RegionProperties{}
})
```

## Benefits

1. **Type Safety**: Compiler enforces correct property types at compile time
2. **Reusability**: Same loader implementation works for all GeoJSON schemas
3. **Zero Runtime Overhead**: Generics are resolved at compile time
4. **Extensibility**: New property types require no loader modifications
5. **Maintainability**: Single implementation reduces code duplication

## Testing

All existing tests have been updated to use the new generic signature:

```go
// Test instantiation
loader := NewGeoJSONLoader(cfg, func() input.RegionProperties {
    return input.RegionProperties{}
})
```

Test results:

- ✅ All loader tests pass
- ✅ Generic implementation maintains backward compatibility in behavior
- ✅ No performance regression

## Migration Guide

For any existing code using the loader:

1. Add import for property type package (e.g., `etl/internal/infrastructure/input`)
2. Add factory function when creating loader
3. No other changes required - behavior is identical

**Example Migration:**

```go
// Old code
loader := loader.NewGeoJSONLoader(cfg)

// New code
loader := loader.NewGeoJSONLoader(cfg, func() input.RegionProperties {
    return input.RegionProperties{}
})
```

## Future Improvements

Potential enhancements:

1. **Interface-based Processing**: Add optional callback interface for custom feature processing
2. **Streaming Output**: Allow features to be written to different sinks (database, file, etc.)
3. **Error Recovery**: Configurable error handling strategies per feature type
4. **Validation**: Add optional JSON schema validation for property types

## Related Files

- `/etl/internal/infrastructure/loader/geojson_loader.go` - Main implementation
- `/etl/internal/infrastructure/loader/geojson_loader_test.go` - Test suite
- `/etl/internal/model/geojson.go` - Generic feature type definition
- `/etl/internal/infrastructure/input/region.go` - Example property type
- `/etl/docs/GEOJSON_LOADER_USAGE.md` - Usage guide and examples

## References

- [Go Generics Tutorial](https://go.dev/doc/tutorial/generics)
- [Type Parameters Proposal](https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md)
- [JSON and Generics in Go](https://go.dev/blog/json)
