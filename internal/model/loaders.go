package model

import "context"

// EntityWithGeoJSONGeometryLoader defines the interface for loading entities with geometry into a database.
type EntityWithGeoJSONGeometryLoader[T any] interface {
	Load(ctx context.Context, entities []EntityWithGeoJSONGeometry[T]) (int, error)
}

// EntityLoader defines the interface for loading entities into a database.
type EntityLoader[T any] interface {
	Load(ctx context.Context, entities []T) (int, error)
}
