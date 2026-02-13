package model

import "context"

type EntityWithGeoJSONGeometryLoader[T any] interface {
	Load(ctx context.Context, entities []EntityWithGeoJSONGeometry[T]) (int, error)
}

type EntityLoader[T any] interface {
	Load(ctx context.Context, entities []T) (int, error)
}
