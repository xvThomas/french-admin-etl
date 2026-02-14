// Package model defines core data structures and interfaces for ETL operations.
package model

import (
	"encoding/json"
	"log/slog"

	"github.com/twpayne/go-geom/encoding/geojson"
)

// CSVRecord is a map representing a single row from a CSV file.
type CSVRecord = map[string]string

// GeoJSONFeature represents a GeoJSON feature with typed properties.
type GeoJSONFeature[T any] struct {
	Type       string           `json:"type"`
	Properties T                `json:"properties"`
	Geometry   geojson.Geometry `json:"geometry"`
}

// EntityWithGeoJSONGeometry combines an entity with its GeoJSON geometry for database storage.
type EntityWithGeoJSONGeometry[T any] struct {
	Data            T      `json:"data"`
	GeoJSONGeometry string `json:"geometry_json"`
}

// ConvertGeoJSONGeometryToBytes converts a GeoJSON geometry to a JSON string for database storage.
func ConvertGeoJSONGeometryToBytes(geoJSONGeometry *geojson.Geometry) (string, error) {
	if geoJSONGeometry == nil {
		slog.Warn("Nil geometry")
		return "", nil
	}

	// Marshal the geometry to JSON string
	jsonBytes, err := json.Marshal(geoJSONGeometry)
	if err != nil {
		slog.Error("Marshal geometry to JSON", "error", err)
		return "", err
	}

	return string(jsonBytes), nil
}
