package model

import (
	"encoding/json"
	"log/slog"

	"github.com/twpayne/go-geom/encoding/geojson"
)

type GeoJSONFeature[T any] struct {
	Type       string           `json:"type"`
	Properties T                `json:"properties"`
	Geometry   geojson.Geometry `json:"geometry"`
}

type EntityWithGeoJSONGeometry[T any] struct {
	Data         T      `json:"data"`
	GeometryJSON string `json:"geometry_json"`
}

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
