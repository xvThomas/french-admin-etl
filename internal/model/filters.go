package model

// CsvRecordFilter defines an interface for filtering CSV records based on custom criteria.
type CsvRecordFilter interface {
	Filter(record map[string]string) bool
}
