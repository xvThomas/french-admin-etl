// Package filters provides utilities for filtering CSV records based on custom criteria.
package filters

import (
	"french-admin-etl/internal/model"
	"slices"
)

type csvRecordFilterFromAllowList struct {
	allowList map[string][]string
}

var _ model.CsvRecordFilter = (*csvRecordFilterFromAllowList)(nil)

// NewCsvRecordFilterFromAllowList creates a new CSV record filter with the provided allowlist.
func NewCsvRecordFilterFromAllowList(allowList map[string][]string) model.CsvRecordFilter {
	return &csvRecordFilterFromAllowList{
		allowList: allowList,
	}
}

func (f *csvRecordFilterFromAllowList) Filter(record map[string]string) bool {
	// If no allowlist is configured, keep all records
	if len(f.allowList) == 0 {
		return true
	}

	// All allowlist columns must match (AND logic)
	for column, allowListValues := range f.allowList {
		value, exists := record[column]
		if !exists {
			return false
		}
		if !slices.Contains(allowListValues, value) {
			return false
		}
	}
	return true
}
