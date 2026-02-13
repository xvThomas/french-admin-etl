package filters

import "slices"

type CsvRecordFilter interface {
	Filter(record map[string]string) bool
}

type csvRecordFilter struct {
	allowList map[string][]string
}

var _ CsvRecordFilter = (*csvRecordFilter)(nil)

func NewCsvRecordFilter() *csvRecordFilter {
	return &csvRecordFilter{
		allowList: make(map[string][]string),
	}
}

func (f *csvRecordFilter) AddToAllowList(column string, value []string) *csvRecordFilter {
	f.allowList[column] = value
	return f
}

func (f *csvRecordFilter) Filter(record map[string]string) bool {
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
