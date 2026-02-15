package filters

import (
	"testing"
)

// TestNewCsvRecordFilterFromAllowList tests the constructor
func TestNewCsvRecordFilterFromAllowList(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France", "Bretagne"},
	}

	filter := NewCsvRecordFilterFromAllowList(allowList)
	if filter == nil {
		t.Error("NewCsvRecordFilterFromAllowList() returned nil")
	}
}

// TestFilter_EmptyAllowList tests that empty allowlist keeps all records
func TestFilter_EmptyAllowList(t *testing.T) {
	filter := NewCsvRecordFilterFromAllowList(nil)

	tests := []struct {
		name   string
		record map[string]string
	}{
		{
			name: "simple record",
			record: map[string]string{
				"code":   "75",
				"nom":    "Paris",
				"region": "Île-de-France",
			},
		},
		{
			name:   "empty record",
			record: map[string]string{},
		},
		{
			name: "single field",
			record: map[string]string{
				"code": "13",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !filter.Filter(tt.record) {
				t.Errorf("Filter() with empty allowlist should accept all records")
			}
		})
	}
}

// TestFilter_SingleColumnMatch tests filtering on a single column with matching value
func TestFilter_SingleColumnMatch(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	record := map[string]string{
		"code":   "75",
		"nom":    "Paris",
		"region": "Île-de-France",
	}

	if !filter.Filter(record) {
		t.Error("Filter() should accept record with matching region")
	}
}

// TestFilter_SingleColumnNoMatch tests filtering on a single column with non-matching value
func TestFilter_SingleColumnNoMatch(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	record := map[string]string{
		"code":   "13",
		"nom":    "Marseille",
		"region": "Provence-Alpes-Côte d'Azur",
	}

	if filter.Filter(record) {
		t.Error("Filter() should reject record with non-matching region")
	}
}

// TestFilter_MultipleAllowedValues tests filtering with multiple allowed values for a column
func TestFilter_MultipleAllowedValues(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France", "Bretagne", "Provence-Alpes-Côte d'Azur"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
		want   bool
	}{
		{
			name: "matches first value",
			record: map[string]string{
				"region": "Île-de-France",
			},
			want: true,
		},
		{
			name: "matches middle value",
			record: map[string]string{
				"region": "Bretagne",
			},
			want: true,
		},
		{
			name: "matches last value",
			record: map[string]string{
				"region": "Provence-Alpes-Côte d'Azur",
			},
			want: true,
		},
		{
			name: "no match",
			record: map[string]string{
				"region": "Occitanie",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Filter(tt.record)
			if got != tt.want {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFilter_MultipleColumnsAllMatch tests filtering with multiple columns (AND logic), all matching
func TestFilter_MultipleColumnsAllMatch(t *testing.T) {
	allowList := map[string][]string{
		"region":      {"Île-de-France"},
		"departement": {"75", "92", "93"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	record := map[string]string{
		"code":        "75056",
		"nom":         "Paris",
		"region":      "Île-de-France",
		"departement": "75",
	}

	if !filter.Filter(record) {
		t.Error("Filter() should accept record when all columns match")
	}
}

// TestFilter_MultipleColumnsOneNoMatch tests filtering with multiple columns where one doesn't match
func TestFilter_MultipleColumnsOneNoMatch(t *testing.T) {
	allowList := map[string][]string{
		"region":      {"Île-de-France"},
		"departement": {"75", "92", "93"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
	}{
		{
			name: "region matches, departement doesn't",
			record: map[string]string{
				"code":        "78000",
				"nom":         "Versailles",
				"region":      "Île-de-France",
				"departement": "78",
			},
		},
		{
			name: "departement matches, region doesn't",
			record: map[string]string{
				"code":        "13001",
				"nom":         "Marseille",
				"region":      "Provence-Alpes-Côte d'Azur",
				"departement": "75",
			},
		},
		{
			name: "neither matches",
			record: map[string]string{
				"code":        "69001",
				"nom":         "Lyon",
				"region":      "Auvergne-Rhône-Alpes",
				"departement": "69",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if filter.Filter(tt.record) {
				t.Error("Filter() should reject record when any column doesn't match (AND logic)")
			}
		})
	}
}

// TestFilter_MissingColumn tests filtering when record is missing a required column
func TestFilter_MissingColumn(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
	}{
		{
			name:   "empty record",
			record: map[string]string{},
		},
		{
			name: "record with other columns",
			record: map[string]string{
				"code": "75",
				"nom":  "Paris",
			},
		},
		{
			name:   "nil record",
			record: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if filter.Filter(tt.record) {
				t.Error("Filter() should reject record missing required column")
			}
		})
	}
}

// TestFilter_EmptyStringValue tests filtering with empty string values
func TestFilter_EmptyStringValue(t *testing.T) {
	allowList := map[string][]string{
		"region": {""},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
		want   bool
	}{
		{
			name: "empty value matches",
			record: map[string]string{
				"region": "",
			},
			want: true,
		},
		{
			name: "non-empty value doesn't match",
			record: map[string]string{
				"region": "Île-de-France",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Filter(tt.record)
			if got != tt.want {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFilter_CaseSensitivity tests that filtering is case-sensitive
func TestFilter_CaseSensitivity(t *testing.T) {
	allowList := map[string][]string{
		"region": {"Île-de-France"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
		want   bool
	}{
		{
			name: "exact match",
			record: map[string]string{
				"region": "Île-de-France",
			},
			want: true,
		},
		{
			name: "lowercase doesn't match",
			record: map[string]string{
				"region": "île-de-france",
			},
			want: false,
		},
		{
			name: "uppercase doesn't match",
			record: map[string]string{
				"region": "ÎLE-DE-FRANCE",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Filter(tt.record)
			if got != tt.want {
				t.Errorf("Filter() = %v, want %v for case sensitivity test", got, tt.want)
			}
		})
	}
}

// TestFilter_ComplexScenario tests a realistic complex filtering scenario
func TestFilter_ComplexScenario(t *testing.T) {
	// Filter for communes in Île-de-France in specific departments
	allowList := map[string][]string{
		"reg_name": {"Île-de-France"},
		"dep_code": {"75", "92", "93", "94"},
	}
	filter := NewCsvRecordFilterFromAllowList(allowList)

	tests := []struct {
		name   string
		record map[string]string
		want   bool
	}{
		{
			name: "Paris - should match",
			record: map[string]string{
				"com_code": "75056",
				"com_name": "Paris",
				"dep_code": "75",
				"dep_name": "Paris",
				"reg_code": "11",
				"reg_name": "Île-de-France",
			},
			want: true,
		},
		{
			name: "Nanterre (92) - should match",
			record: map[string]string{
				"com_code": "92050",
				"com_name": "Nanterre",
				"dep_code": "92",
				"dep_name": "Hauts-de-Seine",
				"reg_code": "11",
				"reg_name": "Île-de-France",
			},
			want: true,
		},
		{
			name: "Versailles (78) - wrong department",
			record: map[string]string{
				"com_code": "78646",
				"com_name": "Versailles",
				"dep_code": "78",
				"dep_name": "Yvelines",
				"reg_code": "11",
				"reg_name": "Île-de-France",
			},
			want: false,
		},
		{
			name: "Marseille - wrong region",
			record: map[string]string{
				"com_code": "13055",
				"com_name": "Marseille",
				"dep_code": "13",
				"dep_name": "Bouches-du-Rhône",
				"reg_code": "93",
				"reg_name": "Provence-Alpes-Côte d'Azur",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Filter(tt.record)
			if got != tt.want {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFilter_AllowListNotInitializedMap tests with empty map (not nil)
func TestFilter_AllowListNotInitializedMap(t *testing.T) {
	allowList := make(map[string][]string)
	filter := NewCsvRecordFilterFromAllowList(allowList)

	record := map[string]string{
		"code":   "75",
		"nom":    "Paris",
		"region": "Île-de-France",
	}

	if !filter.Filter(record) {
		t.Error("Filter() with empty initialized map should accept all records")
	}
}
