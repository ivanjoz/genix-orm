package db

import "testing"

// ToSnakeCase derives every default column name in the schema, so a change here
// silently renames columns. The single-letter cases exercise the separator-collapse
// loop, which does not terminate if its pattern is wrong.
func TestToSnakeCase(t *testing.T) {
	cases := map[string]string{
		"EmpresaID":              "empresa_id",
		"ID":                     "id",
		"Nombre":                 "nombre",
		"UpdatedBy":              "updated_by",
		"DetailProductIDs":       "detail_product_ids",
		"PaisCiudadID":           "pais_ciudad_id",
		"WarehouseProductStatus": "warehouse_product_status",
		"CacheVersion":           "cache_version",
		"S1":                     "s1",
		"N1":                     "n1",
		"A":                      "a",
	}
	for fieldName, want := range cases {
		if got := ToSnakeCase(fieldName); got != want {
			t.Errorf("ToSnakeCase(%q) = %q, want %q", fieldName, got, want)
		}
	}
}

func TestParseDBTag(t *testing.T) {
	if tag := ParseDBTag(""); tag.ColumnName != "" || tag.HasCollectionOptions() {
		t.Errorf("empty tag = %+v, want zero", tag)
	}
	tag := ParseDBTag("pais_ciudad_id")
	if tag.ColumnName != "pais_ciudad_id" || tag.HasCollectionOptions() {
		t.Errorf("name-only tag = %+v", tag)
	}
	// Options are case-insensitive and may appear in any order after the name.
	tag = ParseDBTag("tags, LIST ,frozen")
	if tag.ColumnName != "tags" || !tag.IsList || !tag.IsFrozen || tag.IsSet {
		t.Errorf("option tag = %+v", tag)
	}
}
