package ydbdriver

import (
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

type mappingRecord struct {
	TableStruct[mappingSchema, mappingRecord]
	CompanyID  int32   `db:"company_id"`
	ID         int64   `db:"id"`
	Date       int16   `db:"date"`
	Status     int8    `db:"status"`
	ClientID   int32   `db:"client_id"`
	Email      string  `db:"email"`
	Tags       []int32 `db:"tags"`
	ProductIDs []int32 `db:"product_ids"`
	Name       string  `db:"name"`
	Updated    int32   `json:"upd,omitempty"`
	Version    int16   `json:"ver,omitempty"`
}

type mappingSchema struct {
	TableStruct[mappingSchema, mappingRecord]
	CompanyID  db.Col[*mappingSchema, int32]
	ID         db.Col[*mappingSchema, int64]
	Date       db.Col[*mappingSchema, int16]
	Status     db.Col[*mappingSchema, int8]
	ClientID   db.Col[*mappingSchema, int32]
	Email      db.Col[*mappingSchema, string]
	Tags       db.Col[*mappingSchema, []int32]
	ProductIDs db.Col[*mappingSchema, []int32]
	Name       db.Col[*mappingSchema, string]
	Updated    db.Col[*mappingSchema, int32]
	Version    db.Col[*mappingSchema, int16]
}

func (e mappingSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        10951,
		Name:      "ydb_mapping_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		Indexes: []db.Index{
			{Type: db.TypeGlobalIndex, Keys: db.Cols(e.Email)},
			{Type: db.TypeLocalIndex, Keys: db.Cols(e.ClientID)},
			{Type: db.TypeGlobalIndex, Keys: db.Cols(e.Tags)},
			{Type: db.TypeView, Keys: db.Cols(e.Status, e.Date)},
			{Type: db.TypeDelta, Keys: db.Cols(e.Status)},
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.Status)},
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.ProductIDs)},
		},
	}
}

// One table with every index kind genix declares, mapped to YDB objects: scalar indexes and views
// are native async indexes, a packed (delta) or hashed (cache group) index keys on its generated
// column, and an index over a slice is a key-only side table.
func TestMappingOfEveryIndexKind(t *testing.T) {
	table := compiledTableOf[mappingSchema, mappingRecord]()
	mainStatement := createTableStatement(table.mainTable)
	const storedColumns = "date, status, client_id, email, tags, product_ids, name, updated, version, created"
	for _, fragment := range []string{
		"tags String,", // a slice is colbin bytes (Y10)
		// Generated columns a native index keys on are stored; a packed delta column is unsigned.
		"zz_pk_status_date Int32,", "zz_dlt_status Uint64,", "zz_ig_date_status Int32,",
		"PRIMARY KEY (company_id, id),",
		"INDEX ix_ydb_mapping_records__email_index_0 GLOBAL ASYNC ON (email),",
		"INDEX ix_ydb_mapping_records__client_id_index_1 GLOBAL ASYNC ON (company_id, client_id),",
		"INDEX ix_ydb_mapping_records__pk_status_date_view GLOBAL ASYNC ON (company_id, zz_pk_status_date) COVER (" + storedColumns + "),",
		"INDEX ix_ydb_mapping_records__dlt_status_view GLOBAL ASYNC ON (company_id, zz_dlt_status) COVER (" + storedColumns + "),",
		"INDEX ix_ydb_mapping_records__zz_ig_date_status_index_1 GLOBAL ASYNC ON (company_id, zz_ig_date_status),",
	} {
		if !strings.Contains(mainStatement, fragment) {
			t.Errorf("missing %q in:\n%s", fragment, mainStatement)
		}
	}
	// The slice-hash group column is never a column: its hashes live in a side table.
	if strings.Contains(mainStatement, "zz_igs_") || len(table.mainTable.indexes) != 5 {
		t.Errorf("main table must hold 5 indexes and no zz_igs_ column:\n%s", mainStatement)
	}

	primaryKeyByPath := map[string]string{}
	for _, sideTable := range table.sideTables {
		primaryKeyByPath[sideTable.path] = strings.Join(sideTable.primaryKey, ", ")
	}
	expectedSideTables := map[string]string{
		// A global slice index keeps the partition after the element; a partitioned one before it.
		table.path + "__sx_ydb_mapping_records__tags_index_0":                    "element, company_id, id",
		table.path + "__sx_ydb_mapping_records__zz_igs_date_product_ids_index_0": "company_id, element, id",
		table.path + "__delta_moves":                                             "partition_id, index_tag, pinned_prefix, updated, id",
		table.path + "__index_updated":                                           "partition_id, index_id, index_hash",
	}
	if len(primaryKeyByPath) != len(expectedSideTables) {
		t.Errorf("side tables = %v", primaryKeyByPath)
	}
	for path, expectedKey := range expectedSideTables {
		if primaryKeyByPath[path] != expectedKey {
			t.Errorf("side table %s: key (%s), want (%s)", path, primaryKeyByPath[path], expectedKey)
		}
	}
}
