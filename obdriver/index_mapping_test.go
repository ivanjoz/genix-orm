package obdriver

import (
	"fmt"
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
		ID:        10961,
		Name:      "ob_mapping_records",
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

// One table with every index kind genix declares, mapped to OceanBase objects: scalar indexes and
// views are native indexes, a packed (delta) or hashed (cache group) index keys on its generated
// column, and an index over a slice is a multi-value index over that slice stored as JSON.
func TestMappingOfEveryIndexKind(t *testing.T) {
	table := compiledTableOf[mappingSchema, mappingRecord]()
	mainStatement := createTableStatement(table.mainTable)
	for _, fragment := range []string{
		"`email` VARCHAR(512),",            // a keyed string (D1)
		"`name` LONGTEXT,",                 // an unkeyed string
		"`tags` JSON,",                     // a slice a multi-value index keys on (O11)
		"`product_ids` LONGBLOB,",          // a slice no index keys on directly: colbin
		"`zz_dlt_status` BIGINT UNSIGNED,", // a packed delta column, unsigned layout
		"`zz_ig_date_status` INT,",
		"`zz_igs_date_product_ids` JSON,", // a cache group's hash set over a slice
		"PRIMARY KEY (`company_id`, `id`),",
		"INDEX `ix_ob_mapping_records__email_index_0` (`email`) GLOBAL,",
		"INDEX `ix_ob_mapping_records__client_id_index_1` (`company_id`, `client_id`) LOCAL,",
		// A global slice index: multi-value indexes are LOCAL only (§18b).
		"INDEX `ix_ob_mapping_records__tags_index_0` ((CAST(`tags`->'$' AS SIGNED ARRAY))) LOCAL,",
		// D2: the projection holds LONGTEXT / LONGBLOB columns, so neither view stores anything.
		"INDEX `ix_ob_mapping_records__pk_status_date_view` (`company_id`, `zz_pk_status_date`) LOCAL,",
		"INDEX `ix_ob_mapping_records__dlt_status_view` (`company_id`, `zz_dlt_status`) LOCAL,",
		"INDEX `ix_ob_mapping_records__zz_ig_date_status_index_1` (`company_id`, `zz_ig_date_status`) LOCAL,",
		"INDEX `ix_ob_mapping_records__zz_igs_date_product_ids_index_0` (`company_id`, (CAST(`zz_igs_date_product_ids`->'$' AS SIGNED ARRAY))) LOCAL",
		") DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'",
	} {
		if !strings.Contains(mainStatement, fragment) {
			t.Errorf("missing %q in:\n%s", fragment, mainStatement)
		}
	}
	if len(table.mainTable.indexes) != 7 || strings.Contains(mainStatement, "STORING") || strings.Contains(mainStatement, "PARTITION BY") {
		t.Errorf("want 7 indexes, no STORING and no partitioning:\n%s", mainStatement)
	}

	sideTableKeys := map[string]string{}
	for _, sideTable := range table.sideTables {
		sideTableKeys[sideTable.name] = strings.Join(sideTable.primaryKey, ", ")
	}
	expectedSideTables := map[string]string{
		"ob_mapping_records__delta_moves":   "partition_id, index_tag, pinned_prefix, updated, id",
		"ob_mapping_records__index_updated": "partition_id, index_id, index_hash",
	}
	if len(sideTableKeys) != len(expectedSideTables) {
		t.Errorf("side tables = %v", sideTableKeys)
	}
	for name, expectedKey := range expectedSideTables {
		if sideTableKeys[name] != expectedKey {
			t.Errorf("side table %s: key (%s), want (%s)", name, sideTableKeys[name], expectedKey)
		}
	}
}

// coveredRecord has no LOB or JSON column, so its view and delta index store their projection (D2).
type coveredRecord struct {
	TableStruct[coveredSchema, coveredRecord]
	CompanyID int32   `db:"company_id"`
	ID        int64   `db:"id"`
	Status    int8    `db:"status"`
	Price     float64 `db:"price"`
	Code      string  `db:"code"`
	Updated   int32   `json:"upd,omitempty"`
	Version   int16   `json:"ver,omitempty"`
}

type coveredSchema struct {
	TableStruct[coveredSchema, coveredRecord]
	CompanyID db.Col[*coveredSchema, int32]
	ID        db.Col[*coveredSchema, int64]
	Status    db.Col[*coveredSchema, int8]
	Price     db.Col[*coveredSchema, float64]
	Code      db.Col[*coveredSchema, string]
	Updated   db.Col[*coveredSchema, int32]
	Version   db.Col[*coveredSchema, int16]
}

func (e coveredSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID: 10962, Name: "ob_covered_records", Partition: e.CompanyID, Keys: db.Cols(e.ID),
		Indexes: []db.Index{
			{Type: db.TypeLocalIndex, Keys: db.Cols(e.Code)},
			{Type: db.TypeDelta, Keys: db.Cols(e.Status)},
		},
	}
}

func TestMappingStoresAProjectionWithoutLobs(t *testing.T) {
	statement := createTableStatement(compiledTableOf[coveredSchema, coveredRecord]().mainTable)
	if !strings.Contains(statement, "(`company_id`, `zz_dlt_status`) STORING (`status`, `price`, `code`, `updated`, `version`, `created`) LOCAL") ||
		!strings.Contains(statement, "`code` VARCHAR(512),") {
		t.Errorf("the delta index must store every non-key column:\n%s", statement)
	}
}

type stringSliceRecord struct {
	TableStruct[stringSliceSchema, stringSliceRecord]
	CompanyID int32    `db:"company_id"`
	ID        int64    `db:"id"`
	Labels    []string `db:"labels"`
}

type stringSliceSchema struct {
	TableStruct[stringSliceSchema, stringSliceRecord]
	CompanyID db.Col[*stringSliceSchema, int32]
	ID        db.Col[*stringSliceSchema, int64]
	Labels    db.Col[*stringSliceSchema, []string]
}

func (e stringSliceSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID: 10963, Name: "ob_string_slice_records", Partition: e.CompanyID, Keys: db.Cols(e.ID),
		Indexes: []db.Index{{Type: db.TypeGlobalIndex, Keys: db.Cols(e.Labels)}},
	}
}

// D3 refuses an index on a string slice. (O18, two indexes on one slice column, cannot be declared
// today: the shared compile collapses duplicate declarations and refuses local slice indexes.)
func TestMappingRefusesStringSliceIndexes(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(fmt.Sprint(recovered), "only integer slices") {
			t.Errorf("want a D3 panic, got %v", recovered)
		}
	}()
	compiledTableOf[stringSliceSchema, stringSliceRecord]()
}
