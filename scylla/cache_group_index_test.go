package scylla

import (
	"strings"
	"testing"
)

// cacheGroupScalarRecord groups by two scalar keys: its hash is an int with a local index.
type cacheGroupScalarRecord struct {
	TableStruct[cacheGroupScalarSchema, cacheGroupScalarRecord]
	CompanyID int32 `db:"company_id"`
	ID        int64 `db:"id"`
	Date      int16 `db:"date"`
	Status    int8  `db:"status"`
}

type cacheGroupScalarSchema struct {
	TableStruct[cacheGroupScalarSchema, cacheGroupScalarRecord]
	CompanyID Col[*cacheGroupScalarSchema, int32]
	ID        Col[*cacheGroupScalarSchema, int64]
	Date      Col[*cacheGroupScalarSchema, int16]
	Status    Col[*cacheGroupScalarSchema, int8]
}

func (e cacheGroupScalarSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10044,
		Namespace: "test_keyspace",
		Name:      "cache_group_scalar_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeCacheGroup, Keys: Cols(e.Date, e.Status)}},
	}
}

// sliceLocalIndexRecord declares a local index on a slice column, which Scylla cannot create.
type sliceLocalIndexRecord struct {
	TableStruct[sliceLocalIndexSchema, sliceLocalIndexRecord]
	CompanyID  int32   `db:"company_id"`
	ID         int64   `db:"id"`
	ProductIDs []int32 `db:"product_ids,list"`
}

type sliceLocalIndexSchema struct {
	TableStruct[sliceLocalIndexSchema, sliceLocalIndexRecord]
	CompanyID  Col[*sliceLocalIndexSchema, int32]
	ID         Col[*sliceLocalIndexSchema, int64]
	ProductIDs Col[*sliceLocalIndexSchema, []int32]
}

func (e sliceLocalIndexSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10045,
		Namespace: "test_keyspace",
		Name:      "slice_local_index_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeLocalIndex, Keys: Cols(e.ProductIDs)}},
	}
}

// A multi-key cache group hashes its keys into a virtual column: an int hash is indexed locally (one
// partition answers every grouped read), a set<int> hash globally, the only index Scylla has on a
// collection (checked live: "Local secondary index on collection column ... is not implemented yet").
func TestCacheGroupHashIndexIsLocalUnlessACollection(t *testing.T) {
	resetORMTableCachesForTesting()

	scalarTable := MakeScyllaTable[cacheGroupScalarRecord, cacheGroupScalarSchema]()
	scalarIndex := scalarTable.indexes["cache_group_scalar_records__zz_ig_date_status_index_1"]
	if scalarIndex == nil {
		t.Fatalf("no local hash index, got %v", mapKeys(scalarTable.indexes))
	}
	if script := scalarIndex.getCreateScript(); script != "CREATE INDEX cache_group_scalar_records__zz_ig_date_status_index_1 ON test_keyspace.cache_group_scalar_records ((company_id),zz_ig_date_status)" {
		t.Fatalf("scalar hash index: %s", script)
	}

	// index_group_records groups by (date, client_id, product_ids): product_ids is a slice.
	collectionTable := MakeScyllaTable[indexGroupRecord, indexGroupSchema]()
	collectionIndex := collectionTable.indexes["index_group_records__zz_igs_date_client_id_product_ids_index_0"]
	if collectionIndex == nil {
		t.Fatalf("no global hash index, got %v", mapKeys(collectionTable.indexes))
	}
	if script := collectionIndex.getCreateScript(); script != "CREATE INDEX index_group_records__zz_igs_date_client_id_product_ids_index_0 ON test_keyspace.index_group_records (VALUES(zz_igs_date_client_id_product_ids))" {
		t.Fatalf("collection hash index: %s", script)
	}
}

func TestLocalIndexOnASliceIsRejectedAtCompile(t *testing.T) {
	resetORMTableCachesForTesting()
	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(recovered.(string), "no local index on a collection") {
			t.Fatalf("expected the compile to refuse a local index on a slice, got %v", recovered)
		}
	}()
	MakeScyllaTable[sliceLocalIndexRecord, sliceLocalIndexSchema]()
}

func mapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
