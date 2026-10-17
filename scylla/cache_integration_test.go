package scylla

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// A table on both cache protocols: by-IDs and a delta log filtered by status.
type cacheITRecord struct {
	TableStruct[cacheITSchema, cacheITRecord]
	CompanyID int32   `db:"company_id"`
	ID        int32   `db:"id"`
	Name      string  `db:"name"`
	Status    int8    `db:"status"`
	Updated   int32   `json:"upd,omitempty"`
	Version   int16   `json:"ver,omitempty"`
	Metadata  *string `json:"mdt,omitempty"`
}

type cacheITSchema struct {
	TableStruct[cacheITSchema, cacheITRecord]
	CompanyID Col[*cacheITSchema, int32]
	ID        Col[*cacheITSchema, int32]
	Name      Col[*cacheITSchema, string]
	Status    Col[*cacheITSchema, int8]
	Updated   Col[*cacheITSchema, int32]
	Version   Col[*cacheITSchema, int16]
}

func (e cacheITSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10040,
		Name:        "cache_it_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		CacheByIDs:  true,
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes:     []Index{{Type: TypeDelta, Keys: Cols(e.Status)}},
	}
}

// heldCursor is the cursor a client holds after applying every log file of the folder whole.
func heldCursor(t *testing.T, folder string) cache.DeltaCursor {
	t.Helper()
	files, err := cache.FileStore().ListFiles(folder)
	if err != nil {
		t.Fatal(err)
	}
	cursor := cache.DeltaCursor{}
	for _, fileKey := range files {
		content, _, _ := cache.FileStore().GetFile(fileKey)
		bucket, err := strconv.ParseInt(strings.TrimPrefix(fileKey, folder), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		cursor[bucket] = len(content)
	}
	return cursor
}

func splitMetadata(records []cacheITRecord) (data []cacheITRecord, metadataCount int) {
	for _, record := range records {
		if record.Metadata != nil {
			metadataCount++
		} else {
			data = append(data, record)
		}
	}
	return data, metadataCount
}

// TestCacheProtocolsOnScylla runs both caches against a real node: writes stamp the version and the
// logs, by-IDs reads what moved, and Delta() follows the cursor through an update, a soft delete and
// a replace.
func TestCacheProtocolsOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()
	withMemoryFileStore(t)

	for _, statement := range []string{
		"DROP MATERIALIZED VIEW IF EXISTS genix_orm_it.cache_it_records__pk_status_view",
		"DROP TABLE IF EXISTS genix_orm_it.cache_it_records",
	} {
		if err := QueryExec(statement); err != nil {
			t.Fatal(err)
		}
	}
	DeployScylla(0, &ScyllaController[cacheITRecord, cacheITSchema]{
		Table:  MakeScyllaTable[cacheITRecord, cacheITSchema](),
		Schema: MakeSchema[cacheITRecord, cacheITSchema](),
	})
	table := TableOf[cacheITRecord]()

	if err := Insert(&[]cacheITRecord{
		{CompanyID: 7, ID: 1, Name: "uno", Status: 1},
		{CompanyID: 7, ID: 2, Name: "dos", Status: 1},
	}); err != nil {
		t.Fatal(err)
	}

	// By-IDs: nothing held, so both come back.
	partitions, err := QueryCachedIDs[cacheITRecord, cacheITSchema](cache.CachedIDsRequest{
		CycleYear: cache.YearCycleOf(db.Now()),
		IDs:       []cache.CachedID{{Partition: 7, ID: 1}, {Partition: 7, ID: 2}},
	})
	if err != nil || len(partitions) != 1 || partitions[0].Partition != 7 || len(partitions[0].Records) != 2 {
		t.Fatalf("by-IDs: %+v, err %v", partitions, err)
	}

	// First sync: the active records and one Metadata per log file.
	firstSync := []cacheITRecord{}
	query := Query(&firstSync)
	query.CompanyID.Equals(7)
	if err := query.Delta(nil, int8(1)).Exec(); err != nil {
		t.Fatal(err)
	}
	data, metadataCount := splitMetadata(firstSync)
	if len(data) != 2 || metadataCount == 0 || data[0].Version != 1 {
		t.Fatalf("first sync: %+v", firstSync)
	}

	folder := "delta-log/genix_orm_it/10040/7/status/"
	cursor := heldCursor(t, folder)

	// An update raises the version and only that record comes back.
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	afterUpdate := []cacheITRecord{}
	query = Query(&afterUpdate)
	query.CompanyID.Equals(7)
	if err := query.Delta(cursor, int8(1)).Exec(); err != nil {
		t.Fatal(err)
	}
	data, _ = splitMetadata(afterUpdate)
	if len(data) != 1 || data[0].ID != 1 || data[0].Name != "uno bis" || data[0].Version != 2 {
		t.Fatalf("after the update: %+v", afterUpdate)
	}
	cursor = heldCursor(t, folder)

	// A soft delete logs a deleted entry: no record comes back, only the Metadata.
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 2, Status: 0}}, table.Status); err != nil {
		t.Fatal(err)
	}
	afterDelete := []cacheITRecord{}
	query = Query(&afterDelete)
	query.CompanyID.Equals(7)
	if err := query.Delta(cursor, int8(1)).Exec(); err != nil {
		t.Fatal(err)
	}
	if data, metadataCount = splitMetadata(afterDelete); len(data) != 0 || metadataCount == 0 {
		t.Fatalf("after the soft delete: %+v", afterDelete)
	}
	files, _ := cache.FileStore().ListFiles(folder)
	content, _, _ := cache.FileStore().GetFile(files[len(files)-1])
	entries, _ := cache.DecodeDeltaLogEntries(content, false)
	if !slices.ContainsFunc(entries, func(entry cache.DeltaLogEntry) bool { return entry.LogID == 2 && entry.IsDeleted }) {
		t.Fatalf("expected a deleted entry for 2, got %+v", entries)
	}

	// A cursor older than the log is a replace: the whole partition.
	replaced := []cacheITRecord{}
	query = Query(&replaced)
	query.CompanyID.Equals(7)
	if err := query.Delta(cache.DeltaCursor{1: 0}, int8(1)).Exec(); err != nil {
		t.Fatal(err)
	}
	if data, _ = splitMetadata(replaced); len(data) != 2 {
		t.Fatalf("replace: %+v", replaced)
	}
}
