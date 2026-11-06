package scylla

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// cacheGroupS3Record keeps its (date, status) groups in the file store.
type cacheGroupS3Record struct {
	TableStruct[cacheGroupS3Schema, cacheGroupS3Record]
	CompanyID int32  `db:"company_id"`
	ID        int64  `db:"id"`
	Date      int16  `db:"date"`
	Status    int8   `db:"status"`
	ClientID  int32  `db:"client_id"`
	Name      string `db:"name"`
}

type cacheGroupS3Schema struct {
	TableStruct[cacheGroupS3Schema, cacheGroupS3Record]
	CompanyID Col[*cacheGroupS3Schema, int32]
	ID        Col[*cacheGroupS3Schema, int64]
	Date      Col[*cacheGroupS3Schema, int16]
	Status    Col[*cacheGroupS3Schema, int8]
	ClientID  Col[*cacheGroupS3Schema, int32]
	Name      Col[*cacheGroupS3Schema, string]
}

func (e cacheGroupS3Schema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10046,
		Namespace: "test_keyspace",
		Name:      "cache_group_s3_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeCacheGroupS3, Keys: Cols(e.Date, e.Status)}},
	}
}

// cacheGroupS3HiddenRecord stores a column colbin drops: its snapshots would serve it zeroed.
type cacheGroupS3HiddenRecord struct {
	TableStruct[cacheGroupS3HiddenSchema, cacheGroupS3HiddenRecord]
	CompanyID int32  `db:"company_id"`
	ID        int64  `db:"id"`
	Date      int16  `db:"date"`
	Secret    string `db:"secret" json:"-" cb:"-"`
}

type cacheGroupS3HiddenSchema struct {
	TableStruct[cacheGroupS3HiddenSchema, cacheGroupS3HiddenRecord]
	CompanyID Col[*cacheGroupS3HiddenSchema, int32]
	ID        Col[*cacheGroupS3HiddenSchema, int64]
	Date      Col[*cacheGroupS3HiddenSchema, int16]
	Secret    Col[*cacheGroupS3HiddenSchema, string]
}

func (e cacheGroupS3HiddenSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10047,
		Namespace: "test_keyspace",
		Name:      "cache_group_s3_hidden_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeCacheGroupS3, Keys: Cols(e.Date)}},
	}
}

func TestCacheGroupS3RejectsARecordColbinCannotRoundTrip(t *testing.T) {
	resetORMTableCachesForTesting()
	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(recovered.(string), `no cb:"-" on a stored column`) {
			t.Fatalf("expected the compile to refuse the table, got %v", recovered)
		}
	}()
	MakeScyllaTable[cacheGroupS3HiddenRecord, cacheGroupS3HiddenSchema]()
}

// fakeGroupDatabase stands in for the group's database read: it counts the reads, records the
// post-filter each got, and returns copies of its rows filtered the way the scan would.
type fakeGroupDatabase struct {
	rows        []cacheGroupS3Record
	reads       int
	lastFilter  []ColumnStatement
	beforeReply func()
}

func (fake *fakeGroupDatabase) read(scyllaTable ScyllaTable) func([]ColumnStatement) ([]unsafe.Pointer, error) {
	return func(postFilterStatements []ColumnStatement) ([]unsafe.Pointer, error) {
		fake.reads++
		fake.lastFilter = postFilterStatements
		if fake.beforeReply != nil {
			fake.beforeReply()
		}
		pointers := make([]unsafe.Pointer, len(fake.rows))
		for index := range fake.rows {
			rowCopy := fake.rows[index]
			pointers[index] = unsafe.Pointer(&rowCopy)
		}
		return filterGroupRecords(pointers, postFilterStatements, scyllaTable), nil
	}
}

func groupRecordIDs(records []unsafe.Pointer) []int64 {
	ids := make([]int64, len(records))
	for index, recordPointer := range records {
		ids[index] = (*cacheGroupS3Record)(recordPointer).ID
	}
	return ids
}

func readStoredGroupSnapshot(t *testing.T, store *utils.MemoryFileStore, key string) (cache.RecordSnapshot, bool) {
	t.Helper()
	content, _, err := store.GetFile(key)
	if err != nil {
		return cache.RecordSnapshot{}, false
	}
	snapshot, err := cache.DecodeRecordSnapshot(content)
	if err != nil {
		t.Fatalf("stored file does not decode: %v", err)
	}
	return snapshot, true
}

// A settled group is read whole from the database once and stored under its last_write; the next
// read with that label is served from the file, post-filtered in memory; a new label, or a file past
// the max age, reads the database again and rewrites it.
func TestCacheGroupSnapshotServesAndRewrites(t *testing.T) {
	resetORMTableCachesForTesting()
	store := withMemoryFileStore(t)
	scyllaTable := MakeScyllaTable[cacheGroupS3Record, cacheGroupS3Schema]()
	queryPlan := &db.IndexGroupReadPlan{PartitionValue: 7, IndexGroup: scyllaTable.IndexGroups[0]}
	sink := makeRecordSink(&[]cacheGroupS3Record{})
	snapshotKey := cache.CacheGroupSnapshotKey(scyllaTable.Name, 7, queryPlan.IndexGroup.IndexID, 55)
	database := &fakeGroupDatabase{rows: []cacheGroupS3Record{
		{CompanyID: 7, ID: 1, Date: 100, Status: 1, ClientID: 3, Name: "a"},
		{CompanyID: 7, ID: 2, Date: 100, Status: 1, ClientID: 4, Name: "b"},
	}}
	clientFilter := []ColumnStatement{{Col: "client_id", Operator: "=", Value: int32(4)}}
	const readTime = int32(5000)

	// Miss: the database answers with no post-filter, the file is written, the response filtered.
	records, err := readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1000},
		database.read(scyllaTable), clientFilter, readTime)
	if err != nil || database.reads != 1 || database.lastFilter != nil {
		t.Fatalf("miss: err %v, reads %d, filter %v", err, database.reads, database.lastFilter)
	}
	if ids := groupRecordIDs(records); len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("miss: got %v", ids)
	}
	snapshot, isStored := readStoredGroupSnapshot(t, store, snapshotKey)
	if !isStored || snapshot.Cursor != 1000 || snapshot.ReadTime != readTime {
		t.Fatalf("miss: stored %v %+v", isStored, snapshot)
	}

	// Same label: served from the file, no database read, filtered in memory.
	records, err = readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1000},
		database.read(scyllaTable), clientFilter, readTime+10)
	if err != nil || database.reads != 1 {
		t.Fatalf("hit: err %v, reads %d", err, database.reads)
	}
	if ids := groupRecordIDs(records); len(ids) != 1 || ids[0] != 2 || (*cacheGroupS3Record)(records[0]).Name != "b" {
		t.Fatalf("hit: got %v", ids)
	}
	records, _ = readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1000},
		database.read(scyllaTable), nil, readTime+10)
	if len(records) != 2 || database.reads != 1 {
		t.Fatalf("hit without filter: %d records, reads %d", len(records), database.reads)
	}

	// New label: the database answers and the file carries it.
	database.rows = append(database.rows, cacheGroupS3Record{CompanyID: 7, ID: 3, Date: 100, Status: 1, ClientID: 4})
	records, _ = readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1002},
		database.read(scyllaTable), clientFilter, readTime+20)
	if database.reads != 2 || len(records) != 2 {
		t.Fatalf("new label: reads %d, %d records", database.reads, len(records))
	}
	if snapshot, _ := readStoredGroupSnapshot(t, store, snapshotKey); snapshot.Cursor != 1002 || snapshot.ReadTime != readTime+20 {
		t.Fatalf("new label: stored %+v", snapshot)
	}

	// Past the max age, the same label reads the database and rewrites the file.
	expiredReadTime := readTime + 20 + cache.RecordSnapshotMaxAgeTicks + 1
	readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1002},
		database.read(scyllaTable), nil, expiredReadTime)
	if database.reads != 3 {
		t.Fatalf("expired: reads %d", database.reads)
	}
	if snapshot, _ := readStoredGroupSnapshot(t, store, snapshotKey); snapshot.ReadTime != expiredReadTime {
		t.Fatalf("expired: stored %+v", snapshot)
	}
}

// An unsettled group (label 0) and an empty group are read from the database and never stored.
func TestCacheGroupSnapshotSkipsUnsettledAndEmptyGroups(t *testing.T) {
	resetORMTableCachesForTesting()
	store := withMemoryFileStore(t)
	scyllaTable := MakeScyllaTable[cacheGroupS3Record, cacheGroupS3Schema]()
	queryPlan := &db.IndexGroupReadPlan{PartitionValue: 7, IndexGroup: scyllaTable.IndexGroups[0]}
	sink := makeRecordSink(&[]cacheGroupS3Record{})
	clientFilter := []ColumnStatement{{Col: "client_id", Operator: "=", Value: int32(4)}}

	database := &fakeGroupDatabase{rows: []cacheGroupS3Record{{CompanyID: 7, ID: 1, Date: 100, Status: 1, ClientID: 4}}}
	readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 0},
		database.read(scyllaTable), clientFilter, 5000)
	if database.reads != 1 || len(database.lastFilter) != 1 {
		t.Fatalf("unsettled: reads %d, filter %v (the scan filters it)", database.reads, database.lastFilter)
	}
	if _, isStored := readStoredGroupSnapshot(t, store, cache.CacheGroupSnapshotKey(scyllaTable.Name, 7, queryPlan.IndexGroup.IndexID, 55)); isStored {
		t.Fatal("an unsettled group must never be stored")
	}

	emptyDatabase := &fakeGroupDatabase{}
	readCacheGroupRecords(sink, scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 56, LastWrite: 1000},
		emptyDatabase.read(scyllaTable), nil, 5000)
	if _, isStored := readStoredGroupSnapshot(t, store, cache.CacheGroupSnapshotKey(scyllaTable.Name, 7, queryPlan.IndexGroup.IndexID, 56)); isStored {
		t.Fatal("an empty group must not be stored")
	}
}

// A plain TypeCacheGroup never touches the store.
func TestCacheGroupWithoutS3IgnoresTheStore(t *testing.T) {
	resetORMTableCachesForTesting()
	store := withMemoryFileStore(t)
	scyllaTable := MakeScyllaTable[cacheGroupScalarRecord, cacheGroupScalarSchema]()
	queryPlan := &db.IndexGroupReadPlan{PartitionValue: 7, IndexGroup: scyllaTable.IndexGroups[0]}
	reads := 0
	readCacheGroupRecords(makeRecordSink(&[]cacheGroupScalarRecord{}), scyllaTable, queryPlan, db.IndexGroupFetchState{HashValue: 55, LastWrite: 1000},
		func([]ColumnStatement) ([]unsafe.Pointer, error) {
			reads++
			return []unsafe.Pointer{unsafe.Pointer(&cacheGroupScalarRecord{CompanyID: 7, ID: 1})}, nil
		}, nil, 5000)
	if reads != 1 {
		t.Fatalf("reads %d", reads)
	}
	if _, isStored := readStoredGroupSnapshot(t, store, cache.CacheGroupSnapshotKey(scyllaTable.Name, 7, queryPlan.IndexGroup.IndexID, 55)); isStored {
		t.Fatal("a TypeCacheGroup must not write snapshots")
	}
}

// Two requests racing on a miss: the one that writes second holds a stale ETag, its write is dropped
// and its response is still answered.
func TestCacheGroupSnapshotDropsAWriteRacedByAnother(t *testing.T) {
	resetORMTableCachesForTesting()
	store := withMemoryFileStore(t)
	scyllaTable := MakeScyllaTable[cacheGroupS3Record, cacheGroupS3Schema]()
	queryPlan := &db.IndexGroupReadPlan{PartitionValue: 7, IndexGroup: scyllaTable.IndexGroups[0]}
	snapshotKey := cache.CacheGroupSnapshotKey(scyllaTable.Name, 7, queryPlan.IndexGroup.IndexID, 55)
	database := &fakeGroupDatabase{rows: []cacheGroupS3Record{{CompanyID: 7, ID: 1, Date: 100, Status: 1}}}
	// The other request stores its file while this one reads the database.
	database.beforeReply = func() {
		otherContent := cache.EncodeRecordSnapshot(cache.RecordSnapshot{Cursor: 1000, ReadTime: 4999})
		if _, err := store.PutFileIfMatch(t.Context(), snapshotKey, otherContent, ""); err != nil {
			t.Fatalf("the other request's write: %v", err)
		}
	}
	records, err := readCacheGroupRecords(makeRecordSink(&[]cacheGroupS3Record{}), scyllaTable, queryPlan,
		db.IndexGroupFetchState{HashValue: 55, LastWrite: 1000}, database.read(scyllaTable), nil, 5000)
	if err != nil || len(records) != 1 {
		t.Fatalf("err %v, %d records", err, len(records))
	}
	if snapshot, _ := readStoredGroupSnapshot(t, store, snapshotKey); snapshot.ReadTime != 4999 {
		t.Fatalf("the raced write must be dropped, stored %+v", snapshot)
	}
}
