package dynamo

import (
	"bytes"
	"github.com/ivanjoz/genix-orm/db/cache"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

// ── Delta test entity: no partition, a keyless delta and a fan-out delta ─────

type deltaMember struct {
	ID       int32   `cb:"1"`
	TeamIDs  []int16 `cb:"2"`
	Status   int8    `cb:"3"`
	Updated  int32   `cb:"4"`
	Version  int16   `json:"ver,omitempty" cb:"-"`
	Metadata *string `json:"mdt,omitempty" cb:"-"`
}

type deltaMemberTable struct {
	Model[deltaMemberTable, deltaMember]
	ID      Col[*deltaMemberTable, int32]
	TeamIDs ColSlice[*deltaMemberTable, int16]
	Status  Col[*deltaMemberTable, int8]
	Updated Col[*deltaMemberTable, int32]
}

func (t deltaMemberTable) GetSchema() Schema {
	return Schema{
		Entity:  "delta_member",
		TableID: 78901234,
		Keys:    Cols(t.ID.Size(30)),
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(t.Status)},
			{Type: TypeDelta, Keys: Cols(t.TeamIDs.Size(8), t.Status)},
		},
	}
}

// ── A composite-key entity: one index pinned off the Keys, one on their prefix ─

type deltaVisit struct {
	Date      int16   `cb:"1"`
	ID        int32   `cb:"2"`
	CreatedBy int32   `cb:"3"`
	Status    int8    `cb:"4"`
	Version   int16   `json:"ver,omitempty" cb:"-"`
	Metadata  *string `json:"mdt,omitempty" cb:"-"`
}

type deltaVisitTable struct {
	Model[deltaVisitTable, deltaVisit]
	Date      Col[*deltaVisitTable, int16]
	ID        Col[*deltaVisitTable, int32]
	CreatedBy Col[*deltaVisitTable, int32]
	Status    Col[*deltaVisitTable, int8]
}

func (t deltaVisitTable) GetSchema() Schema {
	return Schema{
		Entity:  "delta_visit",
		TableID: 78901260,
		Keys:    Cols(t.Date.Size(16), t.ID.Size(32)),
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(t.CreatedBy.Size(32), t.Status)},
			{Type: TypeDelta, Keys: Cols(t.Date.Size(16), t.Status)},
		},
	}
}

// withDeltaClock pins Now to a moment in bucket 5000 (SUnixTime 1_500_010) and installs an empty
// memory file store, both restored after the test.
func withDeltaClock(t *testing.T) *utils.MemoryFileStore {
	t.Helper()
	originalNow, originalStore := Now, cache.FileStore()
	t.Cleanup(func() { Now = originalNow; cache.SetFileStore(originalStore) })
	Now = func() time.Time { return cache.SUnixTimeToTime(1_500_010) }
	store := utils.NewMemoryFileStore()
	SetFileStore(store)
	return store
}

func readDeltaLogFile(t *testing.T, store *utils.MemoryFileStore, fileKey string, is64BitScale bool) []DeltaLogEntry {
	t.Helper()
	content, _, err := store.GetFile(fileKey)
	if err != nil {
		t.Fatalf("%s: %v", fileKey, err)
	}
	entries, err := cache.DecodeDeltaLogEntries(content, is64BitScale)
	if err != nil {
		t.Fatalf("%s: %v", fileKey, err)
	}
	return entries
}

func TestDeltaIndexesCompile(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]().meta
	keyless, byTeam := members.deltaIndexes[0], members.deltaIndexes[1]
	// Cols(Status): nothing pinned, the base table lists the view, the log ID is the ID.
	if keyless.tag != "003" || len(keyless.pinned) != 0 || keyless.syncFilterField != "Status" || keyless.rowsIndex != -1 ||
		len(keyless.logIDColumns) != 1 || keyless.logIDColumns[0].fieldName != "ID" || len(keyless.logIDColumnBits) != 0 {
		t.Fatalf("keyless delta = %+v", keyless)
	}
	// Cols(TeamIDs, Status): pinned per team, listed through hidden rows named by the slice's cb id.
	if byTeam.tag != "002-003" || len(byTeam.pinned) != 1 || byTeam.pinned[0].fieldName != "TeamIDs" || byTeam.rowsIndex < 0 ||
		members.arrayIndexes[byTeam.rowsIndex].columnID != "002" {
		t.Fatalf("team delta = %+v", byTeam)
	}

	visits := NewRepo[deltaVisitTable, deltaVisit]().meta
	byCreator, byDate := visits.deltaIndexes[0], visits.deltaIndexes[1]
	// CreatedBy is no Keys column: hidden rows, and both Keys pack into a 48-bit log ID.
	if byCreator.rowsIndex < 0 || len(byCreator.logIDColumns) != 2 || !slices.Equal(byCreator.logIDColumnBits, []int8{16, 32}) || !byCreator.is64BitScale {
		t.Fatalf("creator delta = %+v", byCreator)
	}
	// Date leads the Keys: the base table lists the view by sk prefix, and the log ID is the ID alone.
	if byDate.rowsIndex != -1 || len(byDate.logIDColumns) != 1 || byDate.logIDColumns[0].fieldName != "ID" || byDate.is64BitScale {
		t.Fatalf("date delta = %+v", byDate)
	}
}

// ── Schemas the boot refuses ─────────────────────────────────────────────────

type deltaStringKey struct {
	Code     string  `cb:"1"`
	Status   int8    `cb:"2"`
	Version  int16   `json:"ver,omitempty" cb:"-"`
	Metadata *string `json:"mdt,omitempty" cb:"-"`
}

type deltaStringKeyTable struct {
	Model[deltaStringKeyTable, deltaStringKey]
	Code   Col[*deltaStringKeyTable, string]
	Status Col[*deltaStringKeyTable, int8]
}

func (t deltaStringKeyTable) GetSchema() Schema {
	return Schema{Entity: "delta_string_key", TableID: 78901261, Keys: Cols(t.Code),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}}}
}

type deltaWideKey struct {
	Date     int16   `cb:"1"`
	SaleID   int32   `cb:"2"`
	Line     int16   `cb:"3"`
	Status   int8    `cb:"4"`
	Version  int16   `json:"ver,omitempty" cb:"-"`
	Metadata *string `json:"mdt,omitempty" cb:"-"`
}

type deltaWideKeyTable struct {
	Model[deltaWideKeyTable, deltaWideKey]
	Date   Col[*deltaWideKeyTable, int16]
	SaleID Col[*deltaWideKeyTable, int32]
	Line   Col[*deltaWideKeyTable, int16]
	Status Col[*deltaWideKeyTable, int8]
}

func (t deltaWideKeyTable) GetSchema() Schema {
	return Schema{Entity: "delta_wide_key", TableID: 78901262, Keys: Cols(t.Date.Size(16), t.SaleID.Size(32), t.Line.Size(8)),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}}}
}

type deltaWithoutMetadata struct {
	ID      int32 `cb:"1"`
	Status  int8  `cb:"2"`
	Version int16 `json:"ver,omitempty" cb:"-"`
}

type deltaWithoutMetadataTable struct {
	Model[deltaWithoutMetadataTable, deltaWithoutMetadata]
	ID     Col[*deltaWithoutMetadataTable, int32]
	Status Col[*deltaWithoutMetadataTable, int8]
}

func (t deltaWithoutMetadataTable) GetSchema() Schema {
	return Schema{Entity: "delta_without_metadata", TableID: 78901263, Keys: Cols(t.ID.Size(30)),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}}}
}

type withoutVersion struct {
	ID int32 `cb:"1"`
}

type withoutVersionTable struct {
	Model[withoutVersionTable, withoutVersion]
	ID Col[*withoutVersionTable, int32]
}

func (t withoutVersionTable) GetSchema() Schema {
	return Schema{Entity: "without_version", TableID: 78901264, Keys: Cols(t.ID.Size(30))}
}

type int64Updated struct {
	ID      int32 `cb:"1"`
	Updated int64 `cb:"2"`
	Version int16 `json:"ver,omitempty" cb:"-"`
}

type int64UpdatedTable struct {
	Model[int64UpdatedTable, int64Updated]
	ID      Col[*int64UpdatedTable, int32]
	Updated Col[*int64UpdatedTable, int64]
}

func (t int64UpdatedTable) GetSchema() Schema {
	return Schema{Entity: "int64_updated", TableID: 78901265, Keys: Cols(t.ID.Size(30))}
}

func TestSchemasTheBootRefuses(t *testing.T) {
	for name, testCase := range map[string]struct {
		build       func()
		wantMessage string
	}{
		"a string log ID":       {func() { NewRepo[deltaStringKeyTable, deltaStringKey]() }, "must be a signed integer"},
		"a log ID over 53 bits": {func() { NewRepo[deltaWideKeyTable, deltaWideKey]() }, "packs 56 bits"},
		"no Metadata field":     {func() { NewRepo[deltaWithoutMetadataTable, deltaWithoutMetadata]() }, "mdt"},
		"no Version field":      {func() { NewRepo[withoutVersionTable, withoutVersion]() }, "Version int16"},
		"an int64 Updated":      {func() { NewRepo[int64UpdatedTable, int64Updated]() }, "must be an int32"},
	} {
		func() {
			defer func() {
				recovered, _ := recover().(string)
				if !strings.Contains(recovered, testCase.wantMessage) {
					t.Fatalf("%s: want a panic with %q, got %q", name, testCase.wantMessage, recovered)
				}
			}()
			testCase.build()
		}()
	}
}

// ── Write side: the appends ──────────────────────────────────────────────────

func TestAppendDeltaLogsFollowsTheRecordAcrossFolders(t *testing.T) {
	store := withDeltaClock(t)
	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	created := deltaMember{ID: 7, TeamIDs: []int16{1, 2}, Status: 1}
	moved := deltaMember{ID: 7, TeamIDs: []int16{2, 3}, Status: 1}

	writes := [][]deltaLogWrite{
		{{writtenPtr: unsafe.Pointer(&created), version: 1}},                                    // new
		{{storedPtr: unsafe.Pointer(&created), writtenPtr: unsafe.Pointer(&moved), version: 2}}, // left team 1, joined 3
		{{storedPtr: unsafe.Pointer(&moved), version: 3}},                                       // hard delete
	}
	for _, write := range writes {
		if err := meta.appendDeltaLogs(write); err != nil {
			t.Fatal(err)
		}
	}

	folder := cache.DeltaLogFolder + meta.pkValue(unsafe.Pointer(&created)) + "/"
	expectedEntriesByFile := map[string][]DeltaLogEntry{
		"003/5000":       {{LogID: 7, Version: 1}, {LogID: 7, Version: 2}, {LogID: 7, Version: 3, IsDeleted: true}},
		"002-003/1/5000": {{LogID: 7, Version: 1}, {LogID: 7, Version: 2, IsDeleted: true}},
		"002-003/2/5000": {{LogID: 7, Version: 1}, {LogID: 7, Version: 2}, {LogID: 7, Version: 3, IsDeleted: true}},
		"002-003/3/5000": {{LogID: 7, Version: 2}, {LogID: 7, Version: 3, IsDeleted: true}},
	}
	for file, expectedEntries := range expectedEntriesByFile {
		if entries := readDeltaLogFile(t, store, folder+file, false); !slices.Equal(entries, expectedEntries) {
			t.Errorf("%s = %+v, want %+v", file, entries, expectedEntries)
		}
	}
	if fileKeys, _ := store.ListFiles(cache.DeltaLogFolder); len(fileKeys) != len(expectedEntriesByFile) {
		t.Errorf("files = %v, want only %d", fileKeys, len(expectedEntriesByFile))
	}
}

func TestAppendDeltaLogsKeepsTheLastWriteOfARecordInOneCall(t *testing.T) {
	store := withDeltaClock(t)
	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	first, second, other := deltaMember{ID: 7, Status: 1}, deltaMember{ID: 7, Status: 0}, deltaMember{ID: 8, Status: 1}
	err := meta.appendDeltaLogs([]deltaLogWrite{
		{writtenPtr: unsafe.Pointer(&first), version: 4},
		{writtenPtr: unsafe.Pointer(&other), version: 1},
		{writtenPtr: unsafe.Pointer(&second), version: 5}, // a soft delete is a deleted entry
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := readDeltaLogFile(t, store, cache.DeltaLogFolder+meta.pkValue(unsafe.Pointer(&first))+"/003/5000", false)
	if want := []DeltaLogEntry{{LogID: 7, Version: 5, IsDeleted: true}, {LogID: 8, Version: 1}}; !slices.Equal(entries, want) {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
}

func TestAppendDeltaLogsPacksACompositeLogID(t *testing.T) {
	store := withDeltaClock(t)
	meta := NewRepo[deltaVisitTable, deltaVisit]().meta
	visit := deltaVisit{Date: 20_000, ID: 41, CreatedBy: 9, Status: 1}
	if err := meta.appendDeltaLogs([]deltaLogWrite{{writtenPtr: unsafe.Pointer(&visit), version: 1}}); err != nil {
		t.Fatal(err)
	}
	folder := cache.DeltaLogFolder + meta.pkValue(unsafe.Pointer(&visit)) + "/"
	packedLogID := cache.PackDeltaLogID([]uint64{20_000, 41}, []int8{16, 32})
	if entries := readDeltaLogFile(t, store, folder+"003-004/9/5000", true); len(entries) != 1 || entries[0].LogID != packedLogID {
		t.Fatalf("creator log = %+v, want log ID %d", entries, packedLogID)
	}
	if entries := readDeltaLogFile(t, store, folder+"001-004/20000/5000", false); len(entries) != 1 || entries[0].LogID != 41 {
		t.Fatalf("date log = %+v, want log ID 41", entries)
	}
	if unpacked := cache.UnpackDeltaLogID(packedLogID, []int8{16, 32}); !slices.Equal(unpacked, []uint64{20_000, 41}) {
		t.Fatalf("unpacked = %v", unpacked)
	}
}

func TestAppendDeltaLogsNeedsAFileStore(t *testing.T) {
	withDeltaClock(t)
	cache.SetFileStore(nil)
	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	member := deltaMember{ID: 7, Status: 1}
	if err := meta.appendDeltaLogs([]deltaLogWrite{{writtenPtr: unsafe.Pointer(&member), version: 1}}); err == nil ||
		!strings.Contains(err.Error(), "SetFileStore") {
		t.Fatalf("err = %v, want the SetFileStore error", err)
	}
}

// ── Read side ────────────────────────────────────────────────────────────────

func TestDeltaIndexSelection(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	if query := members.Query().Delta(DeltaCursor{}, 1); query.planErr != nil || query.deltaIndex != &members.meta.deltaIndexes[0] {
		t.Fatalf("no Contains: index %+v, err %v; want the keyless one", query.deltaIndex, query.planErr)
	}
	if query := members.Query().Contains(members.T.TeamIDs, 3).Delta(DeltaCursor{}, 1); query.deltaIndex != &members.meta.deltaIndexes[1] {
		t.Fatalf("one-value Contains: index %+v; want the team one", query.deltaIndex)
	}
	if query := members.Query().Contains(members.T.TeamIDs, 3).Delta(DeltaCursor{1: 0}, 1); query.syncFilter != nil || !query.readsDeltaLog {
		t.Fatalf("a later sync reads the log with no sync filter: %+v", query)
	}

	products := newProducts(t)
	if err := products.Query().Eq(products.T.CategoryID, 7).Delta(DeltaCursor{}).planErr; err == nil || !strings.Contains(err.Error(), "no TypeDelta index") {
		t.Fatalf("a table without delta indexes: err = %v", err)
	}
	ambiguous := &tableMeta{recordType: products.meta.recordType, deltaIndexes: []deltaIndexMeta{{}, {}}}
	if _, err := ambiguous.selectDeltaIndex(nil, false); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("two equal fits: err = %v", err)
	}
}

func TestDeltaMetadataRecords(t *testing.T) {
	visits := NewRepo[deltaVisitTable, deltaVisit]()
	query := visits.Query().Eq(visits.T.CreatedBy, 9).Delta(DeltaCursor{5000: 3}, 1)
	content := []byte{1, 2, 3}
	metadataText := utils.EncodeSafe85(cache.DeltaMetadataBytes(cache.DeltaMetadataFlag64BitScale|cache.DeltaMetadataFlagSealed, 5000, []int8{16, 32}, content))
	record := (*deltaVisit)(query.deltaMetadataRecord(cache.DeltaMetadataFile{Bucket: 5000, Text: metadataText}))
	// −bucket goes in the log ID's last column; the rest of the key stays zero.
	if record.ID != -5000 || record.Date != 0 || record.Metadata == nil {
		t.Fatalf("record = %+v", record)
	}
	decoded, err := utils.DecodeSafe85(*record.Metadata)
	want := cache.DeltaMetadataBytes(cache.DeltaMetadataFlag64BitScale|cache.DeltaMetadataFlagSealed, 5000, []int8{16, 32}, content)
	if err != nil || !bytes.Equal(decoded, want) {
		t.Fatalf("metadata = %v, err %v; want %v", decoded, err, want)
	}
}

// ── Managed fields ───────────────────────────────────────────────────────────

func TestStampUpdatedWritesTheSUnixTime(t *testing.T) {
	withDeltaClock(t)
	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	first, second := deltaMember{}, deltaMember{Updated: 9}
	meta.stampUpdated([]unsafe.Pointer{unsafe.Pointer(&first), unsafe.Pointer(&second)})
	if first.Updated != 1_500_010 || second.Updated != 1_500_010 {
		t.Fatalf("Updated = %d, %d; want 1500010", first.Updated, second.Updated)
	}
	// A table without Updated stamps nothing.
	newProducts(t).meta.stampUpdated([]unsafe.Pointer{unsafe.Pointer(&Product{})})
}
