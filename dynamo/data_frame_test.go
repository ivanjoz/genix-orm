package dynamo

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
)

// ── DataFrame test entity: SaleLine's shape, with a 1-, a 2- and a 3-key frame ──

type frameLine struct {
	Fecha          int16  `cb:"1"`
	SaleID         int32  `cb:"2"`
	Line           int16  `cb:"3"`
	ProductID      int32  `cb:"4"`
	ClientID       int32  `cb:"5"`
	Quantity       int32  `cb:"6"`
	Amount         int32  `cb:"7"`
	Discount       int32  `cb:"8"`
	Note           string `cb:"9"`
	Status         int8   `cb:"10"`
	Updated        int32  `cb:"11"`
	UpdatedVersion int32  `cb:"12"`
	CreatedVersion int32  `cb:"13"`
}

type frameLineTable struct {
	Model[frameLineTable, frameLine]
	Fecha          Col[*frameLineTable, int16]
	SaleID         Col[*frameLineTable, int32]
	Line           Col[*frameLineTable, int16]
	ProductID      Col[*frameLineTable, int32]
	ClientID       Col[*frameLineTable, int32]
	Quantity       Col[*frameLineTable, int32]
	Amount         Col[*frameLineTable, int32]
	Discount       Col[*frameLineTable, int32]
	Note           Col[*frameLineTable, string]
	Status         Col[*frameLineTable, int8]
	Updated        Col[*frameLineTable, int32]
	UpdatedVersion Col[*frameLineTable, int32]
	CreatedVersion Col[*frameLineTable, int32]
}

func (t frameLineTable) GetSchema() Schema {
	return Schema{
		Entity:  "frame_line",
		TableID: 78901260,
		Keys:    Cols(t.Fecha.Size(16), t.SaleID.Size(32), t.Line.Size(8)),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}},
		DataFrames: []DataFrame{
			{Name: "day-product", Keys: Cols(t.Fecha), Rows: t.ProductID, Sums: Cols(t.Quantity)},
			{Name: "day-client-product", Keys: Cols(t.Fecha, t.ClientID), Rows: t.ProductID, Sums: Cols(t.Quantity, t.Amount)},
			// Discount is summed only here, so it alone may be negative.
			{Name: "day-client-sale", Keys: Cols(t.Fecha, t.ClientID, t.SaleID), Rows: t.ProductID, Sums: Cols(t.Amount, t.Discount), AllowNegativeSums: true},
		},
	}
}

var frameLines = NewRepo[frameLineTable, frameLine]()

func TestDataFrameDeclarationRules(t *testing.T) {
	for name, breakSchema := range map[string]func(t *frameLineTable, schema *Schema){
		"an uppercase name":     func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Name = "Day" },
		"a name declared twice": func(t *frameLineTable, schema *Schema) { schema.DataFrames[1].Name = "day-product" },
		"no Keys":               func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Keys = nil },
		"four Keys": func(t *frameLineTable, schema *Schema) {
			schema.DataFrames[2].Keys = Cols(t.Fecha, t.ClientID, t.SaleID, t.Line)
		},
		"no Rows":                      func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Rows = nil },
		"no Sums":                      func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Sums = nil },
		"a string column":              func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Sums = Cols(t.Note) },
		"a column listed twice":        func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Rows = t.Fecha },
		"a Keys[0] that leads nothing": func(t *frameLineTable, schema *Schema) { schema.DataFrames[0].Keys = Cols(t.ClientID) },
		"no whole-entity delta index": func(t *frameLineTable, schema *Schema) {
			schema.Indexes = []Index{{Slot: G1, Keys: Cols(t.ClientID.Size(32))}}
		},
		"a Partition": func(t *frameLineTable, schema *Schema) { schema.Partition = Cols(t.ClientID.Size(32)) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			tablePtr := new(frameLineTable)
			populateColumnNames(tablePtr)
			schema := tablePtr.GetSchema()
			schema.Entity, schema.TableID = "bad_frame_line", 78901261
			breakSchema(tablePtr, &schema)
			buildTableMeta(schema, reflect.TypeFor[frameLine]())
		}()
	}

	// Keys[0] may lead a GSI instead of the entity's Keys; the rebuild then reads it eventually consistent.
	tablePtr := new(frameLineTable)
	populateColumnNames(tablePtr)
	schema := tablePtr.GetSchema()
	schema.Entity, schema.TableID = "gsi_frame_line", 78901262
	schema.Indexes = append(schema.Indexes, Index{Slot: G1, Keys: Cols(tablePtr.ClientID.Size(32))})
	schema.DataFrames = []DataFrame{{Name: "client", Keys: Cols(tablePtr.ClientID), Rows: tablePtr.ProductID, Sums: Cols(tablePtr.Quantity)}}
	if meta := buildTableMeta(schema, reflect.TypeFor[frameLine]()); meta.dataFrames[0].firstKeyLeadsBaseKeys {
		t.Fatal("a frame keyed by a GSI column must not read its rebuild range from the base table")
	}
}

type uncreatedLine struct {
	Fecha          int16 `cb:"1"`
	ProductID      int32 `cb:"2"`
	Quantity       int32 `cb:"3"`
	Status         int8  `cb:"4"`
	UpdatedVersion int32 `cb:"5"`
}

type uncreatedLineTable struct {
	Model[uncreatedLineTable, uncreatedLine]
	Fecha          Col[*uncreatedLineTable, int16]
	ProductID      Col[*uncreatedLineTable, int32]
	Quantity       Col[*uncreatedLineTable, int32]
	Status         Col[*uncreatedLineTable, int8]
	UpdatedVersion Col[*uncreatedLineTable, int32]
}

func TestDataFrameNeedsCreatedVersion(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "CreatedVersion") {
			t.Fatalf("expected a panic naming CreatedVersion, got %v", recovered)
		}
	}()
	tablePtr := new(uncreatedLineTable)
	populateColumnNames(tablePtr)
	buildTableMeta(Schema{
		Entity: "uncreated_line", TableID: 78901263,
		Keys:       Cols(tablePtr.Fecha.Size(16), tablePtr.ProductID.Size(32)),
		Indexes:    []Index{{Type: TypeDelta, Keys: Cols(tablePtr.Status)}},
		DataFrames: []DataFrame{{Name: "day", Keys: Cols(tablePtr.Fecha), Rows: tablePtr.ProductID, Sums: Cols(tablePtr.Quantity)}},
	}, reflect.TypeFor[uncreatedLine]())
}

func TestCheckFrameValues(t *testing.T) {
	for name, testCase := range map[string]struct {
		line      frameLine
		wantsFail bool
	}{
		"every value >= 0":                         {frameLine{ProductID: 1, ClientID: 2, Quantity: 3, Amount: 4}, false},
		"a negative Sums value":                    {frameLine{ProductID: 1, Quantity: -3}, true},
		"a negative Rows value":                    {frameLine{ProductID: -1}, true},
		"a negative Keys value":                    {frameLine{ClientID: -2}, true},
		"a negative value AllowNegativeSums takes": {frameLine{ProductID: 1, Discount: -5}, false},
		// Amount is summed by day-client-sale, which allows negatives, and by day-client-product, which doesn't.
		"a negative value one frame refuses": {frameLine{ProductID: 1, Amount: -5}, true},
	} {
		err := frameLines.meta.checkFrameValues([]unsafe.Pointer{unsafe.Pointer(&testCase.line)})
		if (err != nil) != testCase.wantsFail {
			t.Errorf("%s: got error %v", name, err)
		}
	}
}

// TestFrameLogEntriesOfWrites checks which writes log which frames: only the frames whose values change.
func TestFrameLogEntriesOfWrites(t *testing.T) {
	store := dataframe.NewMemoryStore()
	SetDataFrames(store, time.Minute)
	defer SetDataFrames(nil, frameSettle)

	stored := &frameLine{Fecha: 20730, SaleID: 7, Line: 1, ProductID: 3, ClientID: 9, Quantity: 2000, Amount: 500, Status: 1, UpdatedVersion: 10, CreatedVersion: 4}
	logFrames := func(written *frameLine) []string {
		store.Delete(slices.Collect(func(yield func(string) bool) {
			keys, _ := store.List("")
			for _, key := range keys {
				yield(key)
			}
		})...)
		written.UpdatedVersion = 11
		write := frameWrite{storedPtr: unsafe.Pointer(stored), newVersion: 11}
		if written != nil {
			write.writtenPtr = unsafe.Pointer(written)
		}
		if err := frameLines.meta.appendFrameLogEntries([]frameWrite{write}, false); err != nil {
			t.Fatal(err)
		}
		var loggedFrames []string
		for i := range frameLines.meta.dataFrames {
			frame := &frameLines.meta.dataFrames[i]
			entries, err := dataframe.ReadLog(store, &frame.Frame)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) == 0 {
				continue
			}
			entry := entries[0]
			if entry.NewVersion != 11 || entry.CreatedVersion != 4 || entry.SK != frameLines.meta.skValue(unsafe.Pointer(stored)) {
				t.Fatalf("%s: entry %+v", frame.Name, entry)
			}
			if !dataframe.EqualValues(entry.OldValues, frameLines.meta.frameValuesOf(frame, unsafe.Pointer(stored))) {
				t.Fatalf("%s: old values %+v", frame.Name, entry.OldValues)
			}
			loggedFrames = append(loggedFrames, frame.Name)
		}
		return loggedFrames
	}
	edited := func(edit func(line *frameLine)) *frameLine {
		line := *stored
		edit(&line)
		return &line
	}
	for name, testCase := range map[string]struct {
		written    *frameLine
		wantFrames []string
	}{
		"a column no frame reads": {edited(func(line *frameLine) { line.Note = "gift" }), nil},
		"Quantity":                {edited(func(line *frameLine) { line.Quantity = 3000 }), []string{"day-product", "day-client-product"}},
		"ClientID":                {edited(func(line *frameLine) { line.ClientID = 8 }), []string{"day-client-product", "day-client-sale"}},
		"Discount":                {edited(func(line *frameLine) { line.Discount = -100 }), []string{"day-client-sale"}},
		"Status 1 → 0":            {edited(func(line *frameLine) { line.Status = 0 }), []string{"day-product", "day-client-product", "day-client-sale"}},
	} {
		if loggedFrames := logFrames(testCase.written); !slices.Equal(loggedFrames, testCase.wantFrames) {
			t.Errorf("%s: logged %v, want %v", name, loggedFrames, testCase.wantFrames)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The randomized run test: the spec of the runs. Writers race on a small table
// through the real write-path code (stampCreatedVersions, frameWritesOf,
// appendFrameLogEntries) in steps (read, reserve, log, land) interleaved with
// runs and rebuilds, which crash at random points. After every committed run each
// file must equal a brute-force aggregate of the records at the run's snapshot.
//
// A writer lands conditionally, as PutManyIfVersion does: one that read a version
// since replaced loses and appends its cancel markers. Time is counted in steps: a
// writer lives at most frameSimWriterLifetime steps, and a run targets a version
// read at least frameSimSettle steps before. frameSimSettle is twice the lifetime,
// which makes every losing writer finish before a run can read its entry; in
// production the settle is the Lambda timeout plus a minute, which leaves the
// narrow window described in the README's Limits. A writer crashes after logging
// only when the record is unchanged since its read and no other writer is on it:
// the crash phantoms the design tolerates.
// ─────────────────────────────────────────────────────────────────────────────

const (
	frameSimWriterLifetime = 6
	frameSimSettle         = 2*frameSimWriterLifetime + 1
)

var errSimulatedCrash = errors.New("simulated crash")

// crashingFrameStore fails the run's writes once its budget is spent, and lets the
// writers move on right before the run reads the log.
type crashingFrameStore struct {
	*dataframe.MemoryStore
	mutex              sync.Mutex
	remainingRunWrites int // -1: no crash
	filePuts           int
	beforeLogRead      func()
}

// isFrameDataFile tells a file of rows from a frame's _idx and _log.<shape>.
func isFrameDataFile(key string) bool {
	return !strings.HasSuffix(key, "/_idx") && !strings.Contains(key, "/_log.")
}

func (store *crashingFrameStore) spendRunWrite(isDataFile bool) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.remainingRunWrites == 0 {
		return errSimulatedCrash
	}
	if store.remainingRunWrites > 0 {
		store.remainingRunWrites--
	}
	if isDataFile {
		store.filePuts++
	}
	return nil
}

func (store *crashingFrameStore) Put(key string, content []byte) error {
	if err := store.spendRunWrite(isFrameDataFile(key)); err != nil {
		return err
	}
	return store.MemoryStore.Put(key, content)
}

func (store *crashingFrameStore) PutIfMatch(key string, content []byte, etag string) error {
	if err := store.spendRunWrite(isFrameDataFile(key)); err != nil {
		return err
	}
	return store.MemoryStore.PutIfMatch(key, content, etag)
}

func (store *crashingFrameStore) Delete(keys ...string) error {
	if err := store.spendRunWrite(false); err != nil {
		return err
	}
	return store.MemoryStore.Delete(keys...)
}

func (store *crashingFrameStore) Get(key string) ([]byte, string, error) {
	if strings.Contains(key, "/_log.") && store.beforeLogRead != nil {
		store.beforeLogRead()
	}
	return store.MemoryStore.Get(key)
}

type frameSimLanding struct {
	version int64
	record  *frameLine // nil: deleted
}

type frameSimWriter struct {
	sk              string
	startTime       int
	stored, written *frameLine // stored nil: an insert; written nil: a delete
	writes          []frameWrite
	version         int64
	step            int // 1: read, 2: reserved, 3: logged
	crashes         bool
}

type frameSimState struct {
	hasShape, isBuilt bool
	snapshot, target  int64
	targetTime        int
	// hasPendingCrash: the last run crashed, so files may be at target and the _idx behind them.
	hasPendingCrash bool
}

type frameSimulation struct {
	t        *testing.T
	rng      *rand.Rand
	now      int
	store    *crashingFrameStore
	records  map[string]*frameLine
	history  map[string][]frameSimLanding
	sequence int64
	writers  []*frameSimWriter
	states   []frameSimState
	counts   map[string]int
	trace    []string
}

func (sim *frameSimulation) tracef(format string, args ...any) {
	sim.trace = append(sim.trace, fmt.Sprintf("t=%d ", sim.now)+fmt.Sprintf(format, args...))
}

func TestDataFrameRunsMatchBruteForce(t *testing.T) {
	defer SetDataFrames(nil, frameSettle)
	counts := map[string]int{}
	for seed := range uint64(250) {
		store := &crashingFrameStore{MemoryStore: dataframe.NewMemoryStore(), remainingRunWrites: -1}
		SetDataFrames(store, time.Minute)
		sim := &frameSimulation{
			t: t, rng: rand.New(rand.NewPCG(seed, 77)), store: store,
			records: map[string]*frameLine{}, history: map[string][]frameSimLanding{},
			states: make([]frameSimState, len(frameLines.meta.dataFrames)), counts: counts,
		}
		store.beforeLogRead = sim.advanceSomeWriters
		for range 300 {
			sim.step()
		}
		// Drain the writers, let time pass and run until every frame holds the table as it is now.
		for len(sim.writers) > 0 {
			sim.advanceWriter(sim.writers[0])
		}
		for range 3 {
			sim.now += frameSimSettle
			for i := range sim.states {
				sim.runFrame(i, false)
			}
		}
		for i := range sim.states {
			if sim.states[i].snapshot != sim.sequence {
				t.Fatalf("seed %d: frame %d ended at snapshot %d, the sequence is at %d", seed, i, sim.states[i].snapshot, sim.sequence)
			}
		}
	}
	t.Logf("%v", counts)
	for _, event := range []string{"committed runs", "crashed runs", "lost writes", "crashed writers", "deletes", "range rebuilds", "full rebuilds", "fresh reads"} {
		if counts[event] == 0 {
			t.Errorf("the simulation never produced %s", event)
		}
	}
}

func (sim *frameSimulation) step() {
	sim.now++
	// A writer never outlives its Lambda: one at the end of its lifetime finishes now.
	for i := 0; i < len(sim.writers); {
		if writer := sim.writers[i]; sim.now-writer.startTime >= frameSimWriterLifetime {
			sim.advanceWriter(writer)
			continue
		}
		i++
	}
	switch roll := sim.rng.IntN(100); {
	case roll < 35:
		sim.startWriter()
	case roll < 75:
		sim.advanceSomeWriters()
	case roll < 80:
		sim.freshReadFrame(sim.rng.IntN(len(sim.states)))
	case roll < 94:
		for i := range sim.states {
			sim.runFrame(i, sim.rng.IntN(3) == 0)
		}
	case roll < 98:
		sim.rebuildFrame(sim.rng.IntN(len(sim.states)), true)
	default:
		sim.rebuildFrame(sim.rng.IntN(len(sim.states)), false)
	}
}

func (sim *frameSimulation) advanceSomeWriters() {
	for range sim.rng.IntN(3) {
		if len(sim.writers) > 0 {
			sim.advanceWriter(sim.writers[sim.rng.IntN(len(sim.writers))])
		}
	}
}

func cloneFrameLine(line *frameLine) *frameLine {
	if line == nil {
		return nil
	}
	clone := *line
	return &clone
}

// startWriter reads a record of a small key space and decides what to write over it.
func (sim *frameSimulation) startWriter() {
	rng := sim.rng
	key := &frameLine{Fecha: 20730 + int16(rng.IntN(3)), SaleID: 1 + int32(rng.IntN(3)), Line: 1 + int16(rng.IntN(2))}
	sk := frameLines.meta.skValue(unsafe.Pointer(key))
	writer := &frameSimWriter{sk: sk, startTime: sim.now, stored: cloneFrameLine(sim.records[sk]), step: 1, crashes: rng.IntN(10) == 0}
	randomizeValues := func(line *frameLine) {
		line.ProductID, line.ClientID = 1+int32(rng.IntN(5)), 1+int32(rng.IntN(3))
		line.Quantity, line.Amount, line.Discount = 1000*(1+int32(rng.IntN(5))), 100*int32(rng.IntN(50)), int32(rng.IntN(7))-3
	}
	if writer.stored == nil {
		writer.written = cloneFrameLine(key)
		randomizeValues(writer.written)
		writer.written.Status = int8(min(1, rng.IntN(8)))
	} else {
		writer.written = cloneFrameLine(writer.stored)
		switch rng.IntN(6) {
		case 0:
			randomizeValues(writer.written)
		case 1:
			writer.written.Quantity += 1000
		case 2:
			writer.written.ClientID = 1 + int32(rng.IntN(3))
		case 3:
			writer.written.Note += "x" // no frame reads it
		case 4:
			writer.written.Status = 1 - writer.written.Status
		default:
			writer.written = nil
		}
	}
	sim.writers = append(sim.writers, writer)
}

// advanceWriter takes the writer's next step: reserve its version, append its log
// entries, then land, lose (and cancel) or crash.
func (sim *frameSimulation) advanceWriter(writer *frameSimWriter) {
	meta := frameLines.meta
	switch writer.step {
	case 1:
		sim.sequence++
		writer.version = sim.sequence
		if writer.written == nil {
			writer.writes = []frameWrite{{storedPtr: unsafe.Pointer(writer.stored), newVersion: writer.version}}
		} else {
			writer.written.UpdatedVersion = int32(writer.version)
			writtenPtrs := []unsafe.Pointer{unsafe.Pointer(writer.written)}
			storedByKey := map[string]unsafe.Pointer{}
			if writer.stored != nil {
				storedByKey[meta.recordKey(unsafe.Pointer(writer.stored))] = unsafe.Pointer(writer.stored)
			}
			meta.stampCreatedVersions(writtenPtrs, storedByKey)
			writer.writes = meta.frameWritesOf(writtenPtrs, storedByKey)
		}
		sim.tracef("writer v%d on %q reserved: stored %+v written %+v", writer.version, writer.sk, writer.stored, writer.written)
	case 2:
		if err := meta.appendFrameLogEntries(writer.writes, false); err != nil {
			sim.t.Fatal(err)
		}
		sim.tracef("writer v%d logged", writer.version)
	case 3:
		current := sim.records[writer.sk]
		isUnchanged := (current == nil) == (writer.stored == nil) && (current == nil || current.UpdatedVersion == writer.stored.UpdatedVersion)
		isAlone := !slices.ContainsFunc(sim.writers, func(other *frameSimWriter) bool { return other != writer && other.sk == writer.sk })
		switch {
		case writer.crashes && isUnchanged && isAlone:
			sim.counts["crashed writers"]++
			sim.tracef("writer v%d crashed", writer.version)
		case !isUnchanged:
			if err := meta.appendFrameLogEntries(writer.writes, true); err != nil {
				sim.t.Fatal(err)
			}
			sim.counts["lost writes"]++
			sim.tracef("writer v%d lost", writer.version)
		default:
			sim.tracef("writer v%d landed", writer.version)
			if writer.written == nil {
				delete(sim.records, writer.sk)
				sim.counts["deletes"]++
			} else {
				sim.records[writer.sk] = writer.written
			}
			sim.history[writer.sk] = append(sim.history[writer.sk], frameSimLanding{version: writer.version, record: writer.written})
		}
		sim.writers = slices.DeleteFunc(sim.writers, func(other *frameSimWriter) bool { return other == writer })
		return
	}
	writer.step++
}

// readRecords is a record read of the run: what matches now, before any writer moves on.
func (sim *frameSimulation) readRecords(frame *dataFrameMeta, matches func(line *frameLine) bool) []dataframe.RecordState {
	var ptrs []unsafe.Pointer
	for _, record := range sim.records {
		if matches(record) {
			ptrs = append(ptrs, unsafe.Pointer(record))
		}
	}
	return frameLines.meta.frameRecordStates(frame, ptrs)
}

func (sim *frameSimulation) runFrame(frameIndex int, mayCrash bool) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := &sim.states[frameIndex]
	if !state.hasShape {
		// The reset run of a new frame: its first build targets the version read now.
		state.hasShape, state.target, state.targetTime = true, sim.sequence, sim.now
		return
	}
	if sim.now-state.targetTime < frameSimSettle {
		return
	}
	for _, writer := range sim.writers {
		if writer.version != 0 && writer.version <= state.target {
			sim.t.Fatalf("writer of version %d still running past the settle of target %d", writer.version, state.target)
		}
	}
	nextTarget := sim.sequence
	if mayCrash {
		sim.store.remainingRunWrites = sim.rng.IntN(6)
		defer func() { sim.store.remainingRunWrites = -1 }()
	}
	target := state.target
	var err error
	switch {
	case !state.isBuilt:
		err = dataframe.RebuildAllFiles(sim.store, &frame.Frame, sim.readRecords(frame, func(*frameLine) bool { return true }), target)
	case target > state.snapshot:
		err = dataframe.CompactFrame(sim.store, &frame.Frame, state.snapshot, target,
			func(snapshot int64) ([]dataframe.RecordState, error) {
				records := sim.readRecords(frame, func(line *frameLine) bool { return int64(line.UpdatedVersion) > snapshot })
				sim.advanceSomeWriters() // writes landing between the record read and the log read
				return records, nil
			},
			func(sks []string) ([]dataframe.RecordState, error) {
				return sim.readRecords(frame, func(line *frameLine) bool {
					return slices.Contains(sks, frameLines.meta.skValue(unsafe.Pointer(line)))
				}), nil
			})
	default:
		target = state.snapshot
	}
	if err != nil {
		if !errors.Is(err, errSimulatedCrash) {
			sim.t.Fatal(err)
		}
		sim.counts["crashed runs"]++
		sim.tracef("run %s %d → %d crashed", frame.Name, state.snapshot, target)
		// A first build that crashed leaves files at target too, but the frame is not built yet.
		state.hasPendingCrash = state.isBuilt
		return
	}
	sim.tracef("run %s %d → %d committed (built %v), next target %d", frame.Name, state.snapshot, target, state.isBuilt, nextTarget)
	state.isBuilt, state.snapshot, state.target, state.targetTime, state.hasPendingCrash = true, target, nextTarget, sim.now, false
	sim.counts["committed runs"]++
	if err := dataframe.TruncateLog(sim.store, &frame.Frame, target); err != nil && !errors.Is(err, errSimulatedCrash) {
		sim.t.Fatal(err)
	}
	sim.verifyFrame(frame, *state)
}

// rebuildFrame rebuilds a built frame at its snapshot, a range of days or all of it.
// Unless a crashed run left files ahead, they already hold that snapshot, so on a
// 2–3-key frame a range rebuild must not even rewrite one.
func (sim *frameSimulation) rebuildFrame(frameIndex int, isRange bool) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := sim.states[frameIndex]
	if !state.isBuilt {
		return
	}
	filePutsBefore := sim.store.filePuts
	if isRange {
		fromKey := 20730 + int64(sim.rng.IntN(3))
		toKey := fromKey + int64(sim.rng.IntN(int(20733-fromKey)))
		records := sim.readRecords(frame, func(line *frameLine) bool { return int64(line.Fecha) >= fromKey && int64(line.Fecha) <= toKey })
		if err := dataframe.RebuildFilesInRange(sim.store, &frame.Frame, records, state.snapshot, fromKey, toKey); err != nil {
			sim.t.Fatal(err)
		}
		if len(frame.keys) > 1 && !state.hasPendingCrash && sim.store.filePuts != filePutsBefore {
			sim.t.Fatalf("%s: a range rebuild of files already right rewrote %d of them", frame.Name, sim.store.filePuts-filePutsBefore)
		}
		sim.counts["range rebuilds"]++
		sim.tracef("range rebuild %s at %d of [%d, %d]", frame.Name, state.snapshot, fromKey, toKey)
	} else {
		if err := dataframe.RebuildAllFiles(sim.store, &frame.Frame, sim.readRecords(frame, func(*frameLine) bool { return true }), state.snapshot); err != nil {
			sim.t.Fatal(err)
		}
		sim.counts["full rebuilds"]++
		sim.tracef("full rebuild %s at %d", frame.Name, state.snapshot)
	}
	sim.verifyFrame(frame, state)
}

// freshReadFrame checks a fresh read of a built frame, over a random range of days and
// maybe a pinned ClientID, against a brute-force aggregate of the records as they are
// now, while writers are reserved, logged, about to lose or crashed.
func (sim *frameSimulation) freshReadFrame(frameIndex int) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := sim.states[frameIndex]
	if !state.isBuilt {
		return
	}
	fromKey := 20730 + int64(sim.rng.IntN(3))
	toKey := fromKey + int64(sim.rng.IntN(int(20733-fromKey)))
	var pinnedKeys []int64
	if frame.KeyCount > 1 && sim.rng.IntN(2) == 0 {
		pinnedKeys = []int64{1 + int64(sim.rng.IntN(3))}
	}
	isSelected := func(keys [dataframe.MaxKeys]int64) bool {
		return keys[0] >= fromKey && keys[0] <= toKey && slices.Equal(keys[1:1+len(pinnedKeys)], pinnedKeys)
	}
	// No writer moves during the read, so "now" is one state of the records.
	beforeLogRead := sim.store.beforeLogRead
	sim.store.beforeLogRead = nil
	fileKeys, files, err := dataframe.ReadFreshFiles(sim.store, &frame.Frame, state.snapshot, fromKey, toKey, pinnedKeys,
		func(snapshot int64) ([]dataframe.RecordState, error) {
			return sim.readRecords(frame, func(line *frameLine) bool { return int64(line.UpdatedVersion) > snapshot }), nil
		},
		func(sks []string) ([]dataframe.RecordState, error) {
			return sim.readRecords(frame, func(line *frameLine) bool {
				return slices.Contains(sks, frameLines.meta.skValue(unsafe.Pointer(line)))
			}), nil
		})
	sim.store.beforeLogRead = beforeLogRead
	if err != nil {
		sim.t.Fatal(err)
	}
	failf := func(format string, args ...any) {
		sim.t.Helper()
		sim.t.Fatalf("%s\n%s fresh read of [%d, %d] %v at snapshot %d: %s", strings.Join(sim.trace, "\n"), frame.Name, fromKey, toKey, pinnedKeys,
			state.snapshot, fmt.Sprintf(format, args...))
	}
	if !slices.IsSortedFunc(fileKeys, func(a, b [dataframe.MaxKeys]int64) int { return slices.Compare(a[:], b[:]) }) {
		failf("file keys out of order: %v", fileKeys)
	}
	expected := sim.bruteForceFiles(frame, math.MaxInt64, isSelected)
	for i, keys := range fileKeys {
		objectKey := frame.FileKey(keys)
		if !isSelected(keys) {
			failf("read %s, outside the selection", objectKey)
		}
		want := expected[objectKey]
		delete(expected, objectKey)
		if !slices.Equal(files[i].RowIDs, want.RowIDs) || (len(want.RowIDs) > 0 && !reflect.DeepEqual(files[i].Sums, want.Sums)) {
			failf("%s holds rows %v sums %v, the records add up to rows %v sums %v", objectKey, files[i].RowIDs, files[i].Sums, want.RowIDs, want.Sums)
		}
	}
	for objectKey := range expected {
		failf("%s is missing", objectKey)
	}
	sim.counts["fresh reads"]++
	sim.tracef("fresh read %s of [%d, %d] %v", frame.Name, fromKey, toKey, pinnedKeys)
}

// bruteForceFiles aggregates the records as they were at snapshot into the frame's
// files whose keys isSelected, by object key, from the history of what landed. It
// shares no code with the runs: rows whose sums are all 0 are left out, as the runs
// leave them out.
func (sim *frameSimulation) bruteForceFiles(frame *dataFrameMeta, snapshot int64, isSelected func(keys [dataframe.MaxKeys]int64) bool) map[string]dataframe.File {
	sumsByRowByObjectKey := map[string]map[int64][]int64{}
	for _, landings := range sim.history {
		var recordPtr unsafe.Pointer
		for _, landing := range landings {
			if landing.version <= snapshot {
				recordPtr = nil
				if landing.record != nil {
					recordPtr = unsafe.Pointer(landing.record)
				}
			}
		}
		values := frameLines.meta.frameValuesOf(frame, recordPtr)
		if values == nil || !isSelected(values.Keys) {
			continue
		}
		objectKey := frame.FileKey(values.Keys)
		if sumsByRowByObjectKey[objectKey] == nil {
			sumsByRowByObjectKey[objectKey] = map[int64][]int64{}
		}
		sums := sumsByRowByObjectKey[objectKey][values.Row]
		if sums == nil {
			sums = make([]int64, frame.SumsCount)
			sumsByRowByObjectKey[objectKey][values.Row] = sums
		}
		for i, value := range values.Sums {
			sums[i] += value
		}
	}
	filesByObjectKey := map[string]dataframe.File{}
	for objectKey, sumsByRow := range sumsByRowByObjectKey {
		file := dataframe.File{Sums: make([][]int64, frame.SumsCount)}
		for _, rowID := range slices.Sorted(maps.Keys(sumsByRow)) {
			if !slices.ContainsFunc(sumsByRow[rowID], func(sum int64) bool { return sum != 0 }) {
				continue
			}
			file.RowIDs = append(file.RowIDs, rowID)
			for i, sum := range sumsByRow[rowID] {
				file.Sums[i] = append(file.Sums[i], sum)
			}
		}
		if len(file.RowIDs) > 0 {
			filesByObjectKey[objectKey] = file
		}
	}
	return filesByObjectKey
}

// verifyFrame checks every file of the frame against a brute-force aggregate of the
// records at the frame's snapshot, or, for a file a crashed run left ahead, at that
// run's target. With no crashed run pending, every _idx must list exactly the
// hashes of its folder's files.
func (sim *frameSimulation) verifyFrame(frame *dataFrameMeta, state frameSimState) {
	t := sim.t
	t.Helper()
	failf := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s\n%s at snapshot %d: %s", strings.Join(sim.trace, "\n"), frame.Name, state.snapshot, fmt.Sprintf(format, args...))
	}
	isAnyFile := func([dataframe.MaxKeys]int64) bool { return true }
	expectedAtSnapshot := sim.bruteForceFiles(frame, state.snapshot, isAnyFile)
	var expectedAtTarget map[string]dataframe.File

	listedKeys, _ := sim.store.MemoryStore.List(frame.Folder)
	fileHashesByFolder := map[string]map[string]uint32{}
	var indexKeys []string
	for _, objectKey := range listedKeys {
		switch {
		case strings.Contains(objectKey, "/_log."):
			continue
		case strings.HasSuffix(objectKey, "/_idx"):
			indexKeys = append(indexKeys, objectKey)
			continue
		}
		content, _, _ := sim.store.MemoryStore.Get(objectKey)
		file, err := dataframe.DecodeFile(content, frame.SumsCount)
		if err != nil {
			failf("%s: %v", objectKey, err)
		}
		expected := expectedAtSnapshot[objectKey]
		delete(expectedAtSnapshot, objectKey)
		if file.Snapshot > state.snapshot {
			if !state.hasPendingCrash || file.Snapshot != state.target {
				failf("%s is at snapshot %d, and no crashed run targets it", objectKey, file.Snapshot)
			}
			if expectedAtTarget == nil {
				expectedAtTarget = sim.bruteForceFiles(frame, state.target, isAnyFile)
			}
			expected = expectedAtTarget[objectKey]
		}
		if !slices.Equal(file.RowIDs, expected.RowIDs) || (len(file.RowIDs) > 0 && !reflect.DeepEqual(file.Sums, expected.Sums)) {
			failf("%s (at %d) holds rows %v sums %v, the records add up to rows %v sums %v", objectKey, file.Snapshot, file.RowIDs, file.Sums, expected.RowIDs, expected.Sums)
		}
		folder := path.Dir(objectKey) + "/"
		if fileHashesByFolder[folder] == nil {
			fileHashesByFolder[folder] = map[string]uint32{}
		}
		fileHashesByFolder[folder][objectKey] = dataframe.FileHash(content)
	}
	for objectKey := range expectedAtSnapshot {
		failf("%s is missing", objectKey)
	}
	if len(frame.keys) == 1 || state.hasPendingCrash {
		return
	}
	for _, indexKey := range indexKeys {
		folder := strings.TrimSuffix(indexKey, "_idx")
		day, _ := strconv.ParseInt(path.Base(folder), 10, 64)
		content, _, _ := sim.store.MemoryStore.Get(indexKey)
		entries, err := dataframe.DecodeIndex(content, frame.KeyCount)
		if err != nil {
			failf("%s: %v", indexKey, err)
		}
		indexedHashes := map[string]uint32{}
		for _, entry := range entries {
			indexedHashes[frame.FileKey([dataframe.MaxKeys]int64{day, entry.Keys[0], entry.Keys[1]})] = entry.Hash
		}
		if !reflect.DeepEqual(indexedHashes, fileHashesByFolder[folder]) {
			failf("%s lists %v, the folder holds %v", indexKey, indexedHashes, fileHashesByFolder[folder])
		}
		delete(fileHashesByFolder, folder)
	}
	for folder := range fileHashesByFolder {
		failf("folder %s has files and no _idx", folder)
	}
}
