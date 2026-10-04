package dynamo

import (
	"context"
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
			{Name: "day-client-product", Keys: Cols(t.Fecha, t.ClientID), Rows: t.ProductID, Sums: Cols(t.Quantity, t.Amount), Count: true},
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
		"two names hashing to one folder": func(t *frameLineTable, schema *Schema) {
			schema.DataFrames[0].Name, schema.DataFrames[1].Name = "frame-1522789", "frame-1739192"
		},
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
	meta := buildTableMeta(schema, reflect.TypeFor[frameLine]())
	if meta.dataFrames[0].firstKeyLeadsBaseKeys {
		t.Fatal("a frame keyed by a GSI column must not read its rebuild range from the base table")
	}
	// The folder is stored data: a change of it moves every frame to a new folder, rebuilt from scratch.
	if folder := meta.dataFrames[0].Folder; folder != "3gz-D/1EZdkT/" {
		t.Fatalf("frame folder %q: the TableID and name hash encode differently", folder)
	}
}

// TestFrameCountColumn checks Count: one more Sums column after the declared ones, 1 per record and
// nothing for a soft-deleted one, in the shape, and enough alone for a frame.
func TestFrameCountColumn(t *testing.T) {
	dayClientProduct := &frameLines.meta.dataFrames[1]
	line := frameLine{Fecha: 20730, ClientID: 9, ProductID: 3, Quantity: 2000, Amount: 500, Status: 1}
	if values := frameLines.meta.frameValuesOf(dayClientProduct, unsafe.Pointer(&line)); dayClientProduct.SumsCount != 3 || !slices.Equal(values.Sums, []int64{2000, 500, 1}) {
		t.Fatalf("day-client-product: %d sums, values %+v", dayClientProduct.SumsCount, values)
	}
	line.Status = 0
	if values := frameLines.meta.frameValuesOf(dayClientProduct, unsafe.Pointer(&line)); values != nil {
		t.Fatalf("a soft-deleted record counts: %+v", values)
	}

	tablePtr := new(frameLineTable)
	populateColumnNames(tablePtr)
	schema := tablePtr.GetSchema()
	schema.Entity, schema.TableID = "count_frame_line", 78901264
	schema.DataFrames = []DataFrame{
		{Name: "day-client", Keys: Cols(tablePtr.Fecha), Rows: tablePtr.ClientID, Count: true},
		{Name: "day-client-uncounted", Keys: Cols(tablePtr.Fecha), Rows: tablePtr.ClientID, Sums: Cols(tablePtr.Amount)},
		{Name: "day-client-counted", Keys: Cols(tablePtr.Fecha), Rows: tablePtr.ClientID, Sums: Cols(tablePtr.Amount), Count: true},
	}
	meta := buildTableMeta(schema, reflect.TypeFor[frameLine]())
	line.Status = 1
	if values := meta.frameValuesOf(&meta.dataFrames[0], unsafe.Pointer(&line)); !slices.Equal(values.Sums, []int64{1}) {
		t.Fatalf("a count-only frame: values %+v", values)
	}
	// Declaring Count changes the files: a shape change, rebuilt from scratch.
	counted := &meta.dataFrames[2]
	if counted.Shape != dataframe.ShapeOf(counted.Folder, "001", "005", "007+count") || counted.Shape == dataframe.ShapeOf(counted.Folder, "001", "005", "007") {
		t.Fatal("Count must change the shape, through the Sums")
	}
	if meta.dataFrames[1].Shape != dataframe.ShapeOf(meta.dataFrames[1].Folder, "001", "005", "007") {
		t.Fatal("a frame without Count must keep its shape")
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
	SetDataFrames(store, frameWriteDeadline)
	defer SetDataFrames(nil, frameWriteDeadline)

	stored := &frameLine{Fecha: 20730, SaleID: 7, Line: 1, ProductID: 3, ClientID: 9, Quantity: 2000, Amount: 500, Status: 1, UpdatedVersion: 10, CreatedVersion: 4}
	logFrames := func(written *frameLine) []string {
		store.Delete(context.Background(), slices.Collect(func(yield func(string) bool) {
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
		if err := frameLines.meta.appendFrameLogEntries(context.Background(), []frameWrite{write}, false); err != nil {
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

// TestFrameWriteWindow checks the write deadline: past it a write's log append fails with
// ErrWriteDeadline, its cancel markers still go in within the grace after it, and not later.
func TestFrameWriteWindow(t *testing.T) {
	store := dataframe.NewMemoryStore()
	SetDataFrames(store, frameWriteDeadline)
	defer SetDataFrames(nil, frameWriteDeadline)
	frame := &frameLines.meta.dataFrames[0]

	stored := &frameLine{Fecha: 20730, SaleID: 7, Line: 1, ProductID: 3, Quantity: 2000, Status: 1, UpdatedVersion: 10, CreatedVersion: 4}
	written := *stored
	written.Quantity, written.UpdatedVersion = 3000, 11
	writes := []frameWrite{{storedPtr: unsafe.Pointer(stored), writtenPtr: unsafe.Pointer(&written), newVersion: 11}}

	expiredWindow := frameLines.meta.frameWriteWindowFrom(Now().Add(-frameWriteDeadline))
	ctx, cancel := expiredWindow.landingContext()
	defer cancel()
	if err := frameLandingError(frameLines.meta.appendFrameLogEntries(ctx, writes, false)); !errors.Is(err, ErrWriteDeadline) {
		t.Fatalf("a log append past the deadline returned %v", err)
	}
	if err := frameLines.meta.cancelFrameWrites(expiredWindow, writes); err != nil {
		t.Fatalf("cancel markers within the grace: %v", err)
	}
	if entries, _ := dataframe.ReadLog(store, &frame.Frame); len(entries) != 1 || !entries[0].IsCancel || entries[0].NewVersion != 11 {
		t.Fatalf("the log holds %+v, want one cancel marker of version 11", entries)
	}
	pastGraceWindow := frameLines.meta.frameWriteWindowFrom(Now().Add(-frameWriteDeadline - frameCancelGrace))
	if err := frameLines.meta.cancelFrameWrites(pastGraceWindow, writes); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel markers past the grace returned %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The randomized compaction test: the spec of the runs and of the express
// compactions. Writers race on a small table through the real write-path code
// (stampCreatedVersions, frameWritesOf, appendFrameLogEntries) in steps (read,
// reserve, log, land), interleaved with runs, fresh reads, express compactions and
// rebuilds, which crash at random points. After every commit and rebuild each file
// must equal a brute-force aggregate of the records at its own snapshot (the
// frame's, when older), and every fresh read the records as they are.
//
// A writer lands conditionally, as PutManyIfVersion does: one that read a version
// since replaced loses and appends its cancel markers. Time is counted in steps: a
// writer lives at most frameSimWriterLifetime steps from its read, as the write
// deadline bounds a real one, and a compaction targets only a checkpoint at least
// frameSimSettle steps old, twice the lifetime plus one, as frameSettle is. A writer
// crashes after logging only when the record is unchanged since its read and no
// other writer is on it: the crash phantoms the design tolerates.
//
// Every compaction takes the frame's lock (dataframe.TakeLock) on a clock of
// frameSimStepDuration per step, and skips while another holds it. One that crashes
// never releases it: the next takes it over once it expired. Some express
// compactions are slow: they wait steps between their read and their files, and
// hold the lock steps between their files and their commit, maybe past its expiry,
// when another compaction may have taken it over.
// ─────────────────────────────────────────────────────────────────────────────

const (
	frameSimWriterLifetime = 6
	frameSimSettle         = 2*frameSimWriterLifetime + 1
	frameSimStepDuration   = 2 * time.Second
)

var errSimulatedCrash = errors.New("simulated crash")

// crashingFrameStore fails the compactions' writes once its budget is spent, and lets the writers move
// on right before a run reads the log. The lock's writes spend none: a crash is a holder that stops
// writing and never releases its lock.
type crashingFrameStore struct {
	*dataframe.MemoryStore
	mutex              sync.Mutex
	remainingRunWrites int // -1: no crash
	filePuts           int
	beforeLogRead      func()
}

// isFrameDataFile tells a file of rows from a frame's _idx, _ixt, _log.<shape> and _lock.
func isFrameDataFile(key string) bool {
	return !strings.HasSuffix(key, "/_idx") && !strings.HasSuffix(key, "/_ixt") && !strings.HasSuffix(key, "/_lock") &&
		!strings.Contains(key, "/_log.")
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

func (store *crashingFrameStore) Put(ctx context.Context, key string, content []byte) error {
	if err := store.spendRunWrite(isFrameDataFile(key)); err != nil {
		return err
	}
	return store.MemoryStore.Put(ctx, key, content)
}

func (store *crashingFrameStore) PutIfMatch(ctx context.Context, key string, content []byte, etag string) (string, error) {
	if !strings.HasSuffix(key, "/_lock") {
		if err := store.spendRunWrite(isFrameDataFile(key)); err != nil {
			return "", err
		}
	}
	return store.MemoryStore.PutIfMatch(ctx, key, content, etag)
}

func (store *crashingFrameStore) Delete(ctx context.Context, keys ...string) error {
	if err := store.spendRunWrite(false); err != nil {
		return err
	}
	return store.MemoryStore.Delete(ctx, keys...)
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

// frameSimState is a frame's state item, as the simulation keeps it.
type frameSimState struct {
	hasShape, isBuilt bool
	snapshot          int64
	hasTarget         bool
	target            int64
	targetTime        int
	hasPreviousTarget bool
	previousTarget    int64
	extendedDays      map[int64]bool
	// hasPendingCrash: a compaction crashed since the last run committed, so files may be ahead of
	// the snapshot with the indexes behind them.
	hasPendingCrash bool
}

// pushCheckpoint and settledTarget are pushFrameCheckpoint and frameState.settledTarget, in steps.
func (state *frameSimState) pushCheckpoint(sequence int64, now int) {
	if state.hasTarget && now-state.targetTime < frameSimSettle {
		return
	}
	state.hasPreviousTarget, state.previousTarget = state.hasTarget, state.target
	state.hasTarget, state.target, state.targetTime = true, sequence, now
}

func (state *frameSimState) settledTarget(now int) (int64, bool) {
	if state.hasTarget && now-state.targetTime >= frameSimSettle {
		return state.target, true
	}
	return state.previousTarget, state.hasPreviousTarget
}

// frameSimExpress is an express compaction between its steps: read; then lock taken and files written;
// then committed.
type frameSimExpress struct {
	frameIndex   int
	read         *dataframe.FreshRead
	baseSnapshot int64
	target       int64
	lock         *dataframe.Lock // set once its files are written
	lockedAt     int
	extendedDays []int64
}

type frameSimulation struct {
	t         *testing.T
	rng       *rand.Rand
	now       int
	store     *crashingFrameStore
	records   map[string]*frameLine
	history   map[string][]frameSimLanding
	sequence  int64
	writers   []*frameSimWriter
	expresses []*frameSimExpress // the slow ones in flight
	states    []frameSimState
	// unreleasedLocks is each frame's last lock taken, until its holder releases it.
	unreleasedLocks []*dataframe.Lock
	counts          map[string]int
	trace           []string
}

func (sim *frameSimulation) tracef(format string, args ...any) {
	sim.trace = append(sim.trace, fmt.Sprintf("t=%d ", sim.now)+fmt.Sprintf(format, args...))
}

// clock is the time of the locks: frameSimStepDuration per step.
func (sim *frameSimulation) clock() time.Time {
	return time.Unix(1_000_000_000, 0).Add(time.Duration(sim.now) * frameSimStepDuration)
}

// takeLock is dataframe.TakeLock on the simulation's clock: nil, counted as skippedEvent, while another
// compaction holds the lock.
func (sim *frameSimulation) takeLock(frameIndex int, skippedEvent string) *dataframe.Lock {
	lock, err := dataframe.TakeLock(sim.store, &frameLines.meta.dataFrames[frameIndex].Frame, sim.clock)
	if err != nil {
		sim.t.Fatal(err)
	}
	if lock == nil {
		sim.counts[skippedEvent]++
		return nil
	}
	if sim.unreleasedLocks[frameIndex] != nil {
		sim.counts["locks taken over"]++
		sim.tracef("lock of frame %d taken over", frameIndex)
	}
	sim.unreleasedLocks[frameIndex] = lock
	return lock
}

func (sim *frameSimulation) releaseLock(frameIndex int, lock *dataframe.Lock) {
	if err := lock.Release(); err != nil {
		sim.t.Fatal(err)
	}
	if sim.unreleasedLocks[frameIndex] == lock {
		sim.unreleasedLocks[frameIndex] = nil
	}
}

func TestDataFrameRunsMatchBruteForce(t *testing.T) {
	defer SetDataFrames(nil, frameWriteDeadline)
	counts := map[string]int{}
	for seed := range uint64(250) {
		store := &crashingFrameStore{MemoryStore: dataframe.NewMemoryStore(), remainingRunWrites: -1}
		SetDataFrames(store, frameWriteDeadline)
		sim := &frameSimulation{
			t: t, rng: rand.New(rand.NewPCG(seed, 77)), store: store,
			records: map[string]*frameLine{}, history: map[string][]frameSimLanding{},
			states:          make([]frameSimState, len(frameLines.meta.dataFrames)),
			unreleasedLocks: make([]*dataframe.Lock, len(frameLines.meta.dataFrames)), counts: counts,
		}
		for i := range sim.states {
			sim.states[i].extendedDays = map[int64]bool{}
		}
		store.beforeLogRead = sim.advanceSomeWriters
		for range 300 {
			sim.step()
		}
		// Drain the writers and the express compactions, let time pass (the locks of crashed compactions
		// expire) and run until every frame holds the table as it is now.
		for len(sim.writers) > 0 {
			sim.advanceWriter(sim.writers[0])
		}
		for len(sim.expresses) > 0 {
			sim.advanceExpress(sim.expresses[0])
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
	for _, event := range []string{"committed runs", "crashed runs", "lost writes", "crashed writers", "deletes", "range rebuilds", "full rebuilds",
		"fresh reads", "express commits", "crashed expresses", "slow expresses", "expresses behind w", "runs skipped for the lock",
		"rebuilds skipped for the lock", "expresses skipped for the lock", "locks taken over", "expresses that lost their lock",
		"expresses committed past their lock's expiry"} {
		if counts[event] == 0 {
			t.Errorf("the simulation never produced %s", event)
		}
	}
}

func (sim *frameSimulation) step() {
	sim.now++
	// A writer never outlives its deadline: one at the end of its lifetime finishes now.
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
	case roll < 70:
		sim.advanceSomeWriters()
	case roll < 80:
		sim.freshReadFrame(sim.rng.IntN(len(sim.states)))
	case roll < 85:
		if len(sim.expresses) > 0 {
			sim.advanceExpress(sim.expresses[sim.rng.IntN(len(sim.expresses))])
		}
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
		if err := meta.appendFrameLogEntries(context.Background(), writer.writes, false); err != nil {
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
			if err := meta.appendFrameLogEntries(context.Background(), writer.writes, true); err != nil {
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

// recordReaders are the record reads of a compaction: the records written after a snapshot, and by sk.
func (sim *frameSimulation) recordReaders(frame *dataFrameMeta) (func(snapshot int64) ([]dataframe.RecordState, error), func(sks []string) ([]dataframe.RecordState, error)) {
	readWrittenAfter := func(snapshot int64) ([]dataframe.RecordState, error) {
		return sim.readRecords(frame, func(line *frameLine) bool { return int64(line.UpdatedVersion) > snapshot }), nil
	}
	readBySK := func(sks []string) ([]dataframe.RecordState, error) {
		return sim.readRecords(frame, func(line *frameLine) bool {
			return slices.Contains(sks, frameLines.meta.skValue(unsafe.Pointer(line)))
		}), nil
	}
	return readWrittenAfter, readBySK
}

// checkSettled fails when a writer of a version up to target still runs: target would not be settled.
func (sim *frameSimulation) checkSettled(target int64) {
	for _, writer := range sim.writers {
		if writer.version != 0 && writer.version <= target {
			sim.t.Fatalf("writer of version %d still running past the settle of target %d", writer.version, target)
		}
	}
}

// runFrame is the scheduled run: it takes the lock, pushes a checkpoint when the newest has settled,
// then builds the frame or compacts it to the newest settled checkpoint, merging the days in ix. One
// that crashes keeps the lock.
func (sim *frameSimulation) runFrame(frameIndex int, mayCrash bool) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := &sim.states[frameIndex]
	lock := sim.takeLock(frameIndex, "runs skipped for the lock")
	if lock == nil {
		return
	}
	if !state.hasShape {
		// The reset run of a new frame records a checkpoint; its first build targets it once settled.
		state.hasShape, state.hasTarget, state.target, state.targetTime = true, true, sim.sequence, sim.now
		sim.releaseLock(frameIndex, lock)
		return
	}
	state.pushCheckpoint(sim.sequence, sim.now)
	target, hasTarget := state.settledTarget(sim.now)
	if !state.isBuilt && !hasTarget {
		sim.releaseLock(frameIndex, lock)
		return
	}
	sim.checkSettled(target)
	if mayCrash {
		sim.store.remainingRunWrites = sim.rng.IntN(6)
		defer func() { sim.store.remainingRunWrites = -1 }()
	}
	snapshot := state.snapshot
	mergedDays := slices.Sorted(maps.Keys(state.extendedDays))
	var err error
	if !state.isBuilt {
		snapshot = target
		err = dataframe.RebuildAllFiles(lock, &frame.Frame, sim.readRecords(frame, func(*frameLine) bool { return true }), target)
	} else {
		if hasTarget && target > snapshot {
			snapshot = target
		}
		readWrittenAfter, readBySK := sim.recordReaders(frame)
		err = dataframe.CompactFrame(lock, &frame.Frame, state.snapshot, snapshot, mergedDays,
			func(snapshot int64) ([]dataframe.RecordState, error) {
				records, err := readWrittenAfter(snapshot)
				sim.advanceSomeWriters() // writes landing between the record read and the log read
				return records, err
			}, readBySK)
	}
	if err != nil {
		if !errors.Is(err, errSimulatedCrash) {
			sim.t.Fatal(err)
		}
		sim.counts["crashed runs"]++
		sim.tracef("run %s %d → %d crashed", frame.Name, state.snapshot, snapshot)
		// A first build that crashed leaves files too, but the frame is not built yet.
		state.hasPendingCrash = state.hasPendingCrash || state.isBuilt
		return
	}
	if !sim.commitUnder(lock) {
		sim.t.Fatalf("run %s lost its lock within a step", frame.Name)
	}
	sim.tracef("run %s %d → %d committed (built %v), merged days %v", frame.Name, state.snapshot, snapshot, state.isBuilt, mergedDays)
	for _, day := range mergedDays {
		delete(state.extendedDays, day)
	}
	state.isBuilt, state.snapshot, state.hasPendingCrash = true, snapshot, false
	sim.counts["committed runs"]++
	if err := dataframe.TruncateLog(sim.store, &frame.Frame, snapshot); err != nil && !errors.Is(err, errSimulatedCrash) {
		sim.t.Fatal(err)
	}
	sim.releaseLock(frameIndex, lock)
	sim.verifyFrame(frame, *state)
}

// commitUnder is the bound commitFrameState puts on its write: false once the lock was taken over.
func (sim *frameSimulation) commitUnder(lock *dataframe.Lock) bool {
	_, cancel, err := lock.WriteContext()
	if errors.Is(err, dataframe.ErrLockLost) {
		return false
	}
	if err != nil {
		sim.t.Fatal(err)
	}
	cancel()
	return true
}

// rebuildFrame rebuilds a built frame at its snapshot, a range of days or all of it, under the lock.
// Unless a crashed or an uncommitted compaction left files ahead, they already hold that snapshot, so
// on a 2–3-key frame a range rebuild must not even rewrite one.
func (sim *frameSimulation) rebuildFrame(frameIndex int, isRange bool) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := &sim.states[frameIndex]
	if !state.isBuilt {
		return
	}
	lock := sim.takeLock(frameIndex, "rebuilds skipped for the lock")
	if lock == nil {
		return
	}
	defer sim.releaseLock(frameIndex, lock)
	isQuiet := !state.hasPendingCrash && !sim.hasFilesAhead(frame, state.snapshot)
	filePutsBefore := sim.store.filePuts
	if isRange {
		fromKey := 20730 + int64(sim.rng.IntN(3))
		toKey := fromKey + int64(sim.rng.IntN(int(20733-fromKey)))
		records := sim.readRecords(frame, func(line *frameLine) bool { return int64(line.Fecha) >= fromKey && int64(line.Fecha) <= toKey })
		if err := dataframe.RebuildFilesInRange(lock, &frame.Frame, records, state.snapshot, fromKey, toKey); err != nil {
			sim.t.Fatal(err)
		}
		if len(frame.keys) > 1 && isQuiet && sim.store.filePuts != filePutsBefore {
			sim.t.Fatalf("%s\n%s: a range rebuild of files already right rewrote %d of them", strings.Join(sim.trace, "\n"), frame.Name, sim.store.filePuts-filePutsBefore)
		}
		sim.counts["range rebuilds"]++
		sim.tracef("range rebuild %s at %d of [%d, %d]", frame.Name, state.snapshot, fromKey, toKey)
	} else {
		if err := dataframe.RebuildAllFiles(lock, &frame.Frame, sim.readRecords(frame, func(*frameLine) bool { return true }), state.snapshot); err != nil {
			sim.t.Fatal(err)
		}
		state.hasPendingCrash = false
		sim.counts["full rebuilds"]++
		sim.tracef("full rebuild %s at %d", frame.Name, state.snapshot)
	}
	sim.verifyFrame(frame, *state)
}

// freshReadFrame checks a fresh read of a built frame, over a random range of days and maybe a pinned
// ClientID, against a brute-force aggregate of the records as they are now, while writers are
// reserved, logged, about to lose or crashed. Then, as expressCompactFrame, it pushes a checkpoint,
// and may start an express compaction to the newest checkpoint settled when the read began: half of
// them finish now, the slow ones over later steps.
func (sim *frameSimulation) freshReadFrame(frameIndex int) {
	frame := &frameLines.meta.dataFrames[frameIndex]
	state := &sim.states[frameIndex]
	if !state.isBuilt {
		return
	}
	startState := *state
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
	readWrittenAfter, readBySK := sim.recordReaders(frame)
	fresh, err := dataframe.ReadFreshFiles(sim.store, &frame.Frame, state.snapshot, fromKey, toKey, pinnedKeys, readWrittenAfter, readBySK)
	sim.store.beforeLogRead = beforeLogRead
	if err != nil {
		sim.t.Fatal(err)
	}
	failf := func(format string, args ...any) {
		sim.t.Helper()
		sim.t.Fatalf("%s\n%s fresh read of [%d, %d] %v at snapshot %d: %s", strings.Join(sim.trace, "\n"), frame.Name, fromKey, toKey, pinnedKeys,
			state.snapshot, fmt.Sprintf(format, args...))
	}
	if !slices.IsSortedFunc(fresh.FileKeys, func(a, b [dataframe.MaxKeys]int64) int { return slices.Compare(a[:], b[:]) }) {
		failf("file keys out of order: %v", fresh.FileKeys)
	}
	expected := sim.bruteForceFiles(frame, math.MaxInt64, isSelected)
	for i, keys := range fresh.FileKeys {
		objectKey := frame.FileKey(keys)
		if !isSelected(keys) {
			failf("read %s, outside the selection", objectKey)
		}
		want := expected[objectKey]
		delete(expected, objectKey)
		if !slices.Equal(fresh.Files[i].RowIDs, want.RowIDs) || (len(want.RowIDs) > 0 && !reflect.DeepEqual(fresh.Files[i].Sums, want.Sums)) {
			failf("%s holds rows %v sums %v, the records add up to rows %v sums %v", objectKey, fresh.Files[i].RowIDs, fresh.Files[i].Sums, want.RowIDs, want.Sums)
		}
	}
	for objectKey := range expected {
		failf("%s is missing", objectKey)
	}
	sim.counts["fresh reads"]++
	sim.tracef("fresh read %s of [%d, %d] %v", frame.Name, fromKey, toKey, pinnedKeys)

	state.pushCheckpoint(sim.sequence, sim.now)
	target, hasTarget := startState.settledTarget(sim.now)
	if !hasTarget || target <= startState.snapshot || fresh.ChangedRecords(target) == 0 || sim.rng.IntN(2) == 0 {
		return
	}
	sim.checkSettled(target)
	express := &frameSimExpress{frameIndex: frameIndex, read: fresh, baseSnapshot: startState.snapshot, target: target}
	sim.tracef("express %s %d → %d read", frame.Name, express.baseSnapshot, target)
	if sim.rng.IntN(2) == 0 {
		sim.counts["slow expresses"]++
		sim.expresses = append(sim.expresses, express)
		return
	}
	sim.advanceExpress(express)
}

// advanceExpress takes the express compaction's next step. First it takes the lock, unless another
// compaction holds it, and goes on only while w is still its read's: then it writes its files and
// appends to the _ixt of their days, maybe crashing partway. Then it commits w and ix, unless its lock
// was taken over meanwhile, and releases the lock. A fast one takes both steps at once.
func (sim *frameSimulation) advanceExpress(express *frameSimExpress) {
	frame := &frameLines.meta.dataFrames[express.frameIndex]
	state := &sim.states[express.frameIndex]
	drop := func() {
		sim.expresses = slices.DeleteFunc(sim.expresses, func(other *frameSimExpress) bool { return other == express })
	}
	if express.lock == nil {
		lock := sim.takeLock(express.frameIndex, "expresses skipped for the lock")
		if lock == nil {
			drop()
			return
		}
		if !state.isBuilt || state.snapshot != express.baseSnapshot {
			sim.counts["expresses behind w"]++
			sim.releaseLock(express.frameIndex, lock)
			drop()
			return
		}
		if sim.rng.IntN(8) == 0 {
			sim.store.remainingRunWrites = sim.rng.IntN(4)
		}
		extendedDays, err := express.read.CompactTo(lock, &frame.Frame, express.target)
		sim.store.remainingRunWrites = -1
		if err != nil {
			if !errors.Is(err, errSimulatedCrash) {
				sim.t.Fatal(err)
			}
			sim.counts["crashed expresses"]++
			sim.tracef("express %s %d → %d crashed", frame.Name, express.baseSnapshot, express.target)
			state.hasPendingCrash = true
			drop()
			return
		}
		express.lock, express.lockedAt, express.extendedDays = lock, sim.now, extendedDays
		sim.tracef("express %s %d → %d wrote its files, days %v", frame.Name, express.baseSnapshot, express.target, extendedDays)
		if slices.Contains(sim.expresses, express) {
			return
		}
	}
	drop()
	defer sim.releaseLock(express.frameIndex, express.lock)
	if !sim.commitUnder(express.lock) {
		// Its files are ahead of w, as a crashed compaction's, and indexed.
		sim.counts["expresses that lost their lock"]++
		sim.tracef("express %s %d → %d lost its lock", frame.Name, express.baseSnapshot, express.target)
		return
	}
	if state.snapshot != express.baseSnapshot {
		sim.t.Fatalf("%s\nexpress %s %d → %d: w moved to %d under its lock", strings.Join(sim.trace, "\n"), frame.Name,
			express.baseSnapshot, express.target, state.snapshot)
	}
	if time.Duration(sim.now-express.lockedAt)*frameSimStepDuration > dataframe.LockDuration {
		sim.counts["expresses committed past their lock's expiry"]++
	}
	state.snapshot = express.target
	for _, day := range express.extendedDays {
		state.extendedDays[day] = true
	}
	sim.counts["express commits"]++
	sim.tracef("express %s %d → %d committed", frame.Name, express.baseSnapshot, express.target)
	sim.verifyFrame(frame, *state)
}

// bruteForceFiles aggregates the records as they were at snapshot into the frame's
// files whose keys isSelected, by object key, from the history of what landed. It
// shares no code with the compactions: rows whose sums are all 0 are left out, as
// the compactions leave them out.
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

// hasFilesAhead reports whether a file of the frame is past snapshot: a compaction that crashed or did
// not commit (yet) wrote it.
func (sim *frameSimulation) hasFilesAhead(frame *dataFrameMeta, snapshot int64) bool {
	objectKeys, _ := sim.store.MemoryStore.List(frame.Folder)
	for _, objectKey := range objectKeys {
		if !isFrameDataFile(objectKey) {
			continue
		}
		content, _, _ := sim.store.MemoryStore.Get(objectKey)
		if file, err := dataframe.DecodeFile(content, frame.SumsCount); err == nil && file.Snapshot > snapshot {
			return true
		}
	}
	return false
}

// indexedHashes reads every day index of the frame as object key → hash: the _idx, then the _ixt over
// it, the later entries winning.
func (sim *frameSimulation) indexedHashes(frame *dataFrameMeta) map[string]uint32 {
	objectKeys, _ := sim.store.MemoryStore.List(frame.Folder)
	hashes := map[string]uint32{}
	for _, indexSuffix := range []string{"/_idx", "/_ixt"} {
		for _, objectKey := range objectKeys {
			if !strings.HasSuffix(objectKey, indexSuffix) {
				continue
			}
			day, _ := strconv.ParseInt(path.Base(path.Dir(objectKey)), 10, 64)
			content, _, _ := sim.store.MemoryStore.Get(objectKey)
			decode := dataframe.DecodeIndex
			if indexSuffix == "/_ixt" {
				decode = dataframe.DecodeIndexExtension
			}
			entries, err := decode(content, frame.KeyCount)
			if err != nil {
				sim.t.Fatalf("%s: %v", objectKey, err)
			}
			for _, entry := range entries {
				hashes[frame.FileKey([dataframe.MaxKeys]int64{day, entry.Keys[0], entry.Keys[1]})] = entry.Hash
			}
		}
	}
	return hashes
}

// verifyFrame checks every file of the frame against a brute-force aggregate of the records at its own
// snapshot, or at the frame's when older: a file ahead of the frame is one a compaction that crashed or
// did not commit (yet) wrote. Every file with rows at the frame's snapshot must exist and, on a 2–3-key
// frame, be listed by its day's index. With no crash pending, the indexes must list exactly the files,
// with their hashes, files ahead included.
func (sim *frameSimulation) verifyFrame(frame *dataFrameMeta, state frameSimState) {
	t := sim.t
	t.Helper()
	failf := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s\n%s at snapshot %d: %s", strings.Join(sim.trace, "\n"), frame.Name, state.snapshot, fmt.Sprintf(format, args...))
	}
	isAnyFile := func([dataframe.MaxKeys]int64) bool { return true }
	expectedBySnapshot := map[int64]map[string]dataframe.File{}
	expectedAt := func(snapshot int64) map[string]dataframe.File {
		if expectedBySnapshot[snapshot] == nil {
			expectedBySnapshot[snapshot] = sim.bruteForceFiles(frame, snapshot, isAnyFile)
		}
		return expectedBySnapshot[snapshot]
	}

	objectKeys, _ := sim.store.MemoryStore.List(frame.Folder)
	fileHashes := map[string]uint32{}
	for _, objectKey := range objectKeys {
		if !isFrameDataFile(objectKey) {
			continue
		}
		content, _, _ := sim.store.MemoryStore.Get(objectKey)
		file, err := dataframe.DecodeFile(content, frame.SumsCount)
		if err != nil {
			failf("%s: %v", objectKey, err)
		}
		expected := expectedAt(max(file.Snapshot, state.snapshot))[objectKey]
		if !slices.Equal(file.RowIDs, expected.RowIDs) || (len(file.RowIDs) > 0 && !reflect.DeepEqual(file.Sums, expected.Sums)) {
			failf("%s (at %d) holds rows %v sums %v, the records add up to rows %v sums %v", objectKey, file.Snapshot, file.RowIDs, file.Sums, expected.RowIDs, expected.Sums)
		}
		fileHashes[objectKey] = dataframe.FileHash(content)
	}
	for objectKey := range expectedAt(state.snapshot) {
		if _, isPresent := fileHashes[objectKey]; !isPresent {
			failf("%s is missing", objectKey)
		}
	}
	if frame.KeyCount == 1 {
		return
	}
	indexedHashes := sim.indexedHashes(frame)
	for objectKey := range expectedAt(state.snapshot) {
		if _, isListed := indexedHashes[objectKey]; !isListed {
			failf("%s has rows and no index lists it", objectKey)
		}
	}
	if !state.hasPendingCrash && !maps.Equal(indexedHashes, fileHashes) {
		failf("the indexes list %v, the folders hold %v", indexedHashes, fileHashes)
	}
}
