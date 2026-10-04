package dynamo

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrames (DATA_FRAMES_PLAN.md): group-by aggregates of a table, kept as files
// in a dataframe.Store (an S3 bucket) and brought up to date by MaterializeDataFrames.
// The files, their formats and the run's file work are the dataframe package; this
// side compiles the declarations, reads the records and holds each frame's state.
//
// Inserts reach the frames from DynamoDB: a run reads the records written since its
// snapshot through the whole-entity delta index. Updates and deletes also need the
// values the record held before, which only the write sees, so the write appends
// them to the frame's log, before the base write, when the frame's values change.
// CreatedVersion (the UpdatedVersion of the inserting write) tells a run whether its
// snapshot already holds a record. The run itself is in data_frame_run.go.
// ─────────────────────────────────────────────────────────────────────────────

const (
	createdVersionFieldName = "CreatedVersion"
	frameStateSKPrefix      = "f"
)

var dataFrameNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// ErrWriteDeadline is what a write to a table with DataFrames returns when it could not land within
// its deadline (SetDataFrames) of reading the stored records: nothing it had not sent yet was written.
// Retry it.
var ErrWriteDeadline = errors.New("db: the write passed its DataFrame write deadline: retry it")

const (
	// frameCancelGrace is how long after its deadline a write may still append the cancel markers of
	// the records it logged and did not land.
	frameCancelGrace = 3 * time.Second
	// frameSettleMargin covers a request already sent when its deadline hits, and the clock skew
	// between Lambdas.
	frameSettleMargin = 2 * time.Second
)

var (
	frameStore         dataframe.Store
	frameWriteDeadline = 10 * time.Second
	// frameSettle is how long after a checkpoint is read every write with a version up to it has landed
	// or given up, its cancel markers appended. Such a write reserved its version before the
	// checkpoint, so it lands by the deadline after it. A write that loses to it read the record before
	// it landed, so it is done one deadline plus the grace later.
	frameSettle = 2*frameWriteDeadline + frameCancelGrace + frameSettleMargin
)

// SetDataFrames sets the store of every table's DataFrame files and the write deadline (10 s by
// default): a write to a table with frames lands within it of reading the stored records, or fails
// with ErrWriteDeadline. Compactions reach a version twice the deadline plus 5 s after it was
// reserved (frameSettle). Call it once at boot, before the first write.
func SetDataFrames(store dataframe.Store, writeDeadline time.Duration) {
	frameStore, frameWriteDeadline = store, writeDeadline
	frameSettle = 2*frameWriteDeadline + frameCancelGrace + frameSettleMargin
}

// frameWriteWindow bounds a write to a table with frames (DATA_FRAMES_PLAN.md, D5). It starts at the
// stored read the write diffs against, or at the version reservation when it reads nothing: the log
// entries, the new hidden rows and the base items are sent by landBy, and the cancel markers of the
// records logged and not landed are appended within frameCancelGrace after. landBy is zero on a table
// without frames: no bound.
type frameWriteWindow struct{ landBy time.Time }

func (m *tableMeta) frameWriteWindowFrom(start time.Time) frameWriteWindow {
	if len(m.dataFrames) == 0 {
		return frameWriteWindow{}
	}
	return frameWriteWindow{landBy: start.Add(frameWriteDeadline)}
}

// landingContext bounds the calls that land the write.
func (window frameWriteWindow) landingContext() (context.Context, context.CancelFunc) {
	return window.contextUntil(window.landBy)
}

func (window frameWriteWindow) contextUntil(at time.Time) (context.Context, context.CancelFunc) {
	if window.landBy.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), at.Sub(Now()))
}

// frameLandingError marks the error of a write that ran out of its window with ErrWriteDeadline.
func frameLandingError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrWriteDeadline, err)
	}
	return err
}

// frameColumn is one integer column of a frame.
type frameColumn struct {
	fieldName string
	acc       *colAccessor
}

// dataFrameMeta is one compiled DataFrame: the dataframe.Frame its files are named and coded by,
// and the columns its values are read from.
type dataFrameMeta struct {
	dataframe.Frame
	keys               []frameColumn
	rows               frameColumn
	sums               []frameColumn
	allowsNegativeSums bool
	// countsRecords: the file's last Sums column is the record count (DataFrame.Count).
	countsRecords bool
	// firstKeyLeadsBaseKeys: a rebuild reads a range of Keys[0] from the base table, consistently;
	// otherwise it goes through a GSI, which cannot.
	firstKeyLeadsBaseKeys bool
}

// compileDataFrames resolves the schema's DataFrames. It runs after the indexes are compiled: a
// frame needs the whole-entity delta index the run reads inserts through.
func compileDataFrames(schema Schema, recordType reflect.Type, accessors map[string]*colAccessor, meta *tableMeta) []dataFrameMeta {
	if len(schema.DataFrames) == 0 {
		return nil
	}
	recordName := recordType.Name()
	if len(schema.Partition) > 0 {
		panic(fmt.Sprintf("db: %s declares DataFrames and a Partition: frames need an entity without one", recordName))
	}
	hasWholeEntityDelta := slices.ContainsFunc(meta.arrayIndexes, func(index arrayIndexMeta) bool {
		return index.isDelta && index.elementPosition < 0 && len(index.keys) == 1
	})
	if !hasWholeEntityDelta {
		panic(fmt.Sprintf("db: %s declares DataFrames and needs a TypeDelta index without pinned Keys, such as {Type: TypeDelta, Keys: Cols(table.Status)}: the run reads the changed records through it", recordName))
	}

	// Keys[0] is the rebuild's range: something must sort the records by it.
	leadingKeyFields := []string{schema.Keys[0].col().fieldName}
	for _, index := range schema.Indexes {
		if len(index.Keys) > 0 && (index.Slot.index != "" || index.Type == TypeLocal) {
			leadingKeyFields = append(leadingKeyFields, index.Keys[0].col().fieldName)
		}
	}

	frames := make([]dataFrameMeta, 0, len(schema.DataFrames))
	for _, declared := range schema.DataFrames {
		if !dataFrameNamePattern.MatchString(declared.Name) {
			panic(fmt.Sprintf("db: %s DataFrame name %q must be lowercase letters, digits and '-'", recordName, declared.Name))
		}
		if slices.ContainsFunc(frames, func(frame dataFrameMeta) bool { return frame.Name == declared.Name }) {
			panic(fmt.Sprintf("db: %s declares DataFrame %q twice", recordName, declared.Name))
		}
		if len(declared.Keys) < 1 || len(declared.Keys) > dataframe.MaxKeys || declared.Rows == nil || (len(declared.Sums) == 0 && !declared.Count) {
			panic(fmt.Sprintf("db: %s DataFrame %q needs 1 to %d Keys, a Rows column and at least one Sums column or Count", recordName, declared.Name, dataframe.MaxKeys))
		}

		isUsed := map[string]bool{}
		resolveColumn := func(column Coln) frameColumn {
			fieldName := column.col().fieldName
			accessor := accessors[fieldName]
			if _, isSlice := column.(SliceColn); isSlice || accessor == nil || !accessor.kind.isInteger() {
				panic(fmt.Sprintf("db: %s DataFrame %q column %q must be an integer Col", recordName, declared.Name, fieldName))
			}
			if isUsed[fieldName] {
				panic(fmt.Sprintf("db: %s DataFrame %q lists %q twice", recordName, declared.Name, fieldName))
			}
			isUsed[fieldName] = true
			return frameColumn{fieldName: fieldName, acc: accessor}
		}
		// The shape is built from cb ids (cbColumnID panics without one), so renaming a Go field keeps the files.
		columnIDs := func(columns []frameColumn) string {
			ids := make([]string, len(columns))
			for i, column := range columns {
				ids[i] = cbColumnID(recordType, column.fieldName)
			}
			return strings.Join(ids, ".")
		}

		frame := dataFrameMeta{allowsNegativeSums: declared.AllowNegativeSums, countsRecords: declared.Count}
		for _, column := range declared.Keys {
			frame.keys = append(frame.keys, resolveColumn(column))
		}
		frame.rows = resolveColumn(declared.Rows)
		for _, column := range declared.Sums {
			frame.sums = append(frame.sums, resolveColumn(column))
		}
		if !slices.Contains(leadingKeyFields, frame.keys[0].fieldName) {
			panic(fmt.Sprintf("db: %s DataFrame %q Keys[0] %q must lead the entity's Keys, a GSI or a local index: a rebuild reads a range of it",
				recordName, declared.Name, frame.keys[0].fieldName))
		}
		frame.firstKeyLeadsBaseKeys = leadingKeyFields[0] == frame.keys[0].fieldName
		folder := frameFolder(resolveTableID(schema), declared.Name)
		if collision := slices.IndexFunc(frames, func(other dataFrameMeta) bool { return other.Folder == folder }); collision >= 0 {
			panic(fmt.Sprintf("db: %s DataFrames %q and %q hash to the same folder: rename one", recordName, frames[collision].Name, declared.Name))
		}
		sumsCount, sumsIDs := len(frame.sums), columnIDs(frame.sums)
		if frame.countsRecords {
			sumsCount, sumsIDs = sumsCount+1, sumsIDs+"+count"
		}
		frame.Frame = dataframe.Frame{
			Name:      declared.Name,
			Folder:    folder,
			KeyCount:  len(frame.keys),
			SumsCount: sumsCount,
			Shape:     dataframe.ShapeOf(folder, columnIDs(frame.keys), columnIDs([]frameColumn{frame.rows}), sumsIDs),
		}
		frames = append(frames, frame)
	}
	return frames
}

// frameFolder names a frame's folder in the store by its entity's TableID and a 32-bit hash of its name,
// in the order-preserving base64 of the keys (5 and 6 characters): every object key stays short, and an
// entity renamed with its TableID pinned keeps its files as it keeps its items.
func frameFolder(tableID int32, frameName string) string {
	nameHasher := fnv.New32a()
	nameHasher.Write([]byte(frameName))
	return EncodeOrderedInt(int64(tableID), 5) + "/" + EncodeOrderedUint(uint64(nameHasher.Sum32()), 6) + "/"
}

// resolveCreatedVersion validates the managed CreatedVersion field a table with frames needs.
func resolveCreatedVersion(recordType reflect.Type, accessors map[string]*colAccessor) *colAccessor {
	field, ok := recordType.FieldByName(createdVersionFieldName)
	if !ok || field.Type.Kind() != reflect.Int32 {
		panic(fmt.Sprintf("db: %s declares DataFrames and needs an int32 field %q (json \"crv\")", recordType.Name(), createdVersionFieldName))
	}
	return accessors[createdVersionFieldName]
}

// frameValuesOf reads what the record at ptr contributes to the frame: nil for no record, or for a
// soft-deleted one (Status 0), which counts nowhere. A counting frame gets a 1 after the Sums.
func (m *tableMeta) frameValuesOf(frame *dataFrameMeta, ptr unsafe.Pointer) *dataframe.Values {
	if ptr == nil || (m.status != nil && m.status.getI64(ptr) == 0) {
		return nil
	}
	values := &dataframe.Values{Row: frame.rows.acc.getI64(ptr), Sums: make([]int64, frame.SumsCount)}
	for i, key := range frame.keys {
		values.Keys[i] = key.acc.getI64(ptr)
	}
	for i, sum := range frame.sums {
		values.Sums[i] = sum.acc.getI64(ptr)
	}
	if frame.countsRecords {
		values.Sums[len(frame.sums)] = 1
	}
	return values
}

// checkFrameValues fails a write holding a value its frames can't store: a negative Keys or Rows
// value (they name files and are delta-coded), or a negative Sums value on a frame without
// AllowNegativeSums.
func (m *tableMeta) checkFrameValues(ptrs []unsafe.Pointer) error {
	for _, ptr := range ptrs {
		for i := range m.dataFrames {
			frame := &m.dataFrames[i]
			checkedColumns := append(slices.Clip(frame.keys), frame.rows)
			if !frame.allowsNegativeSums {
				checkedColumns = append(checkedColumns, frame.sums...)
			}
			for _, column := range checkedColumns {
				if value := column.acc.getI64(ptr); value < 0 {
					return fmt.Errorf("db: %s %s is %d: DataFrame %q takes no negative value there", m.recordType.Name(), column.fieldName, value, frame.Name)
				}
			}
		}
	}
	return nil
}

// stampCreatedVersions sets the managed CreatedVersion of the records about to be written: the
// stored version's, or this write's UpdatedVersion for a record with none stored (an insert).
func (m *tableMeta) stampCreatedVersions(ptrs []unsafe.Pointer, storedByKey map[string]unsafe.Pointer) {
	if m.createdVersion == nil {
		return
	}
	for _, ptr := range ptrs {
		createdVersion := m.writeVersion.acc.getI64(ptr)
		if storedPtr := storedByKey[m.recordKey(ptr)]; storedPtr != nil {
			createdVersion = m.createdVersion.getI64(storedPtr)
		}
		m.createdVersion.setI64(ptr, createdVersion)
	}
}

// frameWrite is one record a write call replaces: its stored version, the version written over it
// (nil for a delete) and that write's UpdatedVersion.
type frameWrite struct {
	storedPtr, writtenPtr unsafe.Pointer
	newVersion            int64
}

// frameWritesOf pairs the records about to be written with their stored versions. A record with none
// stored is an insert, which the run reads from DynamoDB: it logs nothing, so it is left out.
func (m *tableMeta) frameWritesOf(ptrs []unsafe.Pointer, storedByKey map[string]unsafe.Pointer) []frameWrite {
	if len(m.dataFrames) == 0 {
		return nil
	}
	var writes []frameWrite
	for _, ptr := range ptrs {
		if storedPtr := storedByKey[m.recordKey(ptr)]; storedPtr != nil {
			writes = append(writes, frameWrite{storedPtr: storedPtr, writtenPtr: ptr, newVersion: m.writeVersion.acc.getI64(ptr)})
		}
	}
	return writes
}

// appendFrameLogEntries appends to each frame's log one entry per write that changes the frame's
// values: the stored values, keyed by the write's version. It runs before the base write, so a change
// a run sees in DynamoDB always has its entry. isCancel appends instead the markers that void the
// entries of writes that did not land (cancelFrameWrites). One append per frame, frames in parallel,
// bounded by ctx.
func (m *tableMeta) appendFrameLogEntries(ctx context.Context, writes []frameWrite, isCancel bool) error {
	entriesByFrame := make([][]dataframe.LogEntry, len(m.dataFrames))
	for _, write := range writes {
		sk := m.skValue(write.storedPtr)
		createdVersion := m.createdVersion.getI64(write.storedPtr)
		for i := range m.dataFrames {
			frame := &m.dataFrames[i]
			oldValues := m.frameValuesOf(frame, write.storedPtr)
			if dataframe.EqualValues(oldValues, m.frameValuesOf(frame, write.writtenPtr)) {
				continue
			}
			entriesByFrame[i] = append(entriesByFrame[i], dataframe.LogEntry{
				NewVersion: write.newVersion, CreatedVersion: createdVersion, SK: sk, IsCancel: isCancel, OldValues: oldValues,
			})
		}
	}
	var appendedFrames []int
	for i, entries := range entriesByFrame {
		if len(entries) > 0 {
			appendedFrames = append(appendedFrames, i)
		}
	}
	if len(appendedFrames) == 0 {
		return nil
	}
	if frameStore == nil {
		return fmt.Errorf("db: %s has DataFrames and no frame store is set: call SetDataFrames at boot", m.recordType.Name())
	}
	return parallel.Run(len(appendedFrames), func(position int) error {
		frameIndex := appendedFrames[position]
		return dataframe.AppendLog(ctx, frameStore, &m.dataFrames[frameIndex].Frame, entriesByFrame[frameIndex])
	})
}

// cancelFrameWrites appends the cancel markers of logged writes that did not land: they lost their
// condition, or were never sent. Left in the log, the entry of a write that never landed could read
// as the record's values after another write that did.
func (m *tableMeta) cancelFrameWrites(window frameWriteWindow, writes []frameWrite) error {
	ctx, cancel := window.contextUntil(window.landBy.Add(frameCancelGrace))
	defer cancel()
	return m.appendFrameLogEntries(ctx, writes, true)
}

// appendFrameDeleteEntries logs the values a record about to be deleted held in the frames it counts
// in, and returns the write it logged. A delete stamps no version, so it reserves one for the entries,
// above the version it read.
func (m *tableMeta) appendFrameDeleteEntries(ctx context.Context, storedPtr unsafe.Pointer) ([]frameWrite, error) {
	countsInAFrame := false
	for i := range m.dataFrames {
		countsInAFrame = countsInAFrame || m.frameValuesOf(&m.dataFrames[i], storedPtr) != nil
	}
	if !countsInAFrame {
		return nil, nil
	}
	deleteVersion, err := reserveSequence(m.pkValue(storedPtr)+updatedVersionSeqSuffix, 1)
	if err != nil {
		return nil, err
	}
	writes := []frameWrite{{storedPtr: storedPtr, newVersion: deleteVersion}}
	return writes, m.appendFrameLogEntries(ctx, writes, false)
}
