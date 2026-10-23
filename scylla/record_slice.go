package scylla

import (
	"reflect"
	"unsafe"
)

// recordSlice is a type-erased view over a *[]T.
//
// Why it exists: every engine function below the Executor boundary used its record type parameter
// for exactly one thing -- xunsafe.AsPointer(&(*records)[i]) -- because column reads and writes
// already go through the precompiled unsafe.Pointer accessors on ScyllaTable. The type parameter
// was doing no work, but Go still stencils the whole function once per record struct, so one engine
// was compiled 54 times. Passing this instead lets those bodies compile once.
//
// The generic surface does not change: the exported entry points stay generic and become thin
// shims that build a recordSlice and call the shared body, the same shape as colCore in
// genix-orm/db/column.go. See docs/BINARY_SIZE_PLAN.md in the parent repo.
type recordSlice struct {
	// firstElement is an unsafe.Pointer rather than a uintptr on purpose: the GC traces
	// unsafe.Pointer fields, so holding one keeps the backing array alive for as long as the
	// recordSlice does. A uintptr here would be a use-after-free waiting for a GC cycle.
	firstElement unsafe.Pointer
	length       int
	elementSize  uintptr
	// recordType is T, for the bodies that must build new slices of it (Merge splits the records
	// into inserts and updates).
	recordType reflect.Type
	// asRecordPointer converts an element address back into an interface holding *T. A handful of
	// places need the record's own method set rather than its columns -- SelfParse is declared on
	// the record type, so no column accessor can reach it.
	// The closure captures nothing, so it is a static func value: no allocation, one per table.
	asRecordPointer func(unsafe.Pointer) any
}

// makeRecordSlice is the only generic function in the write and read paths that survives per
// table, and it compiles to a few instructions.
func makeRecordSlice[T any](records *[]T) recordSlice {
	slice := recordSlice{
		// elementSize still has to be right for an empty slice: it can be sub-sliced and appended
		// to by the caller, and at() must stay correct if it is.
		elementSize:     unsafe.Sizeof(*new(T)),
		recordType:      reflect.TypeFor[T](),
		asRecordPointer: func(elementAddress unsafe.Pointer) any { return (*T)(elementAddress) },
	}
	if records != nil && len(*records) > 0 {
		slice.firstElement = unsafe.Pointer(unsafe.SliceData(*records))
		slice.length = len(*records)
	}
	return slice
}

// recordPointerAt returns element i as an interface holding *T, for the callers that need the
// record type's own methods. Use at() unless you specifically need the method set.
func (r recordSlice) recordPointerAt(index int) any { return r.asRecordPointer(r.at(index)) }

// at returns a pointer to element i, equivalent to xunsafe.AsPointer(&(*records)[i]).
func (r recordSlice) at(index int) unsafe.Pointer {
	return unsafe.Add(r.firstElement, uintptr(index)*r.elementSize)
}

func (r recordSlice) len() int { return r.length }

// sub returns the view of [start, end), matching how the write path chunks records into batches.
func (r recordSlice) sub(start, end int) recordSlice {
	subSlice := r
	if start >= end {
		subSlice.firstElement, subSlice.length = nil, 0
		return subSlice
	}
	subSlice.firstElement, subSlice.length = r.at(start), end-start
	return subSlice
}

// newRecordList starts an empty reflect-built []T that appendCopy fills, for the bodies that split
// or regroup records into slices of their own.
func (r recordSlice) newRecordList() reflect.Value {
	recordsPointer := reflect.New(reflect.SliceOf(r.recordType))
	recordsPointer.Elem().Set(reflect.MakeSlice(recordsPointer.Elem().Type(), 0, r.length))
	return recordsPointer
}

// appendCopy appends a copy of the record at recordPointer to the list newRecordList started.
func (r recordSlice) appendCopy(recordsPointer reflect.Value, recordPointer unsafe.Pointer) {
	records := recordsPointer.Elem()
	records.Set(reflect.Append(records, reflect.NewAt(r.recordType, recordPointer).Elem()))
}

// copyRecord overwrites the record at destination with the one at source.
func (r recordSlice) copyRecord(destination, source unsafe.Pointer) {
	reflect.NewAt(r.recordType, destination).Elem().Set(reflect.NewAt(r.recordType, source).Elem())
}

// pointers materializes one pointer per record, for the few callers that group or index records
// rather than walking them in order.
func (r recordSlice) pointers() []unsafe.Pointer {
	recordPointers := make([]unsafe.Pointer, r.length)
	for index := range recordPointers {
		recordPointers[index] = r.at(index)
	}
	return recordPointers
}

// concat views several slices as one sequence without copying any records. The write path prepares
// inserts and updates in separate slices and then needs to walk both, which is what this is for.
type recordSliceGroup []recordSlice

func (g recordSliceGroup) len() int {
	total := 0
	for _, slice := range g {
		total += slice.length
	}
	return total
}

// at indexes across the group as if it were one slice.
func (g recordSliceGroup) at(index int) unsafe.Pointer {
	for _, slice := range g {
		if index < slice.length {
			return slice.at(index)
		}
		index -= slice.length
	}
	panic("recordSliceGroup: index out of range")
}

// recordSink appends decoded rows to a *[]T without naming T. The read path needs this rather
// than recordSlice because it creates records instead of walking existing ones.
//
// Allocate-then-copy is deliberate, matching what the typed code did: writing straight into the
// destination slice would hand the caller's scan handler a pointer that a later append can
// invalidate by reallocating the backing array.
type recordSink struct {
	newRecord       func() unsafe.Pointer
	appendRecord    func(unsafe.Pointer)
	asRecordPointer func(unsafe.Pointer) any
	// heldRecords points at every record the destination already holds, in order; replaceRecords
	// sets the destination to copies of the given records. Deduplication rewrites the whole
	// destination, including what the caller put there before the read.
	heldRecords    func() []unsafe.Pointer
	replaceRecords func([]unsafe.Pointer)
}

func makeRecordSink[T any](destination *[]T) recordSink {
	return recordSink{
		newRecord:       func() unsafe.Pointer { return unsafe.Pointer(new(T)) },
		appendRecord:    func(record unsafe.Pointer) { *destination = append(*destination, *(*T)(record)) },
		asRecordPointer: func(record unsafe.Pointer) any { return (*T)(record) },
		heldRecords:     func() []unsafe.Pointer { return makeRecordSlice(destination).pointers() },
		replaceRecords: func(records []unsafe.Pointer) {
			// Copied into a new array: records may point into the one being replaced.
			replacement := make([]T, len(records))
			for index, record := range records {
				replacement[index] = *(*T)(record)
			}
			*destination = replacement
		},
	}
}

// makeReflectRecordSlice is makeRecordSlice for a *[]T held in a reflect.Value, for the paths that
// know the record type only at runtime (ScyllaController's admin and backup bodies). Those run
// once per table, not per request, so reflection costs nothing that matters there.
func makeReflectRecordSlice(recordsPointer reflect.Value) recordSlice {
	records := recordsPointer.Elem()
	recordType := records.Type().Elem()
	slice := recordSlice{
		elementSize:     recordType.Size(),
		recordType:      recordType,
		asRecordPointer: func(record unsafe.Pointer) any { return reflect.NewAt(recordType, record).Interface() },
	}
	if records.Len() > 0 {
		slice.firstElement = records.UnsafePointer()
		slice.length = records.Len()
	}
	return slice
}

// makeReflectRecordSink is makeRecordSink for a *[]T held in a reflect.Value.
func makeReflectRecordSink(recordsPointer reflect.Value) recordSink {
	records := recordsPointer.Elem()
	recordType := records.Type().Elem()
	return recordSink{
		newRecord: func() unsafe.Pointer { return reflect.New(recordType).UnsafePointer() },
		appendRecord: func(record unsafe.Pointer) {
			records.Set(reflect.Append(records, reflect.NewAt(recordType, record).Elem()))
		},
		asRecordPointer: func(record unsafe.Pointer) any { return reflect.NewAt(recordType, record).Interface() },
		heldRecords:     func() []unsafe.Pointer { return makeReflectRecordSlice(recordsPointer).pointers() },
		replaceRecords: func(heldRecords []unsafe.Pointer) {
			// Copied into a new array: heldRecords may point into the one being replaced.
			replacement := reflect.MakeSlice(records.Type(), len(heldRecords), len(heldRecords))
			for index, record := range heldRecords {
				replacement.Index(index).Set(reflect.NewAt(recordType, record).Elem())
			}
			records.Set(replacement)
		},
	}
}

// collectInto is a sink of the same record type as base that gathers the records by pointer
// instead of appending them to a typed slice. The read path uses it for the intermediate lists it
// merges or reorders before they reach the caller's destination.
func (base recordSink) collectInto(collected *[]unsafe.Pointer) recordSink {
	collector := base
	collector.appendRecord = func(record unsafe.Pointer) { *collected = append(*collected, record) }
	collector.heldRecords = func() []unsafe.Pointer { return *collected }
	collector.replaceRecords = func(records []unsafe.Pointer) { *collected = records }
	return collector
}

// erasedScanHandler adapts a caller's typed scan handler to the erased read path. Returns nil for
// a nil handler so the hot loop can keep testing it with a plain nil check.
func erasedScanHandler[T any](scanHandler func(record *T) bool) func(unsafe.Pointer) bool {
	if scanHandler == nil {
		return nil
	}
	return func(record unsafe.Pointer) bool { return scanHandler((*T)(record)) }
}
