package scylla

import (
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
	// asRecordPointer converts an element address back into an interface holding *T. A handful of
	// places need the record's own method set rather than its columns -- SelfParse and
	// GetTextSearchIndex are declared on the record type, so no column accessor can reach them.
	// The closure captures nothing, so it is a static func value: no allocation, one per table.
	asRecordPointer func(unsafe.Pointer) any
}

// makeRecordSlice is the only generic function in the write and read paths that survives per
// table, and it compiles to a few instructions.
func makeRecordSlice[T any](records *[]T) recordSlice {
	asRecordPointer := func(elementAddress unsafe.Pointer) any { return (*T)(elementAddress) }
	if records == nil || len(*records) == 0 {
		// elementSize still has to be right: an empty slice can be sub-sliced and appended to by
		// the caller, and at() must stay correct if it is.
		return recordSlice{elementSize: unsafe.Sizeof(*new(T)), asRecordPointer: asRecordPointer}
	}
	return recordSlice{
		firstElement:    unsafe.Pointer(unsafe.SliceData(*records)),
		length:          len(*records),
		elementSize:     unsafe.Sizeof(*new(T)),
		asRecordPointer: asRecordPointer,
	}
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
	if start >= end {
		return recordSlice{elementSize: r.elementSize, asRecordPointer: r.asRecordPointer}
	}
	return recordSlice{
		firstElement:    r.at(start),
		length:          end - start,
		elementSize:     r.elementSize,
		asRecordPointer: r.asRecordPointer,
	}
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
}

func makeRecordSink[T any](destination *[]T) recordSink {
	return recordSink{
		newRecord:       func() unsafe.Pointer { return unsafe.Pointer(new(T)) },
		appendRecord:    func(record unsafe.Pointer) { *destination = append(*destination, *(*T)(record)) },
		asRecordPointer: func(record unsafe.Pointer) any { return (*T)(record) },
	}
}

// erasedScanHandler adapts a caller's typed scan handler to the erased read path. Returns nil for
// a nil handler so the hot loop can keep testing it with a plain nil check.
func erasedScanHandler[T any](scanHandler func(record *T) bool) func(unsafe.Pointer) bool {
	if scanHandler == nil {
		return nil
	}
	return func(record unsafe.Pointer) bool { return scanHandler((*T)(record)) }
}
