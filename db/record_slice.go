package db

import (
	"reflect"
	"unsafe"
)

// RecordSlice is a type-erased view over a *[]T, shared by every driver's write path.
//
// Why it exists: every engine function below the Executor boundary used its record type parameter
// for exactly one thing -- the address of each element -- because column reads and writes already
// go through the precompiled unsafe.Pointer accessors on the table. The type parameter was doing no
// work, but Go still stencils the whole function once per record struct. Passing this instead lets
// those bodies compile once. See docs/BINARY_SIZE_PLAN.md in the parent repo.
type RecordSlice struct {
	// firstElement is an unsafe.Pointer rather than a uintptr on purpose: the GC traces
	// unsafe.Pointer fields, so holding one keeps the backing array alive for as long as the
	// RecordSlice does. A uintptr here would be a use-after-free waiting for a GC cycle.
	firstElement unsafe.Pointer
	length       int
	elementSize  uintptr
	// RecordType is T, for the bodies that must build new records or slices of it.
	RecordType reflect.Type
	// asRecordPointer converts an element address back into an interface holding *T. A handful of
	// places need the record's own method set rather than its columns -- SelfParse is declared on
	// the record type, so no column accessor can reach it.
	// The closure captures nothing, so it is a static func value: no allocation, one per table.
	asRecordPointer func(unsafe.Pointer) any
}

// MakeRecordSlice is the only generic function in the write path that survives per table, and it
// compiles to a few instructions.
func MakeRecordSlice[T any](records *[]T) RecordSlice {
	slice := RecordSlice{
		// elementSize still has to be right for an empty slice: it can be sub-sliced and appended
		// to by the caller, and At() must stay correct if it is.
		elementSize:     unsafe.Sizeof(*new(T)),
		RecordType:      reflect.TypeFor[T](),
		asRecordPointer: func(elementAddress unsafe.Pointer) any { return (*T)(elementAddress) },
	}
	if records != nil && len(*records) > 0 {
		slice.firstElement = unsafe.Pointer(unsafe.SliceData(*records))
		slice.length = len(*records)
	}
	return slice
}

// MakeReflectRecordSlice is MakeRecordSlice for a *[]T held in a reflect.Value, for the paths that
// know the record type only at runtime (admin and backup bodies, the insert/update split).
func MakeReflectRecordSlice(recordsPointer reflect.Value) RecordSlice {
	records := recordsPointer.Elem()
	recordType := records.Type().Elem()
	slice := RecordSlice{
		elementSize:     recordType.Size(),
		RecordType:      recordType,
		asRecordPointer: func(record unsafe.Pointer) any { return reflect.NewAt(recordType, record).Interface() },
	}
	if records.Len() > 0 {
		slice.firstElement = records.UnsafePointer()
		slice.length = records.Len()
	}
	return slice
}

// RecordPointerAt returns element i as an interface holding *T, for the callers that need the
// record type's own methods. Use At() unless you specifically need the method set.
func (r RecordSlice) RecordPointerAt(index int) any { return r.asRecordPointer(r.At(index)) }

// At returns a pointer to element i, equivalent to unsafe.Pointer(&(*records)[i]).
func (r RecordSlice) At(index int) unsafe.Pointer {
	return unsafe.Add(r.firstElement, uintptr(index)*r.elementSize)
}

func (r RecordSlice) Len() int { return r.length }

// Sub returns the view of [start, end), matching how the write paths chunk records.
func (r RecordSlice) Sub(start, end int) RecordSlice {
	subSlice := r
	if start >= end {
		subSlice.firstElement, subSlice.length = nil, 0
		return subSlice
	}
	subSlice.firstElement, subSlice.length = r.At(start), end-start
	return subSlice
}

// NewRecordList starts an empty reflect-built []T that AppendCopy fills, for the bodies that split
// or regroup records into slices of their own.
func (r RecordSlice) NewRecordList() reflect.Value {
	recordsPointer := reflect.New(reflect.SliceOf(r.RecordType))
	recordsPointer.Elem().Set(reflect.MakeSlice(recordsPointer.Elem().Type(), 0, r.length))
	return recordsPointer
}

// AppendCopy appends a copy of the record at recordPointer to the list NewRecordList started.
func (r RecordSlice) AppendCopy(recordsPointer reflect.Value, recordPointer unsafe.Pointer) {
	records := recordsPointer.Elem()
	records.Set(reflect.Append(records, reflect.NewAt(r.RecordType, recordPointer).Elem()))
}

// NewRecord allocates one zero T.
func (r RecordSlice) NewRecord() unsafe.Pointer { return reflect.New(r.RecordType).UnsafePointer() }

// CopyRecord overwrites the record at destination with the one at source.
func (r RecordSlice) CopyRecord(destination, source unsafe.Pointer) {
	reflect.NewAt(r.RecordType, destination).Elem().Set(reflect.NewAt(r.RecordType, source).Elem())
}

// Pointers materializes one pointer per record, for the few callers that group or index records
// rather than walking them in order.
func (r RecordSlice) Pointers() []unsafe.Pointer {
	recordPointers := make([]unsafe.Pointer, r.length)
	for index := range recordPointers {
		recordPointers[index] = r.At(index)
	}
	return recordPointers
}
