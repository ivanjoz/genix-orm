package scylla

import (
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// recordSliceGroup views several db.RecordSlice as one sequence without copying any records. The
// write path prepares inserts and updates in separate slices and then needs to walk both.
type recordSliceGroup []db.RecordSlice

func (g recordSliceGroup) Len() int {
	total := 0
	for _, slice := range g {
		total += slice.Len()
	}
	return total
}

// At indexes across the group as if it were one slice.
func (g recordSliceGroup) At(index int) unsafe.Pointer {
	for _, slice := range g {
		if index < slice.Len() {
			return slice.At(index)
		}
		index -= slice.Len()
	}
	panic("recordSliceGroup: index out of range")
}

// recordSink appends decoded rows to a *[]T without naming T. The read path needs this rather
// than db.RecordSlice because it creates records instead of walking existing ones.
//
// Decode-then-copy is deliberate: writing straight into the destination slice would hand the
// caller's scan handler a pointer that a later append can invalidate by reallocating the backing
// array. Without a scan handler the decode target is one reused scratch record (resetRecord).
type recordSink struct {
	newRecord       func() unsafe.Pointer
	appendRecord    func(unsafe.Pointer)
	asRecordPointer func(unsafe.Pointer) any
	// heldRecords points at every record the destination already holds, in order; replaceRecords
	// sets the destination to copies of the given records. Deduplication rewrites the whole
	// destination, including what the caller put there before the read.
	heldRecords    func() []unsafe.Pointer
	replaceRecords func([]unsafe.Pointer)
	// reserve grows the destination's capacity for n more records, so a read that knows its count
	// copies once instead of doubling the backing array log2(n) times. Nil on a collecting sink.
	reserve func(additionalRecords int)
	// resetRecord zeroes a record in place. Non-nil only when appendRecord copies the value, which
	// is what lets the scan loop decode every row into one scratch record instead of allocating
	// one per row. A collecting sink keeps the pointer, so it leaves this nil.
	resetRecord func(unsafe.Pointer)
}

func makeRecordSink[T any](destination *[]T) recordSink {
	return recordSink{
		newRecord:       func() unsafe.Pointer { return unsafe.Pointer(new(T)) },
		appendRecord:    func(record unsafe.Pointer) { *destination = append(*destination, *(*T)(record)) },
		asRecordPointer: func(record unsafe.Pointer) any { return (*T)(record) },
		heldRecords:     func() []unsafe.Pointer { return db.MakeRecordSlice(destination).Pointers() },
		replaceRecords: func(records []unsafe.Pointer) {
			// Copied into a new array: records may point into the one being replaced.
			replacement := make([]T, len(records))
			for index, record := range records {
				replacement[index] = *(*T)(record)
			}
			*destination = replacement
		},
		reserve:     func(additionalRecords int) { *destination = slices.Grow(*destination, additionalRecords) },
		resetRecord: func(record unsafe.Pointer) { *(*T)(record) = *new(T) },
	}
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
		heldRecords:     func() []unsafe.Pointer { return db.MakeReflectRecordSlice(recordsPointer).Pointers() },
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
	collector.reserve = nil
	collector.resetRecord = nil
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
