package scylla

import (
	"fmt"
	"unsafe"
)

// materializePackedIndex turns a described packed index (db.IndexKindPacked) into a Scylla
// secondary index on its generated column. The column's described raw value is the unsigned
// layout; Scylla compares integers signed, so it is stored with the top bit flipped
// (storeVirtualPacked); planned WHERE bounds get the same mapping in encodeAccessValue.
func materializePackedIndex(dbTable *ScyllaTable, view *viewInfo) {
	packedIndex := view.PackedIndex
	indexName := view.Name
	packedColumnNameLocal := packedIndex.PackedColumnName
	isInt32PackedLocal := packedIndex.IsInt32Packed

	packedColumn := view.Column.(*columnInfo)
	describedRawValue := packedColumn.GetRawValueFn
	packedColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
		return storeVirtualPacked(describedRawValue(ptr).(uint64), isInt32PackedLocal)
	}
	packedColumn.GetValueFn = packedColumn.GetRawValueFn

	if packedIndex.PartitionColumnName != "" {
		view.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`, indexName, dbTable.GetFullName(), packedIndex.PartitionColumnName, packedColumnNameLocal)
		}
	} else {
		view.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, indexName, dbTable.GetFullName(), packedColumnNameLocal)
		}
	}

}
