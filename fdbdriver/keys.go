package fdbdriver

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// Key layout (docs/FOUNDATIONDB_PLAN.md §6.2). Every component encodes so that bytewise order is the
// value's order: integers as 8 big-endian bytes with the sign bit flipped, strings with 0x00 escaped
// as 0x00 0xFF and a 0x00 terminator (the FDB tuple layer's rule). The keys are not FDB tuples: the
// tuple package lives in the cgo binding, which this pure-Go driver must not import.
//
//	namespace       = ks 0x00
//	table           = namespace 0x01 T:u16
//	partition       = table P:int64
//	record          = partition 'r' pk…                          value: row (rowcodec.go)
//	delta entry     = partition 'd' deltaID:u16 packed:u64 pk…   value: a copy of the row
//	delta move      = partition 'm' deltaID:u16 prefix:u64 updated:int64 pk…   value: empty
//	sequence        = namespace 0x02 name
const (
	keyTagTable    = 0x01
	keyTagSequence = 0x02
	keyKindRecord  = 'r'
	keyKindDelta   = 'd'
	keyKindMove    = 'm'
)

func appendOrderedInt64(key []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(key, uint64(value)^(1<<63))
}

func appendOrderedString(key []byte, value string) []byte {
	for index := 0; index < len(value); index++ {
		key = append(key, value[index])
		if value[index] == 0x00 {
			key = append(key, 0xFF)
		}
	}
	return append(key, 0x00)
}

func namespacePrefix(namespace string) []byte {
	return append([]byte(namespace), 0x00)
}

func sequenceKey(namespace, counterName string) []byte {
	return append(append(namespacePrefix(namespace), keyTagSequence), counterName...)
}

// tablePrefix is the subspace of every key of one table.
func (table *FdbTable) tablePrefix() []byte {
	return binary.BigEndian.AppendUint16(append(namespacePrefix(table.Namespace), keyTagTable), uint16(table.ID))
}

// partitionPrefix is the subspace of one partition: its records, index entries and moves.
func (table *FdbTable) partitionPrefix(partition int64) []byte {
	return appendOrderedInt64(table.tablePrefix(), partition)
}

// appendPrimaryKey appends a record's key columns, in key order.
func (table *FdbTable) appendPrimaryKey(key []byte, recordPointer unsafe.Pointer) []byte {
	for _, keyColumn := range table.Keys {
		key = appendKeyComponent(key, keyColumn, keyColumn.GetRawValue(recordPointer))
	}
	return key
}

// appendKeyComponent encodes one key column value: a string column in string order, any integer in
// int64 order.
func appendKeyComponent(key []byte, column db.IColInfo, value any) []byte {
	if column.GetType().Type == 1 {
		text, _ := value.(string)
		return appendOrderedString(key, text)
	}
	return appendOrderedInt64(key, db.ConvertToInt64(value))
}

// recordSuffix is 'r' + pk: what follows the partition prefix in a record key, and the value of every
// index entry pointing to it.
func (table *FdbTable) recordSuffix(recordPointer unsafe.Pointer) []byte {
	return table.appendPrimaryKey([]byte{keyKindRecord}, recordPointer)
}

// deltaEntryPrefix is the span of one delta index in a partition; packed values follow it.
func (table *FdbTable) deltaEntryPrefix(partition int64, deltaID uint16) []byte {
	return binary.BigEndian.AppendUint16(append(table.partitionPrefix(partition), keyKindDelta), deltaID)
}

// deltaMovePrefix is the span of the moves out of one pinned prefix of one delta index.
func (table *FdbTable) deltaMovePrefix(partition int64, deltaID uint16, pinnedPrefix uint64) []byte {
	key := binary.BigEndian.AppendUint16(append(table.partitionPrefix(partition), keyKindMove), deltaID)
	return binary.BigEndian.AppendUint64(key, pinnedPrefix)
}

// stableIndexID names an index in its keys: 16 bits of the CRC32 of its tag, so reordering two
// declarations never swaps their entries. Collisions inside a table panic at compile.
func stableIndexID(tag string) uint16 {
	return uint16(crc32.ChecksumIEEE([]byte(tag)))
}

// prefixEnd is the first key after every key starting with prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] < 0xFF {
			end[index]++
			return end[:index+1]
		}
	}
	panic(fmt.Sprintf("fdbdriver: no key follows prefix %x", prefix))
}
