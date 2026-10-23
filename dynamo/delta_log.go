package dynamo

import (
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/db/cache"
)

// Delta logs: the files, their buckets, the cursor rules and the Metadata live in db/cache
// (delta_log.go). This file is dynamo's side: which folders a record is in, its log ID, listing the
// view through execPlans, and the consistent BatchGetItem of the logged records.

// DeltaLogEntry is one change of a delta log (db/cache).
type DeltaLogEntry = cache.DeltaLogEntry

// deltaLogWrite is one landed write: the record as stored before it (nil when new, or not read: a
// table without hidden rows can't move between folders), as written (nil for a delete), and the
// Version the write gave it.
type deltaLogWrite struct {
	storedPtr, writtenPtr unsafe.Pointer
	version               int64
}

// appendDeltaLogs appends the entries of the landed writes to their delta logs.
func (m *tableMeta) appendDeltaLogs(writes []deltaLogWrite) error {
	if len(m.deltaIndexes) == 0 || len(writes) == 0 {
		return nil
	}
	if cache.FileStore() == nil {
		return fmt.Errorf("db: %s has a TypeDelta index and no file store holds its log: call SetFileStore at boot", m.recordType.Name())
	}
	appends := cache.NewDeltaLogAppends(Now())
	for _, write := range writes {
		keyPtr := write.writtenPtr
		if keyPtr == nil {
			keyPtr = write.storedPtr
		}
		basePK := m.pkValue(keyPtr)
		// Status 0 leaves every view: physical deletes are not part of the app's flow, so the deleted
		// entry is the soft delete's. Whether a screen keeps deactivated records is its own decision:
		// such a route lists its view instead of reading the log.
		isSoftDeleted := write.writtenPtr != nil && m.status != nil && m.status.getI64(write.writtenPtr) == 0
		for i := range m.deltaIndexes {
			deltaIndex := &m.deltaIndexes[i]
			indexFolder := cache.DeltaLogFolder + basePK + "/" + deltaIndex.tag + "/"
			logID, version := deltaIndex.logIDOf(keyPtr), cache.WireVersion(int16(write.version))
			writtenFolders := m.pinnedFolders(deltaIndex, write.writtenPtr)
			leftFolders := m.pinnedFolders(deltaIndex, write.storedPtr)
			if isSoftDeleted {
				leftFolders, writtenFolders = append(leftFolders, writtenFolders...), nil
			}
			for _, folder := range writtenFolders {
				appends.Add(indexFolder+folder, deltaIndex.is64BitScale, DeltaLogEntry{LogID: logID, Version: version})
			}
			for _, folder := range leftFolders {
				if !slices.Contains(writtenFolders, folder) {
					appends.Add(indexFolder+folder, deltaIndex.is64BitScale, DeltaLogEntry{LogID: logID, Version: version, IsDeleted: true})
				}
			}
		}
	}
	if err := appends.Write(); err != nil {
		return fmt.Errorf("db: %s %w", m.recordType.Name(), err)
	}
	return nil
}

// execDelta is Exec for a Delta() read (db/cache's delta_log.go describes the protocol).
func (q *queryState) execDelta(client *dynamodb.Client, appendRecord recordSink) error {
	if q.planErr != nil {
		return q.planErr
	}
	if cache.FileStore() == nil {
		return fmt.Errorf("db: %s Delta() reads a delta log and no file store holds it: call SetFileStore at boot", q.meta.recordType.Name())
	}
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}
	partitionValues, isPartitionPinned := pinPartition(byField, q.meta.partition, map[string]bool{})
	if !isPartitionPinned {
		return fmt.Errorf("db: %s Delta() needs an equality on every Partition column: each partition has its own log", q.meta.recordType.Name())
	}
	folder := cache.DeltaLogFolder + q.meta.numericKey(partitionValues, q.meta.partition) + "/" + q.deltaIndex.tag + "/"
	for _, column := range q.deltaIndex.pinned {
		pinnedValue := byField[column.fieldName].v1
		if byField[column.fieldName].op == opContains {
			pinnedValue = pinnedValue.([]any)[0]
		}
		folder += cache.PinnedValueText(pinnedValue) + "/"
	}

	// The files go first: a write landing after them comes again on the next sync.
	logRead, err := cache.ReadDeltaLog(folder, q.deltaCursor, Now())
	if err != nil {
		return fmt.Errorf("db: %s %w", q.meta.recordType.Name(), err)
	}
	if logRead.IsReplace {
		err = q.execPlans(client, appendRecord)
	} else {
		err = q.appendLoggedRecords(client, partitionValues, byField, logRead, appendRecord)
	}
	if err != nil {
		return err
	}
	for _, metadataFile := range logRead.MetadataFiles(q.deltaIndex.is64BitScale, q.deltaIndex.logIDColumnBits) {
		appendRecord(q.deltaMetadataRecord(metadataFile))
	}
	return nil
}

// appendLoggedRecords reads the records the log entries past the cursor name, the last entry per log
// ID winning, and appends those not deleted through appendRecord.
func (q *queryState) appendLoggedRecords(client *dynamodb.Client, partitionValues []uint64, byField map[string]predicate, logRead *cache.DeltaLogRead, appendRecord recordSink) error {
	entries, err := logRead.LoggedEntries(q.deltaIndex.is64BitScale)
	if err != nil {
		return fmt.Errorf("db: %s %w", q.meta.recordType.Name(), err)
	}

	// Every key shares the partition and the pinned Keys columns; the log ID fills in the rest.
	keyTemplateValue := reflect.New(q.meta.recordType).Elem()
	for i, column := range q.meta.partition {
		setKeyField(keyTemplateValue, column.fieldName, partitionValues[i])
	}
	for _, column := range q.deltaIndex.pinned {
		if pinnedPredicate, isKeysColumn := byField[column.fieldName]; isKeysColumn && pinnedPredicate.op == opEq && q.meta.isKeysColumn(column.fieldName) {
			setKeyField(keyTemplateValue, column.fieldName, pinnedPredicate.v1)
		}
	}
	var itemKeys []map[string]types.AttributeValue
	for _, entry := range entries {
		if entry.IsDeleted {
			continue
		}
		recordKey := reflect.New(q.meta.recordType)
		recordKeyValue := recordKey.Elem()
		recordKeyValue.Set(keyTemplateValue)
		logIDValues := []uint64{entry.LogID}
		if len(q.deltaIndex.logIDColumns) > 1 {
			logIDValues = cache.UnpackDeltaLogID(entry.LogID, q.deltaIndex.logIDColumnBits)
		}
		for j, column := range q.deltaIndex.logIDColumns {
			setKeyField(recordKeyValue, column.fieldName, logIDValues[j])
		}
		itemKeys = append(itemKeys, q.meta.keyOnly(recordKey.UnsafePointer()))
	}
	items, _, err := batchGet(client, itemKeys, true)
	if err != nil {
		return err
	}
	for _, item := range items {
		recordPtr, err := q.meta.decodeRecord(item)
		if err != nil {
			return err
		}
		appendRecord(recordPtr)
	}
	return nil
}

// deltaMetadataRecord is the Metadata record of one log file: −bucket in its log ID's last column.
func (q *queryState) deltaMetadataRecord(metadataFile cache.DeltaMetadataFile) unsafe.Pointer {
	record := reflect.New(q.meta.recordType)
	recordValue := record.Elem()
	setKeyField(recordValue, q.deltaIndex.logIDColumns[len(q.deltaIndex.logIDColumns)-1].fieldName, -metadataFile.Bucket)
	metadataText := metadataFile.Text
	recordValue.FieldByName(metadataFieldName).Set(reflect.ValueOf(&metadataText))
	return record.UnsafePointer()
}

// setKeyField sets a key field of a record from any integer or string value.
func setKeyField(record reflect.Value, fieldName string, value any) {
	field := record.FieldByName(fieldName)
	field.Set(reflect.ValueOf(value).Convert(field.Type()))
}

// isKeysColumn reports whether fieldName is one of the table's Keys.
func (m *tableMeta) isKeysColumn(fieldName string) bool {
	return slices.ContainsFunc(m.keys, func(column keyCol) bool { return column.fieldName == fieldName })
}

// DeltaReplaceMetadata is the Metadata of a route not backed by a delta log that answers its whole
// view every time (db/cache).
func DeltaReplaceMetadata() string { return cache.DeltaReplaceMetadata() }
