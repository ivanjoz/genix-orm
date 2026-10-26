package scylla

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// The DataFrame change log: one table per table with frames, <table>__dtf_log.
//
//	PRIMARY KEY ((half_hour), partition_id, ts, record_id)
//	  half_hour  unix seconds / 1800: every partition's writes of a half hour together,
//	             so a run reads all of them with one query per half hour
//	  ts         microseconds since the half hour started (< 1.8e9, an int)
//	  changes    what the write changed, the same scheme as dynamo's changelog entry:
//	               u8 format, u8 flags (bit 0: the record's insert), then, unless an
//	               insert, per changed tracked column: u16 column hash (big-endian) and
//	               the column's value before the write (zigzag varint)
//
// A write logs a record only when it inserts it or changes one of its tracked columns
// (Keys, Rows, Sums, status), so the log is one small row per real change. The rows go
// to the log BEFORE the record rows, at LocalQuorum: a change the table holds always
// has its log row, and a log row whose write then failed is harmless, because a
// History starts from the values the table holds and only undoes changes.
//
// Rows expire after 48 h (the frames reach back ~36 h, dataframe.MaxDelta), in a
// TimeWindowCompactionStrategy table that drops whole expired files.
// ─────────────────────────────────────────────────────────────────────────────

const (
	dataFrameLogBucketSeconds = 1800
	dataFrameLogTTLSeconds    = 48 * 3600
	dataFrameLogFormat        = 1
	dataFrameLogInsertFlag    = 1
)

func dataFrameLogCreateScript(scyllaTable ScyllaTable) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %v.%v (
			half_hour int, partition_id %v, ts int, record_id %v, changes blob,
			PRIMARY KEY ((half_hour), partition_id, ts, record_id)
		)
		WITH default_time_to_live = %d
		and gc_grace_seconds = 3600
		and compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS', 'compaction_window_size': 6}
		and compression = {'compression_level': '3', 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor'};`,
		resolveKeyspace(scyllaTable), scyllaTable.dataFrames.logTableName,
		scyllaTable.PartKey.GetType().DBType, scyllaTable.Keys[0].GetType().DBType, dataFrameLogTTLSeconds)
}

// dataFrameLogPosition is where a write at moment goes in the log: its half hour and its offset in it.
func dataFrameLogPosition(moment time.Time) (halfHour int32, offsetMicros int32) {
	unixMicros := moment.UnixMicro()
	bucketMicros := int64(dataFrameLogBucketSeconds) * 1e6
	return int32(unixMicros / bucketMicros), int32(unixMicros % bucketMicros)
}

// dataFrameLogMoment is the SUnixTime of a logged write.
func dataFrameLogMoment(halfHour int32, offsetMicros int32) int64 {
	return utils.SUnixTimeOf(time.UnixMicro(int64(halfHour)*dataFrameLogBucketSeconds*1e6 + int64(offsetMicros)))
}

/* Write */

// dataFrameLogRow is one logged write of a record.
type dataFrameLogRow struct {
	partition int64
	recordID  int64
	changes   []byte
}

// writeDataFrameLog logs every record the write inserts or whose tracked columns it changes, from the
// stored rows read before it. A column the write does not set keeps its stored value (0 on a record
// not stored, which the row then gets as null).
func writeDataFrameLog(scyllaTable ScyllaTable, phases []writePhase, stored storedRows) error {
	info := scyllaTable.dataFrames
	if info == nil {
		return nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	var logRows []dataFrameLogRow
	for _, phase := range phases {
		isWritten := map[string]bool{}
		for _, column := range phase.writtenColumns {
			isWritten[column.GetName()] = true
		}
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			logRow := dataFrameLogRow{partition: convertToInt64(partitionColumn.GetRawValue(recordPointer)), recordID: convertToInt64(keyColumn.GetRawValue(recordPointer))}
			if logRow.partition < 0 {
				return fmt.Errorf("table %q: a table with DataFrames takes no negative partition (%d): it names the frames' folders", scyllaTable.Name, logRow.partition)
			}
			storedRow, isStored := stored[[2]int64{logRow.partition, logRow.recordID}]
			storedValues, err := info.trackedValuesOfRow(storedRow)
			if err != nil {
				return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
			}
			writtenValues := slices.Clone(storedValues)
			for c, column := range info.trackedColumns {
				if isWritten[column.GetName()] {
					if writtenValues[c], err = info.trackedValueOf(column, column.GetRawValue(recordPointer)); err != nil {
						return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
					}
				}
			}
			if err := info.frames.CheckValues(writtenValues); err != nil {
				return err
			}
			if logRow.changes = info.encodeLogChanges(isStored, storedValues, writtenValues); logRow.changes != nil {
				logRows = append(logRows, logRow)
			}
		}
	}
	if len(logRows) == 0 {
		return nil
	}

	// Every row of one write shares its moment; the record ID tells them apart.
	halfHour, offsetMicros := dataFrameLogPosition(db.Now())
	statement := fmt.Sprintf("INSERT INTO %v.%v (half_hour, partition_id, ts, record_id, changes) VALUES (?, ?, ?, ?, ?)",
		resolveKeyspace(scyllaTable), info.logTableName)
	session := getScyllaConnection()
	for start := 0; start < len(logRows); start += maxInsertBatchRows {
		batch := session.NewBatch(gocql.UnloggedBatch)
		batch.SetConsistency(gocql.LocalQuorum)
		for _, logRow := range logRows[start:min(start+maxInsertBatchRows, len(logRows))] {
			batch.Query(statement, halfHour, coerceManagedIntegerValue(partitionColumn, logRow.partition), offsetMicros,
				coerceManagedIntegerValue(keyColumn, logRow.recordID), logRow.changes)
		}
		if err := session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("table %q: writing the DataFrame log: %w", scyllaTable.Name, err)
		}
	}
	return nil
}

// encodeLogChanges is the changes blob of one write: the insert of a record not stored, or the stored
// values of the columns it changes; nil when it changes none.
func (info *dataFrameInfo) encodeLogChanges(isStored bool, storedValues, writtenValues []int64) []byte {
	if !isStored {
		return []byte{dataFrameLogFormat, dataFrameLogInsertFlag}
	}
	var changes []byte
	for c := range info.trackedColumns {
		if storedValues[c] != writtenValues[c] {
			if changes == nil {
				changes = []byte{dataFrameLogFormat, 0}
			}
			changes = binary.BigEndian.AppendUint16(changes, info.columnHashes[c])
			changes = binary.AppendVarint(changes, storedValues[c])
		}
	}
	return changes
}

var errDataFrameLogCorrupt = errors.New("the DataFrame log row is corrupt")

// decodeLogChange reads a changes blob into the change at moment. A column no longer tracked is
// skipped: no frame reads it.
func (info *dataFrameInfo) decodeLogChange(moment int64, changes []byte) (dataframe.HistoryChange, error) {
	change := dataframe.HistoryChange{Moment: moment}
	if len(changes) < 2 || changes[0] != dataFrameLogFormat {
		return change, errDataFrameLogCorrupt
	}
	if change.IsInsert = changes[1]&dataFrameLogInsertFlag != 0; change.IsInsert {
		return change, nil
	}
	for position := 2; position < len(changes); {
		if len(changes)-position < 3 {
			return change, errDataFrameLogCorrupt
		}
		columnHash := binary.BigEndian.Uint16(changes[position:])
		before, size := binary.Varint(changes[position+2:])
		if size <= 0 {
			return change, errDataFrameLogCorrupt
		}
		position += 2 + size
		if trackedIndex, isTracked := info.trackedIndexByHash[columnHash]; isTracked {
			change.TrackedIndexes = append(change.TrackedIndexes, trackedIndex)
			change.Before = append(change.Before, before)
		}
	}
	return change, nil
}

/* Read */

// readDataFrameLog reads the changes logged after the SUnixTime afterMoment, of one partition or,
// with isEveryPartition, of all, by (partition, record ID), newest first per record.
func readDataFrameLog(scyllaTable ScyllaTable, partition int64, isEveryPartition bool, afterMoment int64, now time.Time) (map[[2]int64][]dataframe.HistoryChange, error) {
	info := scyllaTable.dataFrames
	firstHalfHour, _ := dataFrameLogPosition(time.Unix(afterMoment*2+1e9, 0))
	lastHalfHour, _ := dataFrameLogPosition(now)
	queryString := fmt.Sprintf("SELECT partition_id, ts, record_id, changes FROM %v.%v WHERE half_hour = ?",
		resolveKeyspace(scyllaTable), info.logTableName)
	if !isEveryPartition {
		queryString += " AND partition_id = ?"
	}

	changesByRecord := map[[2]int64][]dataframe.HistoryChange{}
	for halfHour := firstHalfHour; halfHour <= lastHalfHour; halfHour++ {
		queryValues := []any{halfHour}
		if !isEveryPartition {
			queryValues = append(queryValues, coerceManagedIntegerValue(scyllaTable.GetPartKey(), partition))
		}
		iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
		var rowPartition, recordID int64
		var offsetMicros int32
		var changes []byte
		for iterator.Scan(&rowPartition, &offsetMicros, &recordID, &changes) {
			moment := dataFrameLogMoment(halfHour, offsetMicros)
			if moment <= afterMoment {
				continue
			}
			change, err := info.decodeLogChange(moment, changes)
			if err != nil {
				iterator.Close()
				return nil, fmt.Errorf("table %q: %w", scyllaTable.Name, err)
			}
			recordKey := [2]int64{rowPartition, recordID}
			changesByRecord[recordKey] = append(changesByRecord[recordKey], change)
		}
		if err := iterator.Close(); err != nil {
			return nil, fmt.Errorf("table %q: reading the DataFrame log: %w", scyllaTable.Name, err)
		}
	}
	// A partition's rows come by ts; across half hours they come in order too. Newest first.
	for _, changes := range changesByRecord {
		slices.Reverse(changes)
	}
	return changesByRecord, nil
}
