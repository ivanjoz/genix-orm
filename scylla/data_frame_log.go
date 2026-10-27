package scylla

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"

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
//	  changes    what the write changed: u8 format, u8 flags (bit 0: the record's
//	             insert), then, unless an insert, the lines every frame it changed held
//	             before it (dataframe.Table.EncodeChangedLines: the body of dynamo's
//	             changelog entry)
//
// A write logs a record only when it inserts it or changes the lines it puts in a frame
// (its stored row and the row it leaves, both turned into lines), so the log is one row
// per real change. The rows go to the log BEFORE the record rows, at LocalQuorum: a
// change the table holds always has its log row, and a log row whose write then failed
// is harmless, because a History starts from the lines the table holds and only undoes
// changes.
//
// Rows expire after 48 h (the frames reach back ~36 h, dataframe.MaxDelta), in a
// TimeWindowCompactionStrategy table that drops whole expired files.
// ─────────────────────────────────────────────────────────────────────────────

const (
	dataFrameLogBucketSeconds = 1800
	dataFrameLogTTLSeconds    = 48 * 3600
	dataFrameLogFormat        = 2
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

// writeDataFrameLog logs every record the write inserts or whose lines it changes, from the stored
// rows read before it. The row the write leaves is the stored row with the written columns over it (a
// payment writes only its status), so a partial update still yields the record's whole lines.
func writeDataFrameLog(scyllaTable ScyllaTable, phases []writePhase, stored storedRows) error {
	info := scyllaTable.dataFrames
	if info == nil {
		return nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	var logRows []dataFrameLogRow
	for _, phase := range phases {
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			logRow := dataFrameLogRow{partition: convertToInt64(partitionColumn.GetRawValue(recordPointer)), recordID: convertToInt64(keyColumn.GetRawValue(recordPointer))}
			if logRow.partition < 0 {
				return fmt.Errorf("table %q: a table with DataFrames takes no negative partition (%d): it names the frames' folders", scyllaTable.Name, logRow.partition)
			}
			storedRow, isStored := stored[[2]int64{logRow.partition, logRow.recordID}]
			storedRecord := info.newRecord(storedRow)
			writtenRecord := info.newRecord(storedRow)
			for _, column := range phase.writtenColumns {
				copyColumnField(column, writtenRecord, recordPointer)
			}
			// LinesOf also refuses what a frame can't keep, before anything is written.
			writtenLines, err := info.frames.LinesOf(writtenRecord)
			if err != nil {
				return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
			}
			if !isStored {
				logRow.changes = []byte{dataFrameLogFormat, dataFrameLogInsertFlag}
				logRows = append(logRows, logRow)
				continue
			}
			storedLines, err := info.frames.LinesOf(storedRecord)
			if err != nil {
				return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
			}
			if body := info.frames.EncodeChangedLines(storedLines, writtenLines); body != nil {
				logRow.changes = append([]byte{dataFrameLogFormat, 0}, body...)
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

// copyColumnField copies one column's field from the record at source to the record at destination.
func copyColumnField(column IColInfo, destination, source unsafe.Pointer) {
	field := column.GetInfo().Field
	if field == nil {
		return
	}
	reflect.NewAt(field.Type, field.Pointer(destination)).Elem().Set(reflect.NewAt(field.Type, field.Pointer(source)).Elem())
}

var errDataFrameLogCorrupt = errors.New("the DataFrame log row is corrupt")

// decodeLogChange reads a changes blob into the change at moment. A row of another format (written
// before the frames moved to lines) is skipped: those frames are rebuilt on their new shape anyway.
func decodeLogChange(moment int64, changes []byte) (change dataframe.HistoryChange, isKept bool, err error) {
	change = dataframe.HistoryChange{Moment: moment}
	if len(changes) < 2 {
		return change, false, errDataFrameLogCorrupt
	}
	if changes[0] != dataFrameLogFormat {
		return change, false, nil
	}
	change.IsInsert, change.Body = changes[1]&dataFrameLogInsertFlag != 0, slices.Clone(changes[2:])
	return change, true, nil
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
			change, isKept, err := decodeLogChange(moment, changes)
			if err != nil {
				iterator.Close()
				return nil, fmt.Errorf("table %q: %w", scyllaTable.Name, err)
			}
			if !isKept {
				continue
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
