package dynamo

import (
	"bytes"
	"context"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Reprocess: rebuild everything derived from an entity's records
//
// The records are the truth; the hidden rows (fan-out, delta and local index rows),
// the GroupBy counters and the by-IDs slots are derived from them. Reprocess reads
// every record and every hidden row of the entity, and makes the derived data match:
//
//   - A row no record produces is deleted: a removed element, an index no longer
//     declared, a row shape the ORM stopped writing. A missing row is put, and so is
//     a FullCopy row whose blob or Version is behind its record.
//   - The counters of every partition are recomputed, as rebuildPartitionGroups does,
//     and the counters of an entity that declares no GroupBy anymore are deleted.
//   - The slots of the records written in the current year cycle are raised to their
//     Updated.
//
// It writes only what differs, so a table with nothing to fix costs reads only. It is
// the fix for drift, the backfill after a schema change, and the second half of a
// ReprocessRecords run. The records, Version and the change log stay as they are.
// Writes landing while it runs can be undone by it: run it when the table is quiet; a
// second run converges.
// ─────────────────────────────────────────────────────────────────────────────

// DerivedReport counts what one Reprocess call found over one entity (on a dry run: would write).
type DerivedReport struct {
	Records           int // records read
	HiddenRowsPut     int // missing hidden rows, and FullCopy rows behind their record
	HiddenRowsDeleted int // hidden rows no record produces
	CountersRewritten int // GroupBy counters put or deleted
}

// expectedHiddenRow is a hidden row the records produce.
type expectedHiddenRow struct {
	item       map[string]types.AttributeValue
	isFullCopy bool
}

// Reprocess rebuilds the entity's derived data from its records (see the top of this file). With
// write false it is a dry run: it reads everything and counts what it would write.
func (r *Repo[T, E]) Reprocess(write bool) (DerivedReport, error) {
	m := r.meta
	report := DerivedReport{}
	client, err := Client()
	if err != nil {
		return report, err
	}

	var records []E
	expectedRows := map[string]expectedHiddenRow{} // keyed by pk # sk
	err = scanItems(client, []pkRange{m.baseRowsRange()}, nil, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			var record E
			if err := m.unmarshalItem(item, &record); err != nil {
				return err
			}
			records = append(records, record)
			ptr := unsafe.Pointer(&record)
			basePK := m.pkValue(ptr)
			for i := range m.arrayIndexes {
				arrayIndex := &m.arrayIndexes[i]
				for _, rowSK := range m.arrayRowSKs(arrayIndex, ptr) {
					row := itemKey(basePK+arrayIndex.columnID, rowSK)
					if arrayIndex.fullCopy {
						row[dataColumn], row[versionColumn] = item[dataColumn], item[versionColumn]
					}
					expectedRows[basePK+arrayIndex.columnID+keySeparator+rowSK] = expectedHiddenRow{row, arrayIndex.fullCopy}
				}
			}
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	report.Records = len(records)

	// The stored hidden rows: each one is kept, refreshed or deleted against the expected set. The
	// counters (pk ‖ 000) only name the partitions to recompute, unless no GroupBy is declared.
	var rowPuts, rowDeletes []types.WriteRequest
	partitionsWithCounters := map[string]bool{}
	err = scanItems(client, []pkRange{m.hiddenRowsRange()}, nil, func(items []map[string]types.AttributeValue) error {
		for _, storedRow := range items {
			rowPK, rowSK := attrText(storedRow["pk"]), attrText(storedRow["sk"])
			if strings.HasSuffix(rowPK, countersColumnID) && len(m.groupIndexes) > 0 {
				partitionsWithCounters[strings.TrimSuffix(rowPK, countersColumnID)] = true
				continue
			}
			rowKey := rowPK + keySeparator + rowSK
			expected, isExpected := expectedRows[rowKey]
			if !isExpected {
				rowDeletes = append(rowDeletes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: itemKeyOf(storedRow)}})
				continue
			}
			delete(expectedRows, rowKey)
			if expected.isFullCopy && !fullCopyRowMatches(storedRow, expected.item) {
				rowPuts = append(rowPuts, types.WriteRequest{PutRequest: &types.PutRequest{Item: expected.item}})
			}
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	for _, missing := range expectedRows {
		rowPuts = append(rowPuts, types.WriteRequest{PutRequest: &types.PutRequest{Item: missing.item}})
	}
	report.HiddenRowsPut, report.HiddenRowsDeleted = len(rowPuts), len(rowDeletes)
	if write {
		// New rows first, then stale ones, as the write path does: a crash leaves extra rows, never a
		// missing one, and every read re-checks a row against its record.
		if err := r.batchWriteAll(context.Background(), client, rowPuts); err != nil {
			return report, err
		}
		if err := r.batchWriteAll(context.Background(), client, rowDeletes); err != nil {
			return report, err
		}
	}

	if len(m.groupIndexes) > 0 {
		recordsByPK := map[string][]E{}
		for basePK := range partitionsWithCounters {
			recordsByPK[basePK] = nil // a partition with counters and no records gets them zeroed
		}
		for i := range records {
			basePK := m.pkValue(unsafe.Pointer(&records[i]))
			recordsByPK[basePK] = append(recordsByPK[basePK], records[i])
		}
		for basePK, partitionRecords := range recordsByPK {
			rewritten, err := r.rebuildPartitionGroups(client, basePK, partitionRecords, write)
			report.CountersRewritten += rewritten
			if err != nil {
				return report, err
			}
		}
	}

	if m.cacheByIDs != nil && write {
		// Slot files of past cycles are never read again; a record not written this cycle keeps tick 1.
		currentCycle := CurrentYearCycle()
		var currentCyclePtrs []unsafe.Pointer
		for i := range records {
			ptr := unsafe.Pointer(&records[i])
			if yearCycleOf(sUnixTimeToTime(m.updated.acc.getI64(ptr))) == currentCycle {
				currentCyclePtrs = append(currentCyclePtrs, ptr)
			}
		}
		if err := m.setSlotsUpdated(currentCyclePtrs); err != nil {
			return report, err
		}
	}
	return report, nil
}

// fullCopyRowMatches reports whether a stored FullCopy row already copies its record's blob and Version.
func fullCopyRowMatches(storedRow, expectedRow map[string]types.AttributeValue) bool {
	storedBlob, hasStoredBlob := storedRow[dataColumn].(*types.AttributeValueMemberB)
	expectedBlob, hasExpectedBlob := expectedRow[dataColumn].(*types.AttributeValueMemberB)
	return hasStoredBlob && hasExpectedBlob && bytes.Equal(storedBlob.Value, expectedBlob.Value) &&
		attrText(storedRow[versionColumn]) == attrText(expectedRow[versionColumn])
}
