package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReprocessRecords: change stored records at will
//
// It reads every record of the entity and hands each one to a transform, which
// edits it in place and says whether it changed it. An unchanged record costs only
// its share of the read; a changed one is written back as the record now holds it:
//
//   - The base item only, a PutItem conditioned on its "d" still being the blob
//     read. A concurrent write wins and is reported, never overwritten.
//   - A record whose key changed is written at its new key, only if that key is
//     free, and its old item is deleted. That is also every record stored under a
//     key the schema no longer builds (its Keys or a Size changed), whatever the
//     transform says. An autoincrement ID still 0 is assigned first.
//   - Updated and Version are written as the record holds them, never stamped:
//     a migration keeps each record's real last-write time.
//   - No log append, so clients aren't notified; a change they must see goes with
//     a bump of their cache version.
//   - The hidden rows, GroupBy counters and by-IDs slots are left as they are: run
//     Reprocess (reprocess_derived.go) afterwards.
//
// The transform must be idempotent (false for a record it already changed): then a
// second run writes nothing, and rerunning is how a broken run resumes.
// ─────────────────────────────────────────────────────────────────────────────

// RecordsReport counts what one ReprocessRecords call saw over one entity.
type RecordsReport struct {
	Scanned int // records read
	Changed int // records the transform changed (on a dry run: that would be written)
	Moved   int // of those, the ones written at another key
	Written int
	// Samples shows the first changes as "before → after" JSON, to check a dry run by eye.
	Samples []string
	// Failures names each record that could not be reprocessed, with its pk/sk and the cause:
	// undecodable, a transform error, an invalid key, or written meanwhile. The rest still runs.
	Failures []string
}

const reprocessSampleCount = 3

// reprocessedRecord is a record the transform changed, with the item it was read from.
type reprocessedRecord struct {
	recordPtr  unsafe.Pointer
	storedItem map[string]types.AttributeValue
	item       map[string]types.AttributeValue
	isMove     bool
}

// recordTransform edits the record at recordPtr (a *E) in place and says whether it changed it.
type recordTransform func(recordPtr unsafe.Pointer) (bool, error)

// ReprocessRecords runs transform over every record of the entity and writes back the ones it
// changed (see the top of this file). With write false it is a dry run: it counts and samples, and
// reserves no autoincrement ID.
func (r *Repo[T, E]) ReprocessRecords(transform func(record *E) (bool, error), write bool) (RecordsReport, error) {
	return r.reprocessRecords(func(recordPtr unsafe.Pointer) (bool, error) { return transform((*E)(recordPtr)) }, write)
}

func (r *repoCore) reprocessRecords(transform recordTransform, write bool) (RecordsReport, error) {
	report := RecordsReport{}
	client, err := Client()
	if err != nil {
		return report, err
	}
	err = scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange()}, nil, func(storedItems []map[string]types.AttributeValue) error {
		pageWrites, err := r.reprocessPage(storedItems, transform, write, &report)
		if err != nil || !write {
			return err
		}

		// Each write touches its own items, so a page's writes run in parallel.
		writeFailures := make([]string, len(pageWrites))
		err = utils.RunParallel(len(pageWrites), func(writeIndex int) error {
			failure, err := writeReprocessedItem(client, pageWrites[writeIndex].item, pageWrites[writeIndex].storedItem, pageWrites[writeIndex].isMove)
			writeFailures[writeIndex] = failure
			return err
		})
		if err != nil {
			return err
		}
		for writeIndex, written := range pageWrites {
			if writeFailures[writeIndex] != "" {
				report.Failures = append(report.Failures, itemName(written.storedItem)+": "+writeFailures[writeIndex])
				continue
			}
			report.Written++
		}
		return nil
	})
	return report, err
}

// reprocessPage decodes a page of stored items, runs the transform on each record and returns the
// changed ones with the items to write, counting into report. assignsIDs reserves the autoincrement
// IDs still 0 (a dry run reserves none, so its moved records keep ID 0).
func (r *repoCore) reprocessPage(storedItems []map[string]types.AttributeValue, transform recordTransform, assignsIDs bool, report *RecordsReport) ([]reprocessedRecord, error) {
	m := r.meta
	var changedRecords []reprocessedRecord
	for _, storedItem := range storedItems {
		recordPtr, err := m.decodeRecord(storedItem)
		if err != nil {
			report.Failures = append(report.Failures, itemName(storedItem)+": "+err.Error())
			continue
		}
		report.Scanned++
		isChanged := false
		err = recoverAsError(func() (err error) {
			if isChanged, err = transform(recordPtr); err != nil || isChanged {
				return err
			}
			// Stored under a key the schema no longer builds (its Keys or a Size changed, or an
			// autoincrement ID is still 0): no read finds it, so it moves even when the transform
			// changed nothing.
			isChanged = m.recordKey(recordPtr) != attrText(storedItem["pk"])+keySeparator+attrText(storedItem["sk"])
			return nil
		})
		if err != nil {
			report.Failures = append(report.Failures, itemName(storedItem)+": "+err.Error())
			continue
		}
		if isChanged {
			changedRecords = append(changedRecords, reprocessedRecord{recordPtr: recordPtr, storedItem: storedItem})
		}
	}
	if assignsIDs {
		ptrs := make([]unsafe.Pointer, len(changedRecords))
		for i := range changedRecords {
			ptrs[i] = changedRecords[i].recordPtr
		}
		if _, err := m.assignAutoIDs(ptrs); err != nil {
			return nil, err
		}
	}

	var pageWrites []reprocessedRecord
	for _, changed := range changedRecords {
		err := recoverAsError(func() (err error) {
			changed.item, err = m.marshalItem(changed.recordPtr, m.recordPointer(changed.recordPtr))
			return err
		})
		// A reprocessed record keeps when it was created and its changelog.
		for _, managedColumn := range []string{createdColumn, changelogColumn} {
			if storedAttr, isStored := changed.storedItem[managedColumn]; isStored && changed.item != nil {
				changed.item[managedColumn] = storedAttr
			}
		}
		if err != nil {
			report.Failures = append(report.Failures, itemName(changed.storedItem)+": "+err.Error())
			continue
		}
		changed.isMove = itemName(changed.item) != itemName(changed.storedItem)
		report.Changed++
		if changed.isMove {
			report.Moved++
		}
		if len(report.Samples) < reprocessSampleCount {
			report.Samples = append(report.Samples, r.sampleOfChange(changed.storedItem, changed.recordPtr))
		}
		pageWrites = append(pageWrites, changed)
	}
	return pageWrites, nil
}

// writeReprocessedItem writes a reprocessed base item over the stored one, or at its new key and then
// deletes the stored one. A condition another write broke comes back as a failure line, not an error.
func writeReprocessedItem(client *dynamodb.Client, item, storedItem map[string]types.AttributeValue, isMove bool) (string, error) {
	isStoredBlob := map[string]types.AttributeValue{":stored": storedItem[dataColumn]}
	if !isMove {
		_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName:                 aws.String(tableName()),
			Item:                      item,
			ConditionExpression:       aws.String("#d = :stored"),
			ExpressionAttributeNames:  map[string]string{"#d": dataColumn},
			ExpressionAttributeValues: isStoredBlob,
		})
		if isConditionFailed(err) {
			return "written while reprocessing, run it again", nil
		}
		return "", err
	}

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName:           aws.String(tableName()),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if isConditionFailed(err) {
		return "its new key " + itemName(item) + " is already taken: not moved", nil
	}
	if err != nil {
		return "", err
	}
	_, err = client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       itemKeyOf(storedItem),
		ConditionExpression:       aws.String("#d = :stored"),
		ExpressionAttributeNames:  map[string]string{"#d": dataColumn},
		ExpressionAttributeValues: isStoredBlob,
	})
	if isConditionFailed(err) {
		return "copied to " + itemName(item) + ", but written meanwhile, so not deleted: reconcile the two by hand", nil
	}
	return "", err
}

// ReprocessRecordsAny is ReprocessRecords for a type-erased caller (a Controller): record is the *E.
// It lets one transform run over every registered table.
func (r *repoCore) ReprocessRecordsAny(transform func(record any) (bool, error), write bool) (RecordsReport, error) {
	return r.reprocessRecords(func(recordPtr unsafe.Pointer) (bool, error) { return transform(r.meta.recordPointer(recordPtr)) }, write)
}

// recoverAsError runs step and turns a panic into its error: a transform's own bug, or a key value
// the key encoding refuses (negative, or over its Size), fails one record instead of the whole run.
func recoverAsError(step func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%v", recovered)
		}
	}()
	return step()
}

// sampleOfChange renders a changed record as "before → after" JSON, decoding the stored item again for
// the before. json:"-" fields don't show.
func (r *repoCore) sampleOfChange(storedItem map[string]types.AttributeValue, recordPtr unsafe.Pointer) string {
	storedPtr, err := r.meta.decodeRecord(storedItem)
	if err != nil {
		return itemName(storedItem) + ": " + err.Error()
	}
	beforeJSON, _ := json.Marshal(r.meta.recordPointer(storedPtr))
	afterJSON, _ := json.Marshal(r.meta.recordPointer(recordPtr))
	return string(beforeJSON) + " → " + string(afterJSON)
}
