package dynamo

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Copying a table into a new one
//
// A key shape DynamoDB cannot change in place (an attribute type of the table's
// or a GSI's key) is migrated by copying the data into a new table created with
// the current shape. Only what can't be derived is copied:
//
//   - CopySequencesFrom: the autoincrement and ReserveIDs counters (pk 0). Run it
//     first, so an ID assigned while copying continues the old sequence.
//   - CopyRecordsFrom: every record of the entity, rebuilt at the key the schema
//     builds now, Updated and Version as stored. A record already copied is
//     copied again only when the source holds a higher Version, so a last run
//     right before the switch picks up the writes made meanwhile.
//
// The hidden rows, GroupBy counters and by-IDs slots are derived: run Reprocess
// on the new table afterwards. Both copies skip what the new table already holds,
// so rerunning resumes a broken run. The source is only read.
// ─────────────────────────────────────────────────────────────────────────────

// CopySequencesFrom copies every sequence counter (pk 0) of sourceTable into the current table and
// returns how many it wrote; one already in the current table is only raised. The source's sk may be of
// any key type: the counter name is the same text.
func CopySequencesFrom(sourceTable string, write bool) (int, error) {
	client, err := Client()
	if err != nil {
		return 0, err
	}
	copiedCount := 0
	err = scanItems(client, sourceTable, []pkRange{{sequencePartitionKey, sequencePartitionKey}}, nil, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			counter, hasCounter := item[seqValueAttr].(*types.AttributeValueMemberN)
			if !hasCounter {
				continue
			}
			if !write {
				copiedCount++
				continue
			}
			sequenceItem := sequenceKey(keyAttrText(item["sk"]))
			sequenceItem[seqValueAttr] = counter
			// A counter never moves back: one the current table already holds is raised, never lowered.
			_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName:                 aws.String(tableName()),
				Item:                      sequenceItem,
				ConditionExpression:       aws.String("attribute_not_exists(pk) OR #cv < :cv"),
				ExpressionAttributeNames:  map[string]string{"#cv": seqValueAttr},
				ExpressionAttributeValues: map[string]types.AttributeValue{":cv": counter},
			})
			if isConditionFailed(err) {
				continue
			}
			if err != nil {
				return err
			}
			copiedCount++
		}
		return nil
	})
	return copiedCount, err
}

// CopyRecordsFrom writes every record of the entity stored in sourceTable into the current table, at
// the key the schema builds now, with Updated and Version as stored (no log append, no stamp). A
// record the current table already holds at the same or a higher Version is skipped. With write false it is a dry run.
func (r *Repo[T, E]) CopyRecordsFrom(sourceTable string, write bool) (RecordsReport, error) {
	report := RecordsReport{}
	client, err := Client()
	if err != nil {
		return report, err
	}
	copyEveryRecord := func(record *E) (bool, error) { return true, nil }
	err = scanItems(client, sourceTable, []pkRange{r.meta.baseRowsRange()}, nil, func(storedItems []map[string]types.AttributeValue) error {
		pageWrites, err := r.reprocessPage(storedItems, copyEveryRecord, write, &report)
		if err != nil || !write {
			return err
		}
		isAlreadyCopied := make([]bool, len(pageWrites))
		err = utils.RunParallel(len(pageWrites), func(writeIndex int) error {
			item := pageWrites[writeIndex].item
			// A record written in the source after an earlier run (a higher Version) is copied again.
			_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName:                 aws.String(tableName()),
				Item:                      item,
				ConditionExpression:       aws.String("attribute_not_exists(pk) OR #ver < :ver"),
				ExpressionAttributeNames:  map[string]string{"#ver": versionColumn},
				ExpressionAttributeValues: map[string]types.AttributeValue{":ver": item[versionColumn]},
			})
			if isConditionFailed(err) {
				isAlreadyCopied[writeIndex] = true
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
		for writeIndex := range pageWrites {
			if !isAlreadyCopied[writeIndex] {
				report.Written++
			}
		}
		return nil
	})
	return report, err
}

// CountItemsByOwner counts every item of a table by the entity that owns it (its hidden rows and
// counters included; "sequence" for pk 0),
// or by "TableID <n>" when no registered entity has that TableID, so a copy can be checked to leave
// nothing behind.
func CountItemsByOwner(table string) (map[string]int, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	countByOwner := map[string]int{}
	allPKs := pkRange{"0", strings.Repeat("9", maxNumericKeyDigits)}
	err = scanItems(client, table, []pkRange{allPKs}, []string{"pk", "sk"}, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			pk := attrText(item["pk"])
			if pk == sequencePartitionKey {
				countByOwner["sequence"]++
			} else if entity, isOwned := EntityOfItemKey(pk, keyAttrText(item["sk"])); isOwned {
				countByOwner[entity]++
			} else {
				countByOwner["TableID "+pk[:min(8, len(pk))]]++
			}
		}
		return nil
	})
	return countByOwner, err
}

// keyAttrText reads a sort key of either type: a binary one, or a string one of a table copied from.
func keyAttrText(attr types.AttributeValue) string {
	if stringKey, isString := attr.(*types.AttributeValueMemberS); isString {
		return stringKey.Value
	}
	return attrText(attr)
}
