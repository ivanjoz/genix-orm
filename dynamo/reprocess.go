package dynamo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// Reprocess: carry the stored "d" blobs across a colbin wire change
//
// A colbin release that changes the wire leaves every stored blob unreadable by the
// new decoder. ReprocessBlobs reads each one with the decoder of the version that
// wrote it (the caller vendors that version: Go holds one version of a module) and
// writes it back encoded by the colbin this package is built with.
//
// Every "d" in the entity's pk ranges is a colbin of E: the base rows, the full-copy
// array index rows and the GroupBy counters (a record holding only the group Keys).
// Only "d" is rewritten, with an UpdateItem conditional on the blob still being the
// one read: keys, indexes, counters and upd stay as they are, so no delta client
// refetches and no counter drifts. That is also why it cannot go through Put: a Put
// decodes the stored version first, with the new decoder.
//
// It is idempotent. A blob the current colbin decodes and re-encodes to the same
// bytes is already migrated and left alone, so a second run rewrites nothing.
// ─────────────────────────────────────────────────────────────────────────────

// ReprocessReport counts what one ReprocessBlobs call saw over one entity.
type ReprocessReport struct {
	Scanned   int // items holding a "d" blob
	Current   int // already in the current colbin's format: left alone
	Rewritten int // re-encoded (on a dry run: that would be)
	// Ambiguous counts blobs both colbin versions read, as different records: left alone. Before a
	// first run it must be 0 (such a blob cannot be told apart by its bytes); after one, it counts
	// blobs that run wrote and that the previous colbin happens to misread.
	Ambiguous int
	// Failures names each item that could not be carried over, with its pk/sk and the cause.
	// The rest of the entity is still processed.
	Failures []string
}

// ReprocessBlobs re-encodes every colbin "d" blob of this entity whose bytes differ from what the
// current colbin writes. decodeLegacy decodes a blob written by the previous colbin into a *E.
// With write false it is a dry run: it decodes and counts, and writes nothing.
func (r *Repo[T, E]) ReprocessBlobs(decodeLegacy func(data []byte, dst any) error, write bool) (ReprocessReport, error) {
	report := ReprocessReport{}
	client, err := Client()
	if err != nil {
		return report, err
	}

	lowestPK, highestPK := r.meta.partitionRange(0)
	lowestArrayPK, highestArrayPK := r.meta.partitionRange(arrayIndexColumnIDDigits)
	input := &dynamodb.ScanInput{
		TableName:                aws.String(tableName()),
		FilterExpression:         aws.String("(#pk BETWEEN :lo AND :hi OR #pk BETWEEN :arrayLo AND :arrayHi) AND attribute_exists(#d)"),
		ProjectionExpression:     aws.String("#pk, #sk, #d"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk", "#sk": "sk", "#d": dataColumn},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lo":      &types.AttributeValueMemberN{Value: lowestPK},
			":hi":      &types.AttributeValueMemberN{Value: highestPK},
			":arrayLo": &types.AttributeValueMemberN{Value: lowestArrayPK},
			":arrayHi": &types.AttributeValueMemberN{Value: highestArrayPK},
		},
	}

	type pendingRewrite struct {
		item                      map[string]types.AttributeValue
		itemName                  string
		storedBlob, reencodedBlob []byte
	}
	for {
		res, err := client.Scan(context.Background(), input)
		if err != nil {
			return report, err
		}
		var pageRewrites []pendingRewrite
		for _, item := range res.Items {
			storedBlob, isBinary := item[dataColumn].(*types.AttributeValueMemberB)
			if !isBinary {
				continue
			}
			report.Scanned++
			itemName := fmt.Sprintf("pk=%s sk=%s", attrText(item["pk"]), attrText(item["sk"]))

			reencodedBlob, err := r.meta.reencodeBlob(storedBlob.Value, decodeLegacy)
			if errors.Is(err, errAmbiguousBlob) {
				report.Ambiguous++
				continue
			}
			if err != nil {
				report.Failures = append(report.Failures, itemName+": "+err.Error())
				continue
			}
			if reencodedBlob == nil {
				report.Current++
				continue
			}
			pageRewrites = append(pageRewrites, pendingRewrite{item, itemName, storedBlob.Value, reencodedBlob})
		}

		if !write {
			report.Rewritten += len(pageRewrites)
		} else {
			// Each UpdateItem touches its own item, so a page's writes run in parallel: one at a
			// time is a network round trip per record.
			wasWrittenConcurrently := make([]bool, len(pageRewrites))
			err := parallel.Run(len(pageRewrites), func(rewriteIndex int) error {
				rewrite := pageRewrites[rewriteIndex]
				err := writeReencodedBlob(client, rewrite.item, rewrite.storedBlob, rewrite.reencodedBlob)
				var conditionFailed *types.ConditionalCheckFailedException
				if errors.As(err, &conditionFailed) {
					wasWrittenConcurrently[rewriteIndex] = true
					return nil
				}
				return err
			})
			if err != nil {
				return report, err
			}
			for rewriteIndex, rewrite := range pageRewrites {
				if wasWrittenConcurrently[rewriteIndex] {
					report.Failures = append(report.Failures, rewrite.itemName+": written while reprocessing, run it again")
					continue
				}
				report.Rewritten++
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			return report, nil
		}
		input.ExclusiveStartKey = res.LastEvaluatedKey
	}
}

var errAmbiguousBlob = errors.New("both colbin versions read it, as different records")

// reencodeBlob returns storedBlob encoded by the current colbin, or nil when it already is.
// The re-encoded blob must decode back to the very record the legacy decoder read: a blob that
// does not round-trip is reported, never written.
func (m *tableMeta) reencodeBlob(storedBlob []byte, decodeLegacy func(data []byte, dst any) error) ([]byte, error) {
	currentRecord := reflect.New(m.recordType).Interface()
	legacyRecord := reflect.New(m.recordType).Interface()
	legacyErr := decodeLegacy(storedBlob, legacyRecord)
	if colbin.Unmarshal(storedBlob, currentRecord) == nil {
		if currentBlob, err := colbin.Marshal(currentRecord); err == nil && bytes.Equal(currentBlob, storedBlob) {
			// Bytes both versions accept are current only when both read the same record; a blob
			// that means something else to each one cannot be told apart by its bytes, so it is
			// reported instead of guessed.
			if legacyErr != nil || reflect.DeepEqual(legacyRecord, currentRecord) {
				return nil, nil
			}
			return nil, errAmbiguousBlob
		}
	}

	if legacyErr != nil {
		return nil, fmt.Errorf("legacy decode: %w", legacyErr)
	}
	reencodedBlob, err := colbin.Marshal(legacyRecord)
	if err != nil {
		return nil, fmt.Errorf("colbin marshaling: %w", err)
	}
	if bytes.Equal(reencodedBlob, storedBlob) {
		return nil, nil
	}
	decodedBack := reflect.New(m.recordType).Interface()
	if err := colbin.Unmarshal(reencodedBlob, decodedBack); err != nil {
		return nil, fmt.Errorf("the re-encoded blob does not decode: %w", err)
	}
	if !reflect.DeepEqual(decodedBack, legacyRecord) {
		return nil, fmt.Errorf("the re-encoded blob decodes to a different record")
	}
	return reencodedBlob, nil
}

// writeReencodedBlob SETs only "d", and only while the stored blob is still the one read.
func writeReencodedBlob(client *dynamodb.Client, item map[string]types.AttributeValue, storedBlob, reencodedBlob []byte) error {
	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName()),
		Key:                      map[string]types.AttributeValue{"pk": item["pk"], "sk": item["sk"]},
		UpdateExpression:         aws.String("SET #d = :reencoded"),
		ConditionExpression:      aws.String("#d = :stored"),
		ExpressionAttributeNames: map[string]string{"#d": dataColumn},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":reencoded": &types.AttributeValueMemberB{Value: reencodedBlob},
			":stored":    &types.AttributeValueMemberB{Value: storedBlob},
		},
	})
	return err
}

// attrText renders a pk/sk attribute for a failure line.
func attrText(attr types.AttributeValue) string {
	switch value := attr.(type) {
	case *types.AttributeValueMemberN:
		return value.Value
	case *types.AttributeValueMemberS:
		return value.Value
	}
	return "?"
}
