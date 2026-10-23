package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/utils"
)

// BackfillCreated gives crt (the record's creation, client.go) to every stored record without one:
// its Updated, 0 when the table has none. One-off, for the records written before the ORM managed
// crt; delete it once every environment ran it. It returns how many records lack one (write false
// is a dry run), each written only while it still lacks it, and the items it can't decode, left as
// they are.
func (r *repoCore) BackfillCreated(write bool) (int, []string, error) {
	client, err := Client()
	if err != nil {
		return 0, nil, err
	}
	missingCount := 0
	var failures []string
	err = scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange()}, []string{"pk", "sk", dataColumn, createdColumn}, func(items []map[string]types.AttributeValue) error {
		var lackingItems []map[string]types.AttributeValue
		var createdValues []int64
		for _, item := range items {
			if _, hasCreated := item[createdColumn]; hasCreated {
				continue
			}
			created := int64(0)
			if r.meta.updated != nil {
				recordPtr, err := r.meta.decodeRecord(item)
				if err != nil {
					failures = append(failures, itemName(item)+": "+err.Error())
					continue
				}
				created = r.meta.updated.acc.getI64(recordPtr)
			}
			lackingItems = append(lackingItems, item)
			createdValues = append(createdValues, created)
		}
		missingCount += len(lackingItems)
		if !write {
			return nil
		}
		return utils.RunParallel(len(lackingItems), func(itemIndex int) error {
			_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
				TableName:                 aws.String(tableName()),
				Key:                       itemKeyOf(lackingItems[itemIndex]),
				UpdateExpression:          aws.String("SET #crt = :crt"),
				ConditionExpression:       aws.String("attribute_exists(pk) AND attribute_not_exists(#crt)"),
				ExpressionAttributeNames:  map[string]string{"#crt": createdColumn},
				ExpressionAttributeValues: map[string]types.AttributeValue{":crt": numberAttr(createdValues[itemIndex])},
			})
			if isConditionFailed(err) {
				return nil // written meanwhile, by code that sets crt
			}
			return err
		})
	})
	return missingCount, failures, err
}
