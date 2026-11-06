package ydbdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// Sequences (docs/YDB_PLAN.md Y11, §12): YDB owns them. One row per counter in `<keyspace>/_sequences`,
// advanced inside a serializable transaction, so two writers can never mint the same ID. The SDK
// retries the whole callback when the optimistic lock breaks, so the callback re-reads every time.

func sequencesTablePath() string { return Keyspace() + "/_sequences" }

// GetAutoincrementID reserves recordsSize consecutive IDs and returns the first one.
func GetAutoincrementID(key string, recordsSize int) (int64, error) {
	if recordsSize <= 0 {
		return 0, fmt.Errorf("ydbdriver: GetAutoincrementID %q: recordsSize must be positive, got %d", key, recordsSize)
	}
	previousValue, err := advanceSequence(key, func(previous int64) int64 { return previous + int64(recordsSize) })
	if err != nil {
		return 0, fmt.Errorf("ydbdriver: GetAutoincrementID %q: %w", key, err)
	}
	return previousValue + 1, nil
}

// SetCounter moves a sequence to an absolute value and returns what it held (0 when it did not exist).
func SetCounter(counterName string, value int64) (int64, error) {
	previousValue, err := advanceSequence(counterName, func(int64) int64 { return value })
	if err != nil {
		return 0, fmt.Errorf("ydbdriver: SetCounter %q: %w", counterName, err)
	}
	return previousValue, nil
}

// ListCounters returns the names of every sequence.
func ListCounters() ([]string, error) {
	connection, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	listContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	result, err := connection.Query().Query(listContext, fmt.Sprintf("SELECT name FROM `%s` ORDER BY name", sequencesTablePath()),
		query.WithTxControl(query.SnapshotReadOnlyTxControl()), query.WithIdempotent())
	if err != nil {
		return nil, fmt.Errorf("ydbdriver: ListCounters: %w", err)
	}
	defer result.Close(listContext)
	counterNames := []string{}
	for resultSet, setErr := range result.ResultSets(listContext) {
		if setErr != nil {
			return nil, fmt.Errorf("ydbdriver: ListCounters: %w", setErr)
		}
		for row, rowErr := range resultSet.Rows(listContext) {
			if rowErr != nil {
				return nil, fmt.Errorf("ydbdriver: ListCounters: %w", rowErr)
			}
			var counterName string
			if err := row.Scan(&counterName); err != nil {
				return nil, fmt.Errorf("ydbdriver: ListCounters: %w", err)
			}
			counterNames = append(counterNames, counterName)
		}
	}
	return counterNames, nil
}

// advanceSequence reads the counter, writes nextValue(previous) and returns previous, atomically.
func advanceSequence(counterName string, nextValue func(previous int64) int64) (int64, error) {
	connection, err := sharedConnection()
	if err != nil {
		return 0, err
	}
	return advanceSequenceOn(connection, sequencesTablePath(), counterName, nextValue)
}

func advanceSequenceOn(connection *ydb.Driver, sequencesPath, counterName string, nextValue func(previous int64) int64) (int64, error) {
	transactionContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	var previousValue int64
	err := connection.Query().DoTx(transactionContext, func(ctx context.Context, transaction query.TxActor) error {
		previousValue = 0
		row, err := transaction.QueryRow(ctx,
			fmt.Sprintf("DECLARE $name AS Utf8;\nSELECT value FROM `%s` WHERE name = $name", sequencesPath),
			query.WithParameters(ydb.ParamsBuilder().Param("$name").Text(counterName).Build()))
		switch {
		case err == nil:
			var storedValue *int64
			if err := row.Scan(&storedValue); err != nil {
				return err
			}
			if storedValue != nil {
				previousValue = *storedValue
			}
		case errors.Is(err, query.ErrNoRows):
		default:
			return err
		}
		return transaction.Exec(ctx,
			fmt.Sprintf("DECLARE $name AS Utf8;\nDECLARE $value AS Int64;\nUPSERT INTO `%s` (name, value) VALUES ($name, $value)", sequencesPath),
			query.WithParameters(ydb.ParamsBuilder().Param("$name").Text(counterName).Param("$value").Int64(nextValue(previousValue)).Build()))
	}, query.WithTxSettings(query.TxSettings(query.WithSerializableReadWrite())))
	return previousValue, err
}
