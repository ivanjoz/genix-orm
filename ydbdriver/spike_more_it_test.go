package ydbdriver

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const spikeItemsDDL = "CREATE TABLE `%s` (company_id Int32 NOT NULL, id Int64 NOT NULL, updated Int32, name Utf8, extra Utf8, " +
	"PRIMARY KEY (company_id, id), INDEX ix_delta GLOBAL ASYNC ON (company_id, updated) COVER (name))"

func spikeCreateItems(t *testing.T, connection *ydb.Driver) string {
	directory := spikeDirectory(t, connection)
	tablePath := directory + "/items"
	spikeMustExec(t, connection, fmt.Sprintf(spikeItemsDDL, tablePath))
	return tablePath
}

// spikeListParameter wraps struct values as one List<Struct<...>> parameter (plan §6: bulk input travels as AS_TABLE).
func spikeListParameter(name string, rows []types.Value) query.ExecuteOption {
	return query.WithParameters(ydb.ParamsBuilder().Param(name).Any(types.ListValue(rows...)).Build())
}

func spikeCount(connection *ydb.Driver, statement string, txControl *query.TransactionControl) int64 {
	counts, err := spikeInt64Rows(connection, statement, txControl)
	if err != nil || len(counts) != 1 {
		return -1
	}
	return counts[0]
}

func spikeDescribe(t *testing.T, connection *ydb.Driver, relativePath string) options.Description {
	t.Helper()
	describeContext, cancel := spikeContext()
	defer cancel()
	var description options.Description
	err := connection.Table().Do(describeContext, func(ctx context.Context, session table.Session) (err error) {
		description, err = session.DescribeTable(ctx, connection.Name()+"/"+relativePath, options.WithTableStats(), options.WithPartitionStats(), options.WithShardKeyBounds())
		return err
	}, table.WithIdempotent())
	if err != nil {
		t.Fatalf("describe %s: %v", relativePath, err)
	}
	return description
}

// Item 4: UPSERT of a column subset leaves the other columns untouched and creates missing rows.
func TestSpikeUpsertSubsetColumns(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)
	spikeMustExec(t, connection, "UPSERT INTO `"+tablePath+"` (company_id, id, updated, name, extra) VALUES (1, 1, 10, 'a', 'keep-me')")
	spikeMustExec(t, connection, "UPSERT INTO `"+tablePath+"` (company_id, id, updated) VALUES (1, 1, 99), (1, 2, 50)")
	untouched := spikeCount(connection, "SELECT CAST(COUNT(*) AS Int64) FROM `"+tablePath+"` WHERE company_id = 1 AND id = 1 AND extra = 'keep-me' AND name = 'a' AND updated = 99", nil)
	created := spikeCount(connection, "SELECT CAST(COUNT(*) AS Int64) FROM `"+tablePath+"` WHERE company_id = 1 AND id = 2 AND updated = 50 AND name IS NULL", nil)
	spikeLog(t, "item4 UPSERT subset: existing row keeps other columns=%d, missing row created with NULLs=%d", untouched, created)
	if untouched != 1 || created != 1 {
		t.Fatalf("UPSERT subset semantics differ from the plan")
	}
}

// Item 5: AS_TABLE($keys) INNER JOIN on the full primary key is a lookup join.
func TestSpikeLookupJoin(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)
	spikeMustExec(t, connection, "UPSERT INTO `"+tablePath+"` (company_id, id, updated, name) VALUES (1, 1, 10, 'a'), (1, 2, 20, 'b'), (1, 3, 30, 'c')")
	statement := "DECLARE $keys AS List<Struct<company_id: Int32, id: Int64>>;\n" +
		"SELECT t.id FROM AS_TABLE($keys) AS k INNER JOIN `" + tablePath + "` AS t ON t.company_id = k.company_id AND t.id = k.id"
	plan := spikePlan(t, connection, statement)
	spikeLog(t, "item5 AS_TABLE join plan: LookupJoin/Lookup=%v MapJoin=%v\n%s", strings.Contains(plan, "Lookup"), strings.Contains(plan, "MapJoin"), plan)
	keyRows := []types.Value{}
	for _, id := range []int64{1, 3} {
		keyRows = append(keyRows, types.StructValue(types.StructFieldValue("company_id", types.Int32Value(1)), types.StructFieldValue("id", types.Int64Value(id))))
	}
	rows, err := spikeInt64RowsWith(connection, statement, spikeListParameter("$keys", keyRows))
	spikeLog(t, "item5 AS_TABLE join result rows=%v err=%v", rows, err)
}

func spikeInt64RowsWith(connection *ydb.Driver, statement string, parameters query.ExecuteOption) ([]int64, error) {
	statementContext, cancel := spikeContext()
	defer cancel()
	result, err := connection.Query().Query(statementContext, statement, parameters, query.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer result.Close(statementContext)
	values := []int64{}
	for resultSet, setErr := range result.ResultSets(statementContext) {
		if setErr != nil {
			return nil, setErr
		}
		for row, rowErr := range resultSet.Rows(statementContext) {
			if rowErr != nil {
				return nil, rowErr
			}
			var value int64
			if err := row.Scan(&value); err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

// Item 6: BulkUpsert on a table with an async index updates the index?
func TestSpikeBulkUpsertWithAsyncIndex(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)
	rows := []types.Value{}
	for id := int64(1); id <= 5; id++ {
		rows = append(rows, types.StructValue(
			types.StructFieldValue("company_id", types.Int32Value(1)),
			types.StructFieldValue("id", types.Int64Value(id)),
			types.StructFieldValue("updated", types.OptionalValue(types.Int32Value(int32(id)*10))),
			types.StructFieldValue("name", types.OptionalValue(types.TextValue("n")))))
	}
	bulkContext, cancel := spikeContext()
	defer cancel()
	err := connection.Table().BulkUpsert(bulkContext, connection.Name()+"/"+tablePath, table.BulkUpsertDataRows(types.ListValue(rows...)))
	if err != nil {
		spikeLog(t, "item6 BulkUpsert on table with async index: rejected: %v", err)
		return
	}
	startedAt := time.Now()
	var indexed int64
	for time.Since(startedAt) < 30*time.Second {
		indexed = spikeCount(connection, "SELECT CAST(COUNT(*) AS Int64) FROM `"+tablePath+"` VIEW ix_delta WHERE company_id = 1", query.StaleReadOnlyTxControl())
		if indexed == 5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	spikeLog(t, "item6 BulkUpsert on table with async index: accepted, index rows=%d after %v", indexed, time.Since(startedAt))
}

// Item 7: BATCH DELETE / BATCH UPDATE on a table with async indexes.
func TestSpikeBatchStatementsWithAsyncIndex(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)
	spikeMustExec(t, connection, "UPSERT INTO `"+tablePath+"` (company_id, id, updated, name) VALUES (1, 1, 10, 'a'), (1, 2, 20, 'b'), (2, 1, 10, 'c')")
	updateErr := spikeExec(t, connection, "BATCH UPDATE `"+tablePath+"` SET extra = name WHERE company_id = 1")
	spikeLog(t, "item7 BATCH UPDATE: err=%v", updateErr)
	deleteErr := spikeExec(t, connection, "BATCH DELETE FROM `"+tablePath+"` WHERE company_id = 1")
	spikeLog(t, "item7 BATCH DELETE: err=%v", deleteErr)
	remaining := spikeCount(connection, "SELECT CAST(COUNT(*) AS Int64) FROM `"+tablePath+"`", nil)
	spikeLog(t, "item7 rows after BATCH DELETE of company 1: %d (expected 1)", remaining)
}

// Item 8: transaction size, 1k / 5k / 20k row writes in one transaction.
func TestSpikeTransactionSize(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)
	nextID := int64(1)
	for _, rowCount := range []int{1000, 5000, 20000} {
		sizeRows := make([]types.Value, 0, rowCount)
		for rowIndex := 0; rowIndex < rowCount; rowIndex++ {
			sizeRows = append(sizeRows, types.StructValue(
				types.StructFieldValue("company_id", types.Int32Value(1)), types.StructFieldValue("id", types.Int64Value(nextID)),
				types.StructFieldValue("updated", types.OptionalValue(types.Int32Value(int32(rowIndex)))),
				types.StructFieldValue("name", types.OptionalValue(types.TextValue("row-name-for-size-test")))))
			nextID++
		}
		parameters := spikeListParameter("$rows", sizeRows)
		statement := "DECLARE $rows AS List<Struct<company_id: Int32, id: Int64, updated: Int32?, name: Utf8?>>;\n" +
			"UPSERT INTO `" + tablePath + "` SELECT company_id, id, updated, name FROM AS_TABLE($rows)"
		statementContext, cancel := spikeContext()
		startedAt := time.Now()
		err := connection.Query().DoTx(statementContext, func(ctx context.Context, tx query.TxActor) error {
			return tx.Exec(ctx, statement, parameters)
		})
		cancel()
		spikeLog(t, "item8 one tx, %d rows: %v err=%v", rowCount, time.Since(startedAt).Round(time.Millisecond), err)
	}
}

// Item 9: TTL on Uint32 ... AS SECONDS.
func TestSpikeTimeToLive(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	err := spikeExec(t, connection, "CREATE TABLE `"+directory+"/ttl_log` (id Int64 NOT NULL, expires_at Uint32, PRIMARY KEY (id)) WITH (TTL = Interval(\"PT0S\") ON expires_at AS SECONDS)")
	spikeLog(t, "item9 TTL on Uint32 AS SECONDS at CREATE: err=%v", err)
	if err != nil {
		t.Fatal(err)
	}
	description := spikeDescribe(t, connection, directory+"/ttl_log")
	spikeLog(t, "item9 DescribeTable TTL settings: %+v", description.TimeToLiveSettings)
}

// Item 10: lz4 family at create, ALTER FAMILY at homologation, DescribeTable reports it.
func TestSpikeCompressionFamily(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	spikeMustExec(t, connection, "CREATE TABLE `"+directory+"/lz` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id), FAMILY default (COMPRESSION = \"lz4\"))")
	spikeMustExec(t, connection, "CREATE TABLE `"+directory+"/plain` (id Int64 NOT NULL, value Utf8, PRIMARY KEY (id))")
	plainBefore := spikeDescribe(t, connection, directory+"/plain").ColumnFamilies
	spikeLog(t, "item10 default table families (no clause): %+v", plainBefore)
	spikeLog(t, "item10 lz4 table families at CREATE: %+v", spikeDescribe(t, connection, directory+"/lz").ColumnFamilies)
	alterErr := spikeExec(t, connection, "ALTER TABLE `"+directory+"/plain` ALTER FAMILY default SET COMPRESSION \"lz4\"")
	spikeLog(t, "item10 ALTER FAMILY default SET COMPRESSION lz4: err=%v", alterErr)
	spikeLog(t, "item10 families after ALTER: %+v", spikeDescribe(t, connection, directory+"/plain").ColumnFamilies)
}

// Item 11 (Y20): partitioning settings at CREATE, ALTER INDEX SET, DescribeTable reads them back.
func TestSpikePartitioningSettings(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	tablePath := directory + "/parted"
	settings := "AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50"
	spikeMustExec(t, connection, "CREATE TABLE `"+tablePath+"` (company_id Int32 NOT NULL, id Int64 NOT NULL, updated Int32, PRIMARY KEY (company_id, id), INDEX ix_upd GLOBAL ASYNC ON (company_id, updated)) WITH ("+settings+")")
	spikeLog(t, "item11 table PartitioningSettings at CREATE: %+v", spikeDescribe(t, connection, tablePath).PartitioningSettings)
	spikeLog(t, "item11 index PartitioningSettings before ALTER: %+v", spikeDescribe(t, connection, tablePath+"/ix_upd/indexImplTable").PartitioningSettings)
	alterErr := spikeExec(t, connection, "ALTER TABLE `"+tablePath+"` ALTER INDEX ix_upd SET ("+settings+")")
	spikeLog(t, "item11 ALTER INDEX ix_upd SET (...): err=%v", alterErr)
	spikeLog(t, "item11 index PartitioningSettings after ALTER: %+v", spikeDescribe(t, connection, tablePath+"/ix_upd/indexImplTable").PartitioningSettings)
}

// Item 11b (Y20): one company, increasing IDs, full speed; how long until load splitting splits the hot partition.
func TestSpikeHotPartitionSplit(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	tablePath := directory + "/hot"
	settings := "AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50"
	spikeMustExec(t, connection, "CREATE TABLE `"+tablePath+"` (company_id Int32 NOT NULL, id Int64 NOT NULL, payload Utf8, PRIMARY KEY (company_id, id)) WITH ("+settings+")")
	deadline := time.Now().Add(6 * time.Minute)
	startedAt := time.Now()
	var nextID int64
	var idMutex sync.Mutex
	var written int64
	var writeGroup sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		writeGroup.Add(1)
		go func() {
			defer writeGroup.Done()
			for time.Now().Before(deadline) {
				hotRows := make([]types.Value, 0, 200)
				idMutex.Lock()
				firstID := nextID
				nextID += 200
				idMutex.Unlock()
				for offset := int64(0); offset < 200; offset++ {
					hotRows = append(hotRows, types.StructValue(types.StructFieldValue("company_id", types.Int32Value(1)), types.StructFieldValue("id", types.Int64Value(firstID+offset)),
						types.StructFieldValue("payload", types.OptionalValue(types.TextValue(strings.Repeat("x", 200))))))
				}
				statementContext, cancel := spikeContext()
				err := connection.Query().Exec(statementContext, "DECLARE $rows AS List<Struct<company_id: Int32, id: Int64, payload: Utf8?>>;\nUPSERT INTO `"+tablePath+"` SELECT company_id, id, payload FROM AS_TABLE($rows)", spikeListParameter("$rows", hotRows))
				cancel()
				if err == nil {
					idMutex.Lock()
					written += 200
					idMutex.Unlock()
				}
			}
		}()
	}
	pollDone := make(chan struct{})
	go func() {
		writeGroup.Wait()
		close(pollDone)
	}()
	lastPartitions := uint64(0)
	for ticker := time.NewTicker(10 * time.Second); ; {
		select {
		case <-ticker.C:
			stats := spikeDescribe(t, connection, tablePath).Stats
			if stats != nil && stats.Partitions != lastPartitions {
				lastPartitions = stats.Partitions
				spikeLog(t, "item11b t=%v partitions=%d rows_written=%d", time.Since(startedAt).Round(time.Second), lastPartitions, written)
			}
		case <-pollDone:
			spikeLog(t, "item11b finished: partitions=%d rows_written=%d in %v", lastPartitions, written, time.Since(startedAt).Round(time.Second))
			return
		}
	}
}

func percentile(sortedValues []time.Duration, fraction float64) time.Duration {
	if len(sortedValues) == 0 {
		return 0
	}
	index := int(float64(len(sortedValues)-1) * fraction)
	return sortedValues[index]
}

// Async index lag: write at concurrency 1 and 64, poll the index until each row appears.
func TestSpikeAsyncIndexLag(t *testing.T) {
	connection := requireYdbIntegration(t)
	for _, concurrency := range []int{1, 64} {
		tablePath := spikeCreateItems(t, connection)
		rowsPerWorker := 300
		if concurrency == 1 {
			rowsPerWorker = 600
		}
		lags := []time.Duration{}
		var lagMutex sync.Mutex
		var workerGroup sync.WaitGroup
		for worker := 0; worker < concurrency; worker++ {
			workerGroup.Add(1)
			go func(worker int) {
				defer workerGroup.Done()
				for rowIndex := 0; rowIndex < rowsPerWorker; rowIndex++ {
					rowID := int64(worker*100000 + rowIndex)
					writeContext, cancel := spikeContext()
					err := connection.Query().Exec(writeContext, fmt.Sprintf("UPSERT INTO `%s` (company_id, id, updated, name) VALUES (1, %d, %d, 'x')", tablePath, rowID, rowID+1))
					cancel()
					if err != nil {
						continue
					}
					writtenAt := time.Now()
					for time.Since(writtenAt) < 60*time.Second {
						found := spikeCount(connection, fmt.Sprintf("SELECT CAST(COUNT(*) AS Int64) FROM `%s` VIEW ix_delta WHERE company_id = 1 AND updated = %d", tablePath, rowID+1), query.StaleReadOnlyTxControl())
						if found == 1 {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					lagMutex.Lock()
					lags = append(lags, time.Since(writtenAt))
					lagMutex.Unlock()
				}
			}(worker)
		}
		workerGroup.Wait()
		sort.Slice(lags, func(a, b int) bool { return lags[a] < lags[b] })
		spikeLog(t, "async lag (write ack -> visible via index poll, 20ms poll) concurrency=%d samples=%d p50=%v p99=%v p99.9=%v max=%v",
			concurrency, len(lags), percentile(lags, 0.5), percentile(lags, 0.99), percentile(lags, 0.999), lags[len(lags)-1])
	}
}
