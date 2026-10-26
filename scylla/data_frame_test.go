package scylla

import (
	"strings"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/utils"
)

type frameSaleRecord struct {
	TableStruct[frameSaleSchema, frameSaleRecord]
	CompanyID int32   `db:"company_id"`
	ID        int32   `db:"id"`
	Year      int16   `db:"year"`
	ProductID int32   `db:"product_id"`
	Quantity  int32   `db:"quantity"`
	Amount    float64 `db:"amount"`
	Note      string  `db:"note"`
	Status    int8    `db:"status"`
}

type frameSaleSchema struct {
	TableStruct[frameSaleSchema, frameSaleRecord]
	CompanyID Col[*frameSaleSchema, int32]
	ID        Col[*frameSaleSchema, int32]
	Year      Col[*frameSaleSchema, int16]
	ProductID Col[*frameSaleSchema, int32]
	Quantity  Col[*frameSaleSchema, int32]
	Amount    Col[*frameSaleSchema, float64]
	Note      Col[*frameSaleSchema, string]
	Status    Col[*frameSaleSchema, int8]
}

func (e frameSaleSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10202,
		Name:      "frame_sales",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		DataFrames: []DataFrame{{
			Name: "sales-by-year", Keys: Cols(e.Year), Rows: Cols(e.ProductID), Sums: Cols(e.Quantity, e.Amount), Count: true,
		}},
	}
}

func compiledFrameSaleTable(t *testing.T) ScyllaTable {
	t.Helper()
	resetORMTableCachesForTesting()
	return MakeScyllaTable[frameSaleRecord, frameSaleSchema]()
}

func TestDataFramesCompile(t *testing.T) {
	scyllaTable := compiledFrameSaleTable(t)
	info := scyllaTable.dataFrames
	if info == nil || info.logTableName != "frame_sales__dtf_log" || len(info.trackedColumns) != 5 {
		t.Fatalf("compiled frames: %+v", info)
	}
	trackedNames := []string{}
	for _, column := range info.trackedColumns {
		trackedNames = append(trackedNames, column.GetName())
	}
	// Tracked in column id (field position) order, the status included.
	if strings.Join(trackedNames, ",") != "year,product_id,quantity,amount,status" || info.frames.StatusIndex != 4 {
		t.Fatalf("tracked columns: %v (status at %d)", trackedNames, info.frames.StatusIndex)
	}
	frame := info.frames.FrameNamed("sales-by-year")
	if frame == nil || frame.Folder != "dtf/"+utils.EncodeUintBase64(10202)+"/sales-by-year/" || !frame.Sums[1].IsFloat {
		t.Fatalf("frame: %+v", frame)
	}
	script := dataFrameLogCreateScript(scyllaTable)
	for _, part := range []string{"frame_sales__dtf_log", "PRIMARY KEY ((half_hour), partition_id, ts, record_id)", "default_time_to_live = 172800", "TimeWindowCompactionStrategy"} {
		if !strings.Contains(script, part) {
			t.Fatalf("the log table script lacks %q:\n%s", part, script)
		}
	}
}

// TestQueryFrameResolvesBeforeReading: the typed query reaches the driver, which refuses an unknown
// frame and a selection without its first Key, and asks for the file store before any read.
func TestQueryFrameResolvesBeforeReading(t *testing.T) {
	compiledFrameSaleTable(t)
	columns := db.TableOf[frameSaleRecord]()
	for _, testCase := range []struct {
		query *db.FrameQuery[frameSaleSchema, frameSaleRecord]
		want  string
	}{
		{db.QueryFrame[frameSaleRecord](1, "nope").Equals(columns.Year, 2026), "no DataFrame"},
		{db.QueryFrame[frameSaleRecord](1, "sales-by-year").Equals(columns.ProductID, 3), "needs an Equals or a Between on year"},
		{db.QueryFrame[frameSaleRecord](1, "sales-by-year").Between(columns.Year, 2025, 2026), "no file store"},
	} {
		if _, err := testCase.query.Exec(); err == nil || !strings.Contains(err.Error(), testCase.want) {
			t.Fatalf("want an error with %q, got %v", testCase.want, err)
		}
	}
}

func TestDataFramesRefuseABadDeclaration(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "integer or a float") {
			t.Fatalf("a string Sums column compiled: %v", recovered)
		}
	}()
	scyllaTable := compiledFrameSaleTable(t)
	columns := db.InitStructTable[frameSaleSchema, frameSaleRecord](new(frameSaleSchema))
	schema := columns.GetSchema()
	schema.DataFrames[0].Sums = Cols(columns.Note)
	configureDataFrames(&scyllaTable, schema)
}

// TestDataFrameLogChangesRoundTrip: an insert, a write changing two columns (a float and a negative
// value among them) and a write changing nothing, through the blob and back.
func TestDataFrameLogChangesRoundTrip(t *testing.T) {
	info := compiledFrameSaleTable(t).dataFrames
	if changes := info.encodeLogChanges(false, make([]int64, 5), []int64{2026, 7, 3, 1_500_000, 1}); len(changes) != 2 {
		t.Fatalf("an insert logs only its flag, got %x", changes)
	}
	insert, err := info.decodeLogChange(40, []byte{dataFrameLogFormat, dataFrameLogInsertFlag})
	if err != nil || !insert.IsInsert || insert.Moment != 40 {
		t.Fatalf("insert: %+v %v", insert, err)
	}
	stored := []int64{2026, 7, 3, 1_500_000, 1}
	written := []int64{2026, 7, -4, 2_250_000, 1}
	if info.encodeLogChanges(true, stored, stored) != nil {
		t.Fatal("a write changing nothing tracked must log nothing")
	}
	change, err := info.decodeLogChange(41, info.encodeLogChanges(true, stored, written))
	if err != nil || change.IsInsert || len(change.TrackedIndexes) != 2 || change.TrackedIndexes[0] != 2 || change.Before[0] != 3 || change.TrackedIndexes[1] != 3 || change.Before[1] != 1_500_000 {
		t.Fatalf("change: %+v %v", change, err)
	}
	if _, err := info.decodeLogChange(41, []byte{dataFrameLogFormat, 0, 1}); err == nil {
		t.Fatal("a truncated log row decoded")
	}
}

func TestDataFrameLogPositionAndMoment(t *testing.T) {
	moment := time.Date(2026, 10, 25, 14, 47, 31, 123456000, time.UTC)
	halfHour, offsetMicros := dataFrameLogPosition(moment)
	if halfHour != int32(moment.Unix()/1800) || offsetMicros != int32((17*60+31)*1e6+123456) {
		t.Fatalf("position: %d %d", halfHour, offsetMicros)
	}
	if dataFrameLogMoment(halfHour, offsetMicros) != (moment.Unix()-1e9)/2 {
		t.Fatal("the moment of a log row must be its write's SUnixTime")
	}
}
