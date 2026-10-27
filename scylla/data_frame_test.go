package scylla

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
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
	TagIDs    []int32 `db:"tag_ids"`
}

// frameTagLine is one line of the "sales-by-tag" frame: the sale's quantity once per tag.
type frameTagLine struct {
	Year     int16 `frame:"key"`
	TagID    int32 `frame:"row"`
	Quantity int32 `frame:"sum"`
}

func (e *frameSaleRecord) toTagLines() []frameTagLine {
	if e.Status == 0 {
		return nil
	}
	lines := []frameTagLine{}
	for _, tagID := range e.TagIDs {
		lines = append(lines, frameTagLine{Year: e.Year, TagID: tagID, Quantity: e.Quantity})
	}
	return lines
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
	TagIDs    ColSlice[*frameSaleSchema, int32]
}

func (e frameSaleSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10202,
		Name:      "frame_sales",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		DataFrames: []DataFrame{{
			Name: "sales-by-year", Keys: Cols(e.Year), Rows: Cols(e.ProductID), Sums: Cols(e.Quantity, e.Amount), Count: true,
		}, {
			Name: "sales-by-tag", ToFrames: db.ToFrames((*frameSaleRecord).toTagLines), Version: 1, UseZeroSumsMask: true,
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
	if info == nil || info.logTableName != "frame_sales__dtf_log" || info.recordType != reflect.TypeFor[frameSaleRecord]() || len(info.storedColumns) != 9 {
		t.Fatalf("compiled frames: %+v", info)
	}
	frame := info.frames.FrameNamed("sales-by-year")
	if frame == nil || frame.Folder != "dtf/"+utils.EncodeUintBase64(10202)+"/sales-by-year/" || !frame.Sums[1].IsFloat || frame.IsProjected {
		t.Fatalf("frame: %+v", frame)
	}
	if tags := info.frames.FrameNamed("sales-by-tag"); tags == nil || !tags.IsProjected || !tags.UsesZeroSumsMask || tags.Rows[0].FieldName != "TagID" {
		t.Fatalf("projected frame: %+v", tags)
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
		// The record's Year column selects the "Year" key of the ToFrames frame.
		{db.QueryFrame[frameSaleRecord](1, "sales-by-tag").Between(columns.Year, 2025, 2026), "no file store"},
		{db.QueryFrame[frameSaleRecord](1, "sales-by-tag").Equals(columns.ProductID, 3), "needs an Equals or a Between on Year"},
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
	configureDataFrames(&scyllaTable, schema, reflect.ValueOf(columns).Elem())
}

// TestDataFrameLinesOfAStoredRow: a stored row, as the write path and the Histories read it, decodes
// into the record its frames take lines from (the list column included), and a partial write (only
// the status) merged over it leaves the record's other fields, so its lines go from the stored ones to
// none.
func TestDataFrameLinesOfAStoredRow(t *testing.T) {
	info := compiledFrameSaleTable(t).dataFrames
	storedRow := map[string]any{"company_id": int32(1), "id": int32(9), "year": int16(2026), "product_id": int32(7),
		"quantity": int32(3), "amount": 1.5, "status": int8(1), "tag_ids": []int32{4, 5}}
	storedLines, err := info.linesOfRow(storedRow)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(storedLines[0], []dataframe.Line{{2026, 7, 3, 1_500_000}}) || !reflect.DeepEqual(storedLines[1], []dataframe.Line{{2026, 4, 3}, {2026, 5, 3}}) {
		t.Fatalf("stored lines: %v", storedLines)
	}
	scyllaTable := compiledFrameSaleTable(t)
	annul := frameSaleRecord{Status: 0}
	writtenRecord := info.newRecord(storedRow)
	copyColumnField(scyllaTable.ColumnsMap["status"], writtenRecord, unsafe.Pointer(&annul))
	if merged := (*frameSaleRecord)(writtenRecord); merged.Year != 2026 || merged.Status != 0 || len(merged.TagIDs) != 2 {
		t.Fatalf("merged record: %+v", merged)
	}
	writtenLines, _ := info.frames.LinesOf(writtenRecord)
	if writtenLines[0] != nil || writtenLines[1] != nil {
		t.Fatalf("an annulled sale puts no lines, got %v", writtenLines)
	}
	if body := info.frames.EncodeChangedLines(storedLines, writtenLines); body == nil {
		t.Fatal("the annulment must be logged")
	}
}

// TestDataFrameLogChangesDecode: an insert, a change with its body, a row of an older format
// (skipped) and a truncated one.
func TestDataFrameLogChangesDecode(t *testing.T) {
	insert, isKept, err := decodeLogChange(40, []byte{dataFrameLogFormat, dataFrameLogInsertFlag})
	if err != nil || !isKept || !insert.IsInsert || insert.Moment != 40 {
		t.Fatalf("insert: %+v %v", insert, err)
	}
	change, isKept, err := decodeLogChange(41, []byte{dataFrameLogFormat, 0, 1, 2, 3})
	if err != nil || !isKept || change.IsInsert || !reflect.DeepEqual(change.Body, []byte{1, 2, 3}) {
		t.Fatalf("change: %+v %v", change, err)
	}
	if _, isKept, err := decodeLogChange(41, []byte{1, 0, 1, 2}); err != nil || isKept {
		t.Fatal("a row of the column format must be skipped")
	}
	if _, _, err := decodeLogChange(41, []byte{dataFrameLogFormat}); err == nil {
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
