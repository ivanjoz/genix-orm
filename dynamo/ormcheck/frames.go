package ormcheck

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/dynamo"
	"github.com/ivanjoz/genix-orm/dataframe"
)

// CheckFrameLine is a sale line with two DataFrames: quantities and line counts per day and product, and
// quantities and amounts per day, client and product.
type CheckFrameLine struct {
	Fecha          int16 `cb:"1"`
	ID             int32 `cb:"2"`
	ProductID      int32 `cb:"3"`
	ClientID       int32 `cb:"4"`
	Quantity       int32 `cb:"5"`
	Amount         int32 `cb:"6"`
	Status         int8  `cb:"7"`
	Updated        int64 `cb:"8"`
	CreatedVersion int64 `cb:"9"`
}

type CheckFrameLineTable struct {
	dynamo.Model[CheckFrameLineTable, CheckFrameLine]
	Fecha          dynamo.Col[CheckFrameLineTable, int16]
	ID             dynamo.Col[CheckFrameLineTable, int32]
	ProductID      dynamo.Col[CheckFrameLineTable, int32]
	ClientID       dynamo.Col[CheckFrameLineTable, int32]
	Quantity       dynamo.Col[CheckFrameLineTable, int32]
	Amount         dynamo.Col[CheckFrameLineTable, int32]
	Status         dynamo.Col[CheckFrameLineTable, int8]
	Updated        dynamo.Col[CheckFrameLineTable, int64]
	CreatedVersion dynamo.Col[CheckFrameLineTable, int64]
}

const (
	checkFrameDayProduct       = "day-product"
	checkFrameDayClientProduct = "day-client-product"
)

func (table CheckFrameLineTable) GetSchema() dynamo.Schema {
	return dynamo.Schema{
		Name:    "ORM check: frame lines",
		Entity:  "ormcheck_frame_line",
		Keys:    dynamo.Cols(table.Fecha.Size(16), table.ID.Size(32)),
		Indexes: []dynamo.Index{{Type: dynamo.TypeDelta, Keys: dynamo.Cols(table.Status)}},
		DataFrames: []dynamo.DataFrame{
			{Name: checkFrameDayProduct, Keys: dynamo.Cols(table.Fecha), Rows: table.ProductID, Sums: dynamo.Cols(table.Quantity), Count: true},
			{Name: checkFrameDayClientProduct, Keys: dynamo.Cols(table.Fecha, table.ClientID), Rows: table.ProductID, Sums: dynamo.Cols(table.Quantity, table.Amount)},
		},
	}
}

var CheckFrameLines = dynamo.NewRepo[CheckFrameLineTable, CheckFrameLine]()

const checkFrameDay int16 = 20_000

// runDataFrameChecks writes three lines, builds both frames, changes the lines and checks the frames
// follow (and that a Fresh read sees the changes before any run), proves a range rebuild finds nothing
// to fix, then has a Fresh read of 101 new lines write them into the files (an express compaction).
// The files live in an in-memory store, and the check moves the ORM's clock a minute forward where a
// checkpoint must settle (25 s with the 10 s write deadline) instead of waiting for it.
func runDataFrameChecks(runner *checkRunner) error {
	frameStore := dataframe.NewMemoryStore()
	dataframe.Configure(frameStore, 10*time.Second)
	clockOffset := time.Duration(0)
	realNow := dynamo.Now
	dynamo.Now = func() time.Time { return realNow().Add(clockOffset) }
	defer func() { dynamo.Now = realNow }()
	passSettle := func() { clockOffset += time.Minute }
	lines := CheckFrameLines.T
	materialize := func(name string) error {
		return runner.write(name, CheckFrameLines.MaterializeDataFrames)
	}

	runner.section("DataFrames: group-by files kept up to date by runs (in-memory store, the clock moved past each settle)")
	firstLines := []CheckFrameLine{
		{Fecha: checkFrameDay, ID: 1, ProductID: 10, ClientID: 5, Quantity: 2_000, Amount: 300, Status: 1},
		{Fecha: checkFrameDay, ID: 2, ProductID: 10, ClientID: 6, Quantity: 1_000, Amount: 150, Status: 1},
		{Fecha: checkFrameDay + 1, ID: 3, ProductID: 11, ClientID: 5, Quantity: 3_000, Amount: 900, Status: 1},
	}
	if err := runner.write("PutMany 3 lines: no stored version, so no log entry", func() error { return CheckFrameLines.PutMany(firstLines) }); err != nil {
		return err
	}
	if err := materialize("Run 1: a new frame only records a checkpoint, the Updated clock now"); err != nil {
		return err
	}
	runner.expectOnce("QueryFrame before the first build", []string{"not built"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	passSettle()
	if err := materialize("Run 2, the checkpoint settled: builds every file at it"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha = 20000", []string{"20000/10 q3000 n2"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	runner.expect("day-product: Fecha BETWEEN 20000 AND 20001", []string{"20000/10 q3000 n2", "20001/11 q3000 n1"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expect("day-client-product: Fecha = 20000, every client (_idx)", []string{"20000/5/10 q2000 a300", "20000/6/10 q1000 a150"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay)))
	runner.expect("day-client-product: ClientID = 5, Fecha BETWEEN 20000 AND 20001", []string{"20000/5/10 q2000 a300", "20001/5/11 q3000 a900"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.ClientID, int32(5)).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expectRejected("QueryFrame: no Fecha", queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.ProductID, int32(10))))

	firstLines[0].Quantity = 5_000
	firstLines[1].Status = 0
	if err := runner.write("PutMany: line 1 Quantity 2000 -> 5000, line 2 Status 0 (2 log entries per frame)", func() error { return CheckFrameLines.PutMany(firstLines[:2]) }); err != nil {
		return err
	}
	if err := runner.write("Delete line 3: its log entries take the Updated it stamps", func() error { return CheckFrameLines.Delete(&firstLines[2]) }); err != nil {
		return err
	}
	passSettle()
	if err := materialize("Run 3: checkpoints after the changes; its target is still the one before them"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha = 20000, not changed yet", []string{"20000/10 q3000 n2"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	runner.expect("Fresh day-product: Fecha BETWEEN 20000 AND 20001, the files plus the changes", []string{"20000/10 q5000 n1"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1).Fresh()))
	runner.expect("Fresh day-client-product: Fecha = 20000, every client", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay).Fresh()))
	// Run 4 targets the checkpoint run 3 read, so this insert stays out of the files.
	newClientLine := CheckFrameLine{Fecha: checkFrameDay, ID: 4, ProductID: 12, ClientID: 7, Quantity: 500, Amount: 50, Status: 1}
	if err := runner.write("Put line 4: a client the day's _idx doesn't list", func() error { return CheckFrameLines.Put(&newClientLine) }); err != nil {
		return err
	}
	runner.expect("Fresh day-client-product: Fecha = 20000, the new client's file read too", []string{"20000/5/10 q5000 a300", "20000/7/12 q500 a50"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay).Fresh()))
	// FrameSQL's read: the same fresh files, filtered by the caller before any GET and handed over as decoded.
	runner.expect("FrameSource day-client-product: Fecha = 20000, selectFiles keeps client 7", []string{"20000/7/12 q500 a50"}, func() ([]string, error) {
		labels := []string{}
		_, err := CheckFrameLines.FrameSource(checkFrameDayClientProduct).Scan(int64(checkFrameDay), int64(checkFrameDay), nil,
			func(fileKeys [][dataframe.MaxKeys]int64) ([][dataframe.MaxKeys]int64, error) {
				return slices.DeleteFunc(fileKeys, func(keys [dataframe.MaxKeys]int64) bool { return keys[1] != 7 }), nil
			},
			func(keys [dataframe.MaxKeys]int64, file dataframe.File) error {
				for row, productID := range file.RowIDs {
					labels = append(labels, fmt.Sprintf("%d/%d/%d q%d a%d", keys[0], keys[1], productID, file.Sums[0][row], file.Sums[1][row]))
				}
				return nil
			})
		return labels, err
	})
	passSettle()
	if err := materialize("Run 4, run 3's checkpoint settled: adds the changes"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha BETWEEN 20000 AND 20001", []string{"20000/10 q5000 n1"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expect("day-client-product: ClientID = 5, Fecha BETWEEN 20000 AND 20001", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.ClientID, int32(5)).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expect("day-client-product: Fecha = 20000, every client", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay)))

	// The files the runs kept up to date must be those a rebuild computes from the records. The rebuild
	// does delete the files the runs emptied (line 2's client, line 3's day), and rewrites their _idx;
	// it writes the frame's _lock too, taking and releasing it.
	runner.expectOnce("RebuildDataFrames day-client-product 20000..20001: files rewritten", []string{"0"}, func() ([]string, error) {
		etagsBefore, err := frameStoreETags(frameStore)
		if err != nil {
			return nil, err
		}
		if err := CheckFrameLines.RebuildDataFrames(checkFrameDayClientProduct, int64(checkFrameDay), int64(checkFrameDay)+1); err != nil {
			return nil, err
		}
		etagsAfter, err := frameStoreETags(frameStore)
		rewritten := 0
		for key, etag := range etagsAfter {
			if etagsBefore[key] != etag && !strings.HasSuffix(key, "/_idx") && !strings.HasSuffix(key, "/_lock") {
				rewritten++
			}
		}
		return []string{strconv.Itoa(rewritten)}, err
	})
	runner.expectOnce("RebuildDataFramesAll: the same rows", []string{"20000/10 q5000 n1"}, func() ([]string, error) {
		if err := CheckFrameLines.RebuildDataFramesAll(""); err != nil {
			return nil, err
		}
		return queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1))()
	})

	// An express compaction targets the frame's newest checkpoint settled when its Fresh read began, and
	// needs more than 100 records changed up to it: the first Fresh read after the inserts checkpoints
	// them, the second one, once that settled, writes them into the files. Each frame has its own
	// checkpoints, so both read day-client-product.
	expressDay := checkFrameDay + 2
	expressLines := make([]CheckFrameLine, 101)
	for i := range expressLines {
		expressLines[i] = CheckFrameLine{Fecha: expressDay, ID: int32(10 + i), ProductID: int32(100 + i), ClientID: 8, Quantity: 10, Amount: 1, Status: 1}
	}
	if err := runner.write("InsertMany 101 lines on 20002", func() error { return CheckFrameLines.InsertMany(expressLines) }); err != nil {
		return err
	}
	expressDayRows := frameRowsTotal(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, expressDay))
	expressDayFreshRows := frameRowsTotal(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, expressDay).Fresh())
	passSettle()
	runner.expectOnce("Fresh day-client-product: Fecha = 20002, checkpoints the inserts", []string{"101 rows q1010"}, expressDayFreshRows)
	runner.expectOnce("day-client-product: Fecha = 20002, not in the files yet", []string{"0 rows q0"}, expressDayRows)
	passSettle()
	runner.expectOnce("Fresh day-client-product: Fecha = 20002, writes the 102 changes into the files", []string{"101 rows q1010"}, expressDayFreshRows)
	runner.expect("day-client-product: Fecha = 20002, from the files (the _ixt lists them)", []string{"101 rows q1010"}, expressDayRows)
	runner.expect("day-client-product: Fecha = 20000, line 4 listed by the _ixt", []string{"20000/5/10 q5000 a300", "20000/7/12 q500 a50"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay)))
	if err := materialize("Run 5: merges the _ixt of the days the express compaction appended to"); err != nil {
		return err
	}
	runner.expect("day-client-product: Fecha BETWEEN 20000 AND 20002, after the merge", []string{"20000/5/10 q5000 a300", "20000/7/12 q500 a50", "101 rows on 20002"},
		func() ([]string, error) {
			labels, err := queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Between(lines.Fecha, checkFrameDay, expressDay))()
			expressDayLabels := slices.DeleteFunc(slices.Clone(labels), func(label string) bool { return !strings.HasPrefix(label, "20002/") })
			labels = slices.DeleteFunc(labels, func(label string) bool { return strings.HasPrefix(label, "20002/") })
			return append(labels, fmt.Sprintf("%d rows on 20002", len(expressDayLabels))), err
		})
	return nil
}

// frameRowsTotal summarizes a read of many rows: how many, and their Quantity total.
func frameRowsTotal(query *dynamo.FrameQuery[CheckFrameLine]) func() ([]string, error) {
	return func() ([]string, error) {
		rows, err := query.Exec()
		quantityTotal := int64(0)
		for _, row := range rows {
			quantityTotal += row.Sum(CheckFrameLines.T.Quantity)
		}
		return []string{fmt.Sprintf("%d rows q%d", len(rows), quantityTotal)}, err
	}
}

// queryFrame labels each row by its Keys, Rows and sums (n is day-product's count); "not built" for
// dataframe.ErrNotBuilt.
func queryFrame(query *dynamo.FrameQuery[CheckFrameLine]) func() ([]string, error) {
	lines := CheckFrameLines.T
	return func() ([]string, error) {
		rows, err := query.Exec()
		if errors.Is(err, dataframe.ErrNotBuilt) {
			return []string{"not built"}, nil
		}
		labels := make([]string, len(rows))
		for i, row := range rows {
			if row.Key.ClientID == 0 {
				labels[i] = fmt.Sprintf("%d/%d q%d n%d", row.Key.Fecha, row.Key.ProductID, row.Sum(lines.Quantity), row.Count())
				continue
			}
			labels[i] = fmt.Sprintf("%d/%d/%d q%d a%d", row.Key.Fecha, row.Key.ClientID, row.Key.ProductID, row.Sum(lines.Quantity), row.Sum(lines.Amount))
		}
		return labels, err
	}
}

func frameStoreETags(store *dataframe.MemoryStore) (map[string]string, error) {
	keys, err := store.List("")
	if err != nil {
		return nil, err
	}
	etags := map[string]string{}
	for _, key := range slices.Sorted(slices.Values(keys)) {
		if _, etag, err := store.Get(key); err == nil {
			etags[key] = etag
		}
	}
	return etags, nil
}
