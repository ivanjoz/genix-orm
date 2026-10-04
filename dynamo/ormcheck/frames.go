package ormcheck

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ivanjoz/genix-orm/dynamo"
	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
)

// CheckFrameLine is a sale line with two DataFrames: quantities per day and product, and quantities
// and amounts per day, client and product.
type CheckFrameLine struct {
	Fecha          int16 `cb:"1"`
	ID             int32 `cb:"2"`
	ProductID      int32 `cb:"3"`
	ClientID       int32 `cb:"4"`
	Quantity       int32 `cb:"5"`
	Amount         int32 `cb:"6"`
	Status         int8  `cb:"7"`
	UpdatedVersion int32 `cb:"8"`
	CreatedVersion int32 `cb:"9"`
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
	UpdatedVersion dynamo.Col[CheckFrameLineTable, int32]
	CreatedVersion dynamo.Col[CheckFrameLineTable, int32]
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
			{Name: checkFrameDayProduct, Keys: dynamo.Cols(table.Fecha), Rows: table.ProductID, Sums: dynamo.Cols(table.Quantity)},
			{Name: checkFrameDayClientProduct, Keys: dynamo.Cols(table.Fecha, table.ClientID), Rows: table.ProductID, Sums: dynamo.Cols(table.Quantity, table.Amount)},
		},
	}
}

var CheckFrameLines = dynamo.NewRepo[CheckFrameLineTable, CheckFrameLine]()

const checkFrameDay int16 = 20_000

// runDataFrameChecks writes three lines, builds both frames, changes the lines and checks the frames
// follow (and that a Fresh read sees the changes before any run), then proves a range rebuild finds
// nothing to fix. The files live in an in-memory store and
// the settle is 0, so a write shows up two runs later instead of 10 to 20 minutes.
func runDataFrameChecks(runner *checkRunner) error {
	frameStore := dataframe.NewMemoryStore()
	dynamo.SetDataFrames(frameStore, 0)
	lines := CheckFrameLines.T
	materialize := func(name string) error {
		return runner.write(name, CheckFrameLines.MaterializeDataFrames)
	}

	runner.section("DataFrames: group-by files kept up to date by runs (in-memory store, settle 0)")
	firstLines := []CheckFrameLine{
		{Fecha: checkFrameDay, ID: 1, ProductID: 10, ClientID: 5, Quantity: 2_000, Amount: 300, Status: 1},
		{Fecha: checkFrameDay, ID: 2, ProductID: 10, ClientID: 6, Quantity: 1_000, Amount: 150, Status: 1},
		{Fecha: checkFrameDay + 1, ID: 3, ProductID: 11, ClientID: 5, Quantity: 3_000, Amount: 900, Status: 1},
	}
	if err := runner.write("PutMany 3 lines: no stored version, so no log entry", func() error { return CheckFrameLines.PutMany(firstLines) }); err != nil {
		return err
	}
	if err := materialize("Run 1: a new frame only records its target version"); err != nil {
		return err
	}
	runner.expectOnce("QueryFrame before the first build", []string{"not built"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	if err := materialize("Run 2: builds every file at that version"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha = 20000", []string{"20000/10 q3000"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	runner.expect("day-product: Fecha BETWEEN 20000 AND 20001", []string{"20000/10 q3000", "20001/11 q3000"},
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
	if err := runner.write("Delete line 3: reserves a version for its log entries", func() error { return CheckFrameLines.Delete(&firstLines[2]) }); err != nil {
		return err
	}
	if err := materialize("Run 3: its target is the version run 2 read, before the changes"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha = 20000, not changed yet", []string{"20000/10 q3000"}, queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Eq(lines.Fecha, checkFrameDay)))
	runner.expect("Fresh day-product: Fecha BETWEEN 20000 AND 20001, the files plus the changes", []string{"20000/10 q5000"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1).Fresh()))
	runner.expect("Fresh day-client-product: Fecha = 20000, every client", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay).Fresh()))
	// Run 4 targets the version run 3 read, so this insert stays out of the files until run 5.
	newClientLine := CheckFrameLine{Fecha: checkFrameDay, ID: 4, ProductID: 12, ClientID: 7, Quantity: 500, Amount: 50, Status: 1}
	if err := runner.write("Put line 4: a client the day's _idx doesn't list", func() error { return CheckFrameLines.Put(&newClientLine) }); err != nil {
		return err
	}
	runner.expect("Fresh day-client-product: Fecha = 20000, the new client's file read too", []string{"20000/5/10 q5000 a300", "20000/7/12 q500 a50"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay).Fresh()))
	if err := materialize("Run 4: adds the changes"); err != nil {
		return err
	}
	runner.expect("day-product: Fecha BETWEEN 20000 AND 20001", []string{"20000/10 q5000"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expect("day-client-product: ClientID = 5, Fecha BETWEEN 20000 AND 20001", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.ClientID, int32(5)).Between(lines.Fecha, checkFrameDay, checkFrameDay+1)))
	runner.expect("day-client-product: Fecha = 20000, every client", []string{"20000/5/10 q5000 a300"},
		queryFrame(CheckFrameLines.QueryFrame(checkFrameDayClientProduct).Eq(lines.Fecha, checkFrameDay)))

	// The files the runs kept up to date must be those a rebuild computes from the records. The rebuild
	// does delete the files the runs emptied (line 2's client, line 3's day), and rewrites their _idx.
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
			if etagsBefore[key] != etag && !strings.HasSuffix(key, "/_idx") {
				rewritten++
			}
		}
		return []string{strconv.Itoa(rewritten)}, err
	})
	runner.expectOnce("RebuildDataFramesAll: the same rows", []string{"20000/10 q5000"}, func() ([]string, error) {
		if err := CheckFrameLines.RebuildDataFramesAll(""); err != nil {
			return nil, err
		}
		return queryFrame(CheckFrameLines.QueryFrame(checkFrameDayProduct).Between(lines.Fecha, checkFrameDay, checkFrameDay+1))()
	})
	return nil
}

// queryFrame labels each row by its Keys, Rows and sums; "not built" for dataframe.ErrNotBuilt.
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
				labels[i] = fmt.Sprintf("%d/%d q%d", row.Key.Fecha, row.Key.ProductID, row.Sum(lines.Quantity))
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
