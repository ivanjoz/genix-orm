package db

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// Expanded slices (TableSchema.ExpandedSlices): an integer slice of a fixed size stored as one scalar
// column per element, so a write can add to one element in SQL (`purchases_3` = `purchases_3` + ?)
// instead of reading the record and writing the whole slice back. A record holding 7 daily sums
// stays one row, where one row per day would cost 7 rows' headers and primary keys.
//
//	ExpandedSlices: db.Cols(e.Purchases.Expand(7), e.Payments.Expand(7))
//	db.Increment(ClientWeek{CompanyID: 1, ID: clientWeekID}, t.Purchases.At(3).Add(amount))
//
// Only MariaDB stores them; the other SQL drivers refuse the declaration (RefuseExpandedSlices).

// ColSliceElement is one element of an expanded slice column (ColSlice.At).
type ColSliceElement[E any] struct {
	columnName   string
	elementIndex int
}

// Add is the increment of the element by delta, for db.Increment.
func (element ColSliceElement[E]) Add(delta E) ColumnIncrement {
	deltaValue := reflect.ValueOf(delta)
	increment := ColumnIncrement{ColumnName: element.columnName, ElementIndex: element.elementIndex}
	if deltaValue.CanInt() {
		increment.Delta = deltaValue.Int()
	} else if deltaValue.CanUint() {
		increment.Delta = int64(deltaValue.Uint())
	}
	return increment
}

// ColumnIncrement adds Delta to element ElementIndex of the expanded slice column ColumnName.
type ColumnIncrement struct {
	ColumnName   string
	ElementIndex int
	Delta        int64
}

// integerElementKinds are the element kinds an expanded slice may hold: any integer.
var integerElementKinds = []reflect.Kind{
	reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
	reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
}

// ConfigureExpandedSlices stamps each TableSchema.ExpandedSlices column with its size, once the
// indexes are described: an expanded column is stored as scalars, so it can't be a key or key an
// index (an index over a slice keys on its stored array). Schema mistakes panic when the table compiles.
func (core *TableCore) ConfigureExpandedSlices(schema TableSchema) {
	keyedColumnNames := []string{}
	for _, source := range core.IndexSources {
		keyedColumnNames = append(keyedColumnNames, source.Columns...)
		for _, packedColumn := range source.PackedSourceColumns {
			keyedColumnNames = append(keyedColumnNames, packedColumn.GetName())
		}
	}
	for _, indexGroup := range core.IndexGroups {
		for _, sourceColumn := range indexGroup.SourceColumns {
			keyedColumnNames = append(keyedColumnNames, sourceColumn.Column.GetName())
		}
	}
	for _, deltaIndex := range core.DeltaIndexes {
		for _, pinnedColumn := range deltaIndex.Pinned {
			keyedColumnNames = append(keyedColumnNames, pinnedColumn.GetName())
		}
	}

	for _, declared := range schema.ExpandedSlices {
		fail := func(reason string) {
			panic(fmt.Sprintf(`Table "%v": expanded slice "%v" %v`, core.Name, declared.GetName(), reason))
		}
		expandedSize := declared.GetInfo().ExpandedSize
		if expandedSize < 1 {
			fail("must be declared with .Expand(size)")
		}
		column, isColumnInfo := core.ColumnsMap[declared.GetName()].(*ColumnInfo)
		if !isColumnInfo || column.Field == nil || column.IsComputed {
			fail("must be a column of the record")
		}
		if column.Field.Type.Kind() != reflect.Slice || !slices.Contains(integerElementKinds, column.Field.Type.Elem().Kind()) {
			fail(fmt.Sprintf("must be a slice of integers (not a pointer). Found: %v", column.Field.Type))
		}
		if slices.Contains(core.KeysIdx, column.Idx) {
			fail("can't be part of the primary key")
		}
		if slices.Contains(keyedColumnNames, column.Name) {
			fail("can't key an index, view or cache group")
		}
		column.ExpandedSize = expandedSize
	}
}

// RefuseExpandedSlices fails the compile of a table declaring ExpandedSlices on a driver that does
// not store them, instead of silently storing them as one value.
func RefuseExpandedSlices(schema TableSchema, driverName string) {
	if len(schema.ExpandedSlices) > 0 {
		panic(fmt.Sprintf(`Table "%v": ExpandedSlices are not implemented on %v (MariaDB only)`, schema.Name, driverName))
	}
}

// incrementExecutor is a driver that adds to expanded slice elements in SQL (mariadbdriver).
type incrementExecutor[RecordT any] interface {
	Increment(record *RecordT, increments []ColumnIncrement) error
	// IncrementIn returns what runs after the commit.
	IncrementIn(driverTransaction any, record *RecordT, increments []ColumnIncrement) (func() error, error)
}

func incrementExecutorOf[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]]() (incrementExecutor[RecordT], error) {
	executor := executorFor[RecordT, TableT, D]()
	incrementer, isIncrementer := executor.(incrementExecutor[RecordT])
	if !isIncrementer {
		return nil, fmt.Errorf("db: driver %q cannot increment expanded slice columns", executor.Name())
	}
	return incrementer, nil
}

// ErrIncrementNotStored is what Increment returns (wrapped) when no record is stored under the key:
// a caller that creates the record on its first increment tests it with errors.Is and inserts it.
var ErrIncrementNotStored = errors.New("no record is stored with the key")

// Increment adds to elements of the expanded slice columns of the stored record with record's
// partition and keys, and stamps its updated (and version): the other fields of record are ignored.
// A KeyIntPacking key is packed from its slot columns, as a write does. A record that is not stored
// fails the increment with ErrIncrementNotStored; nothing is created.
func Increment[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	record RecordT, increments ...ColumnIncrement,
) error {
	incrementer, err := incrementExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	return incrementer.Increment(&record, increments)
}

// IncrementIn is Increment inside the transaction of RunTransaction.
func IncrementIn[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, record RecordT, increments ...ColumnIncrement,
) error {
	incrementer, err := incrementExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	if transaction.driverTransaction == nil {
		return incrementer.Increment(&record, increments)
	}
	afterCommit, err := incrementer.IncrementIn(transaction.driverTransaction, &record, increments)
	// A missing record is the body's to handle (it inserts it): it is not noted as the driver error,
	// which would hide a later deadlock from the rerun.
	if errors.Is(err, ErrIncrementNotStored) {
		return err
	}
	if err != nil {
		return transaction.noteDriverError(err)
	}
	transaction.afterCommit = append(transaction.afterCommit, afterCommit)
	return nil
}
