package mariadbdriver

import (
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// Expanded slices (db.TableSchema.ExpandedSlices) on MariaDB: an integer slice of size N is stored as
// N scalar columns, <name>_0 … <name>_<N-1>, each one storedColumn (isExpandedElement). A write binds
// every element (0 past the slice's end), a read returns the slice at its full size, and db.Increment
// adds to elements in SQL.

// expandedElementName is the column of one element of an expanded slice: purchases_3.
func expandedElementName(columnName string, elementIndex int) string {
	return columnName + "_" + strconv.Itoa(elementIndex)
}

var expandedElementSQLTypes = map[reflect.Kind]string{
	reflect.Int8: "TINYINT", reflect.Int16: "SMALLINT", reflect.Int32: "INT", reflect.Int64: "BIGINT", reflect.Int: "BIGINT",
	reflect.Uint8: "TINYINT UNSIGNED", reflect.Uint16: "SMALLINT UNSIGNED", reflect.Uint32: "INT UNSIGNED",
	reflect.Uint64: "BIGINT UNSIGNED", reflect.Uint: "BIGINT UNSIGNED",
}

// expandedElementSQLType is the column type of each element: the element's integer type, so an
// increment past its range fails in strict mode instead of wrapping.
func expandedElementSQLType(column *db.ColumnInfo) string {
	return expandedElementSQLTypes[column.Field.Type.Elem().Kind()]
}

// expandedElementArgument is the statement argument of one element: 0 past the slice's end. A slice
// longer than its declared size fails the write rather than lose elements.
func expandedElementArgument(stored storedColumn, sliceValue reflect.Value) (any, error) {
	expandedSize := int(stored.column.GetInfo().ExpandedSize)
	if sliceValue.Len() > expandedSize {
		return nil, fmt.Errorf("expanded slice %q holds %d elements, its size is %d", stored.column.GetName(), sliceValue.Len(), expandedSize)
	}
	if stored.elementIndex >= sliceValue.Len() {
		return 0, nil
	}
	return sliceValue.Index(stored.elementIndex).Interface(), nil
}

// assignExpandedElement writes one scanned element into its slice, grown to the declared size first:
// a read always returns every element. NULL (a column added to a populated table) reads 0.
func assignExpandedElement(stored storedColumn, sliceValue reflect.Value, target any) {
	if expandedSize := int(stored.column.GetInfo().ExpandedSize); sliceValue.Len() < expandedSize {
		grownSlice := reflect.MakeSlice(sliceValue.Type(), expandedSize, expandedSize)
		reflect.Copy(grownSlice, sliceValue)
		sliceValue.Set(grownSlice)
	}
	element := sliceValue.Index(stored.elementIndex)
	switch scanned := target.(type) {
	case *sql.NullInt64:
		if scanned.Valid {
			element.SetInt(scanned.Int64)
		}
	case *sql.Null[uint64]:
		if scanned.Valid {
			element.SetUint(scanned.V)
		}
	}
}

// Increment is db.Increment in its own transaction.
func (Exec[TableT, RecordT]) Increment(record *RecordT, increments []db.ColumnIncrement) error {
	table := compiledTableOf[TableT, RecordT]()
	var afterCommit func() error
	err := runTransaction(func(transaction *writeTransaction) error {
		var err error
		afterCommit, err = table.incrementRecord(transaction, db.MakeRecordSlice(&[]RecordT{*record}), increments)
		return err
	})
	if err != nil {
		return err
	}
	return afterCommit()
}

// IncrementIn is db.IncrementIn inside the transaction of db.RunTransaction.
func (Exec[TableT, RecordT]) IncrementIn(driverTransaction any, record *RecordT, increments []db.ColumnIncrement) (func() error, error) {
	return compiledTableOf[TableT, RecordT]().incrementRecord(driverTransaction.(*writeTransaction), db.MakeRecordSlice(&[]RecordT{*record}), increments)
}

// incrementRecord adds the increments to the stored record keyed like records' only record. A table
// whose side writes derive from the whole stored record (cache-group marks, a ToFrames frame) goes
// through it: read under lock, incremented, written as an Update. Any other is one UPDATE that adds
// in SQL, with no read.
func (table *MariaTable) incrementRecord(transaction *writeTransaction, records db.RecordSlice, increments []db.ColumnIncrement) (func() error, error) {
	if len(increments) == 0 {
		return nil, fmt.Errorf("table %q: Increment needs at least one element: e.Column.At(index).Add(delta)", table.Name)
	}
	for _, increment := range increments {
		column := table.ColumnsMap[increment.ColumnName]
		if column == nil || column.GetInfo().ExpandedSize == 0 {
			return nil, fmt.Errorf(`table %q: Increment: column %q is not declared in ExpandedSlices`, table.Name, increment.ColumnName)
		}
		if expandedSize := int(column.GetInfo().ExpandedSize); increment.ElementIndex < 0 || increment.ElementIndex >= expandedSize {
			return nil, fmt.Errorf(`table %q: Increment: element %d of column %q is out of its size %d`, table.Name, increment.ElementIndex, increment.ColumnName, expandedSize)
		}
	}
	// A packed key is built from its slot columns, as a write builds it. An autoincrement slot would
	// mint a new key, which no stored record has: the caller must give the key.
	if len(table.KeyIntPacking) > 0 {
		if table.AutoincrementCol != nil {
			return nil, fmt.Errorf("table %q: Increment can't pack a key with an autoincrement slot: give the record its ID", table.Name)
		}
		if err := table.AssignAutoincrementKeys(records, nil); err != nil {
			return nil, err
		}
	}
	if len(table.IndexGroups) > 0 || (table.dataFrames != nil && table.dataFrames.hasToFrames) {
		return table.incrementStoredRecord(transaction, records, increments)
	}
	return func() error { return nil }, table.incrementInSQL(transaction, records, increments)
}

// incrementInSQL is one UPDATE by primary key: each element adds its delta, updated is stamped, the
// version raised, and each TypeDelta packed column takes the new updated under its stored prefix (an
// expanded slice is never pinned). It runs at once, not queued: a record that is not stored matches no
// row (rows affected counts matched rows, driver.go) and fails the increment.
func (table *MariaTable) incrementInSQL(transaction *writeTransaction, records db.RecordSlice, increments []db.ColumnIncrement) error {
	stamps := table.StampManagedColumns(records, false, int64(db.ManagedUnixTime()))
	recordPointer := records.At(0)
	assignments, args := []string{}, []any{}
	for _, increment := range increments {
		quoted := quoteName(expandedElementName(increment.ColumnName, increment.ElementIndex))
		assignments = append(assignments, fmt.Sprintf("%s = IFNULL(%s, 0) + ?", quoted, quoted))
		args = append(args, increment.Delta)
	}
	if table.UpdatedCol != nil {
		assignments = append(assignments, quoteName(table.UpdatedCol.GetName())+" = ?")
		args = append(args, db.CoerceManagedInteger(table.UpdatedCol, stamps[0].Updated))
	}
	if table.VersionCol != nil {
		assignments = append(assignments, versionAssignment(table.VersionCol.GetName()))
	}
	updatedMask := uint64(1)<<db.DeltaUpdatedSlotBits - 1
	for _, deltaIndex := range table.DeltaIndexes {
		quoted := quoteName(deltaIndex.PackedColumn.GetName())
		assignments = append(assignments, fmt.Sprintf("%s = (%s & %d) | ?", quoted, quoted, ^updatedMask))
		args = append(args, deltaIndex.PackedKey(table.Name, make([]int64, len(deltaIndex.Pinned)), stamps[0].Updated)&updatedMask)
	}
	keyNames := make([]string, len(table.primaryKeyColumns))
	keyValues := make([]any, len(table.primaryKeyColumns))
	for position, keyColumn := range table.primaryKeyColumns {
		keyNames[position], keyValues[position] = keyColumn.GetName(), keyColumn.GetRawValue(recordPointer)
	}
	statement := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table.qualifiedName(), strings.Join(assignments, ", "), quotedEqualities(keyNames))

	if err := transaction.flush(); err != nil {
		return err
	}
	result, err := transaction.connection.ExecContext(transaction.ctx, statement, append(args, keyValues...)...)
	if err != nil {
		return fmt.Errorf("table %q: incrementing: %w", table.Name, err)
	}
	if matchedRows, err := result.RowsAffected(); err != nil || matchedRows == 0 {
		return fmt.Errorf("table %q: Increment: %w %v", table.Name, db.ErrIncrementNotStored, keyValues)
	}
	if table.CacheByIDs {
		return table.raiseWrittenSlots(transaction, records, 0, 1)
	}
	return nil
}

// incrementStoredRecord reads the whole stored record under lock, adds the increments to a copy of it
// and writes its incremented columns as an Update, which derives every side write from it. The read
// is remembered for that write (rememberLocked), so it is the only read.
func (table *MariaTable) incrementStoredRecord(transaction *writeTransaction, records db.RecordSlice, increments []db.ColumnIncrement) (func() error, error) {
	readColumns := make([]db.IColInfo, 0, len(table.storedColumns))
	for _, stored := range table.storedColumns {
		readColumns = append(readColumns, stored.column)
	}
	storedByKey, err := table.lockStoredRecords(transaction, writePhase{records: records}, 0, 1, readColumns)
	if err != nil {
		return nil, err
	}
	storedRecord, isStored := storedByKey[table.primaryKeyToken(records.At(0))]
	if !isStored {
		return nil, fmt.Errorf("table %q: Increment: %w %v", table.Name, db.ErrIncrementNotStored, table.GetKeyValues(records.At(0)))
	}
	transaction.rememberLocked(table, storedByKey)

	// The stored record stays as read: the frames subtract the lines it gave.
	incrementedList := records.NewRecordList()
	records.AppendCopy(incrementedList, storedRecord)
	incrementedRecords := db.MakeReflectRecordSlice(incrementedList)
	incrementedPointer := incrementedRecords.At(0)
	incrementedColumns := []db.Coln{}
	for _, increment := range increments {
		column := table.ColumnsMap[increment.ColumnName].(*db.ColumnInfo)
		element := reflect.NewAt(column.Field.Type, column.Field.Pointer(incrementedPointer)).Elem().Index(increment.ElementIndex)
		if element.CanInt() {
			element.SetInt(element.Int() + increment.Delta)
		} else {
			element.SetUint(uint64(int64(element.Uint()) + increment.Delta))
		}
		if !slices.ContainsFunc(incrementedColumns, func(incremented db.Coln) bool { return incremented.GetName() == increment.ColumnName }) {
			incrementedColumn, err := db.ColnByName(table, increment.ColumnName)
			if err != nil {
				return nil, err
			}
			incrementedColumns = append(incrementedColumns, incrementedColumn)
		}
	}
	return writeRecordsIn(transaction, table, []writePhase{updateIncludePhase(incrementedRecords, incrementedColumns)})
}
