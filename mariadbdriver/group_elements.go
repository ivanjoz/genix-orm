package mariadbdriver

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// The cache groups over a slice (a TypeCacheGroup keyed on Date, ClientID and a slice of product IDs)
// keep no hash set: every slice such groups key on has one table of raw values, shared by all of them,
//
//	<table>__kv_<slice>: partition, element, the groups' other columns, the record's key
//
// all of it the primary key: one row per element of the slice and record. A group is a key prefix of
// it (the partition, the element, then its other columns' values), so its fetch is a key range, and a
// group of (Date, product) and one of (Date, ClientID, product) read the same rows. The rows are
// written from the record in memory, in its transaction: the slice may be computed (not stored).

// groupElementTable is the raw-values table of one slice.
type groupElementTable struct {
	sliceColumn db.IColInfo
	// scalarColumns are the other columns of the groups over the slice, the most shared first: they
	// follow the element in the key, so the groups sharing them read a key prefix.
	scalarColumns []db.IColInfo
	// recordKeyColumns are the record's primary-key columns, partition first.
	recordKeyColumns []db.IColInfo
	// sourceColumns are the stored columns the rows derive from: a write of any rewrites them.
	sourceColumns []db.IColInfo
	sideTable     tableSpec
}

// isGroupOverSlice: a cache group keyed on a slice, kept in its slice's groupElementTable.
func isGroupOverSlice(source *db.IndexSource) bool {
	return source.IndexGroup != nil && source.IndexGroup.UsesCollectionValues
}

// groupElementTables builds the raw-values table of every slice the table's cache groups key on.
func groupElementTables(table *MariaTable, columnTypeByName map[string]string) []groupElementTable {
	partitionColumn := table.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return nil
	}
	recordKeyColumns := append([]db.IColInfo{partitionColumn}, table.Keys...)
	groupsBySlice := map[string][]*db.IndexGroupInfo{}
	sliceColumnByName := map[string]db.IColInfo{}
	sliceNames := []string{}
	for _, source := range table.IndexSources {
		if !isGroupOverSlice(source) {
			continue
		}
		sliceIndex := slices.IndexFunc(source.IndexGroup.SourceColumns, func(sourceColumn db.IndexGroupSourceColumn) bool {
			return sourceColumn.Column.GetType().IsSlice
		})
		sliceColumn := source.IndexGroup.SourceColumns[sliceIndex].Column
		if slices.ContainsFunc(source.IndexGroup.SourceColumns[sliceIndex+1:], func(sourceColumn db.IndexGroupSourceColumn) bool {
			return sourceColumn.Column.GetType().IsSlice
		}) {
			panic(fmt.Sprintf(`mariadbdriver: table "%v" cache group "%v" keys on two slices: MariaDB supports one`, table.Name, source.IndexGroup.Name))
		}
		assertIndexableElements(table, &db.IndexSource{Name: source.Name}, sliceColumn)
		if sliceColumnByName[sliceColumn.GetName()] == nil {
			sliceNames = append(sliceNames, sliceColumn.GetName())
		}
		sliceColumnByName[sliceColumn.GetName()] = sliceColumn
		groupsBySlice[sliceColumn.GetName()] = append(groupsBySlice[sliceColumn.GetName()], source.IndexGroup)
	}

	built := []groupElementTable{}
	for _, sliceName := range sliceNames {
		elementTable := groupElementTable{sliceColumn: sliceColumnByName[sliceName], recordKeyColumns: recordKeyColumns}
		// The groups' other columns, by how many groups use them (ties keep declaration order).
		usesByName := map[string]int{}
		for _, indexGroup := range groupsBySlice[sliceName] {
			for _, sourceColumn := range indexGroup.SourceColumns {
				columnName := sourceColumn.Column.GetName()
				if columnName == sliceName || columnName == partitionColumn.GetName() {
					continue
				}
				if usesByName[columnName] == 0 {
					elementTable.scalarColumns = append(elementTable.scalarColumns, sourceColumn.Column)
				}
				usesByName[columnName]++
			}
		}
		slices.SortStableFunc(elementTable.scalarColumns, func(left, right db.IColInfo) int {
			return cmp.Compare(usesByName[right.GetName()], usesByName[left.GetName()])
		})

		spec := tableSpec{database: table.Namespace, name: boundedIdentifier(table.Name + "__kv_" + sliceName)}
		addKeyColumn := func(columnName, sqlType string) {
			if slices.Contains(spec.primaryKey, columnName) {
				return
			}
			if sqlType == "" {
				panic(fmt.Sprintf(`mariadbdriver: table "%v" cache group column "%v" over slice "%v" must be a stored column`, table.Name, columnName, sliceName))
			}
			spec.columns = append(spec.columns, columnSpec{columnName, sqlType, true})
			spec.primaryKey = append(spec.primaryKey, columnName)
		}
		addKeyColumn(partitionColumn.GetName(), columnTypeByName[partitionColumn.GetName()])
		addKeyColumn(sliceName, elementColumnType)
		for _, scalarColumn := range elementTable.scalarColumns {
			addKeyColumn(scalarColumn.GetName(), columnTypeByName[scalarColumn.GetName()])
		}
		recordKeyNames := []indexKeyPart{}
		for _, keyColumn := range recordKeyColumns {
			addKeyColumn(keyColumn.GetName(), columnTypeByName[keyColumn.GetName()])
			recordKeyNames = append(recordKeyNames, indexKeyPart{column: keyColumn.GetName()})
		}
		// A write clears a record's rows by its key.
		spec.indexes = []indexSpec{{name: "ix_record", keyParts: recordKeyNames}}
		elementTable.sideTable = spec

		for _, column := range append([]db.IColInfo{elementTable.sliceColumn}, elementTable.scalarColumns...) {
			elementTable.sourceColumns = append(elementTable.sourceColumns, table.StoredColumnsOf(column)...)
		}
		built = append(built, elementTable)
	}
	return built
}

// groupElementWrite is what a chunk writes to one raw-values table: the records whose rows are
// cleared, then the rows they hold now.
type groupElementWrite struct {
	clearedKeys [][]any
	rows        [][]any
}

// rowsOf are a record's rows: one per distinct element of its slice.
func (elementTable *groupElementTable) rowsOf(recordPointer unsafe.Pointer) [][]any {
	elements := db.FlattenInt64Values(elementTable.sliceColumn.GetRawValue(recordPointer))
	slices.Sort(elements)
	elements = slices.Compact(elements)
	rows := make([][]any, 0, len(elements))
	for _, element := range elements {
		row := []any{elementTable.recordKeyColumns[0].GetRawValue(recordPointer), element}
		for _, scalarColumn := range elementTable.scalarColumns {
			row = append(row, scalarColumn.GetRawValue(recordPointer))
		}
		for _, keyColumn := range elementTable.recordKeyColumns[1:] {
			if !slices.ContainsFunc(elementTable.scalarColumns, func(scalarColumn db.IColInfo) bool { return scalarColumn.GetName() == keyColumn.GetName() }) {
				row = append(row, keyColumn.GetRawValue(recordPointer))
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// writeGroupElements rewrites the rows of the chunk's records in its transaction.
func (table *MariaTable) writeGroupElements(transaction *writeTransaction, elementWrites []groupElementWrite) error {
	for tableIndex, elementWrite := range elementWrites {
		elementTable := &table.groupElementTables[tableIndex]
		if len(elementWrite.clearedKeys) > 0 {
			keyNames := make([]string, len(elementTable.recordKeyColumns))
			for position, keyColumn := range elementTable.recordKeyColumns {
				keyNames[position] = keyColumn.GetName()
			}
			// One record's key is written as plain equalities: MariaDB plans a single-table DELETE on a
			// one-tuple `(a, b) IN ((?, ?))` as a full scan.
			keyCondition := quotedEqualities(keyNames)
			if len(elementWrite.clearedKeys) > 1 {
				keyTuple := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(keyNames)), ", ") + ")"
				keyCondition = fmt.Sprintf("(%s) IN (%s)", quotedNames(keyNames), strings.TrimSuffix(strings.Repeat(keyTuple+", ", len(elementWrite.clearedKeys)), ", "))
			}
			if err := transaction.queue("DELETE FROM "+elementTable.sideTable.qualifiedName()+" WHERE "+keyCondition, slices.Concat(elementWrite.clearedKeys...)...); err != nil {
				return fmt.Errorf("table %q: clearing %q: %w", table.Name, elementTable.sideTable.name, err)
			}
		}
		if len(elementWrite.rows) == 0 {
			continue
		}
		columnNames := columnNamesOf(elementTable.sideTable.columns)
		rowPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columnNames)), ", ") + ")"
		statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", elementTable.sideTable.qualifiedName(), quotedNames(columnNames),
			strings.TrimSuffix(strings.Repeat(rowPlaceholders+", ", len(elementWrite.rows)), ", "))
		if err := transaction.queue(statement, slices.Concat(elementWrite.rows...)...); err != nil {
			return fmt.Errorf("table %q: writing %q: %w", table.Name, elementTable.sideTable.name, err)
		}
	}
	return nil
}

// groupElementCondition is the WHERE of a cache group over a slice: the partition, and the record's
// key IN the raw-values rows of the group's values (a key prefix of that table). ok is false for a
// group kept otherwise.
func (table *MariaTable) groupElementCondition(queryPlan *db.IndexGroupReadPlan, groupValues []int64) (condition string, args []any, ok bool) {
	indexGroup := queryPlan.IndexGroup
	if !indexGroup.UsesCollectionValues || indexGroup.BaseGroup != nil {
		return "", nil, false
	}
	partitionName := table.GetPartKey().GetName()
	conditions, args := []string{quoteName(partitionName) + " = ?"}, []any{queryPlan.PartitionValue}
	var elementTable *groupElementTable
	for position, sourceColumn := range indexGroup.SourceColumns {
		if sourceColumn.Column.GetType().IsSlice {
			tableIndex := slices.IndexFunc(table.groupElementTables, func(built groupElementTable) bool {
				return built.sliceColumn.GetName() == sourceColumn.Column.GetName()
			})
			elementTable = &table.groupElementTables[tableIndex]
		}
		conditions = append(conditions, quoteName(sourceColumn.Column.GetName())+" = ?")
		args = append(args, groupValues[position])
	}
	keyNames := make([]string, len(elementTable.recordKeyColumns))
	for position, keyColumn := range elementTable.recordKeyColumns {
		keyNames[position] = keyColumn.GetName()
	}
	keyList := quotedNames(keyNames)
	condition = fmt.Sprintf("%s = ? AND (%s) IN (SELECT %s FROM %s WHERE %s)", quoteName(partitionName), keyList, keyList,
		elementTable.sideTable.qualifiedName(), strings.Join(conditions, " AND "))
	return condition, append([]any{queryPlan.PartitionValue}, args...), true
}
