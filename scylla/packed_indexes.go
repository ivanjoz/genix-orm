package scylla

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// materializePackedIndex turns a described packed index (db.IndexKindPacked) into a Scylla
// secondary index on its generated column. The column's described raw value is the unsigned
// layout; Scylla compares integers signed, so it is stored with the top bit flipped
// (storeVirtualPacked), and every WHERE bound below goes through the same mapping.
func materializePackedIndex(dbTable *ScyllaTable, view *viewInfo) {
	packedIndex := view.PackedIndex
	indexName := view.Name
	packedColumnNameLocal := packedIndex.PackedColumnName
	sourceColumnNames := packedIndex.SourceColumnNames
	lastSourceColNameLocal := sourceColumnNames[len(sourceColumnNames)-1]
	slotBitsLocal := packedIndex.SlotBitsPerColumn
	isInt32PackedLocal := packedIndex.IsInt32Packed
	// A global index restricts IN to the first component, which bounds its fan-out.
	requireInOnlyFirst := packedIndex.PartitionColumnName == ""

	packedColumn := view.Column.(*columnInfo)
	describedRawValue := packedColumn.GetRawValueFn
	packedColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
		return storeVirtualPacked(describedRawValue(ptr).(uint64), isInt32PackedLocal)
	}
	packedColumn.GetValueFn = packedColumn.GetRawValueFn

	if packedIndex.PartitionColumnName != "" {
		view.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`, indexName, dbTable.GetFullName(), packedIndex.PartitionColumnName, packedColumnNameLocal)
		}
	} else {
		view.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, indexName, dbTable.GetFullName(), packedColumnNameLocal)
		}
	}

	view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
		statementByColumn := map[string]ColumnStatement{}
		for _, st := range statements {
			statementByColumn[st.Col] = st
		}

		// Expand prefix fanout first so each emitted clause keeps a deterministic packed value order.
		prefixValueGroups := [][]int64{{}}
		for i := 0; i < len(sourceColumnNames)-1; i++ {
			colName := sourceColumnNames[i]
			st, ok := statementByColumn[colName]
			if !ok {
				return nil
			}

			if requireInOnlyFirst && i > 0 && st.Operator == "IN" {
				return nil
			}

			values := []int64{}
			switch st.Operator {
			case "=":
				values = append(values, db.ConvertToInt64(st.Value))
			case "IN":
				for _, v := range st.Values {
					values = append(values, db.ConvertToInt64(v))
				}
			default:
				return nil
			}

			nextGroups := [][]int64{}
			for _, group := range prefixValueGroups {
				for _, v := range values {
					nextGroup := append(slices.Clone(group), v)
					nextGroups = append(nextGroups, nextGroup)
				}
			}
			prefixValueGroups = nextGroups
		}

		lastStatement, ok := statementByColumn[lastSourceColNameLocal]
		if !ok {
			return nil
		}

		// The range column is the last slot, so a bound packs exactly. A one-sided bound still spills
		// into the neighbouring prefixes, which the post-filter drops (RequiresPostFilter).
		emitRangeClause := func(prefixValues []int64, operator string, boundValue int64) boundWhereClause {
			packed := db.PackSlotValues(append(slices.Clone(prefixValues), boundValue), slotBitsLocal)
			return boundWhereClause{
				Clause: fmt.Sprintf("%v %v ?", packedColumnNameLocal, operator),
				Values: []any{storeVirtualPacked(packed, isInt32PackedLocal)},
			}
		}

		whereStatements := []boundWhereClause{}
		for _, prefixValues := range prefixValueGroups {
			switch lastStatement.Operator {
			case "=":
				whereStatements = append(whereStatements, emitRangeClause(prefixValues, "=", db.ConvertToInt64(lastStatement.Value)))
			case "BETWEEN":
				if len(lastStatement.From) == 0 || len(lastStatement.To) == 0 {
					return nil
				}
				fromValue := db.ConvertToInt64(lastStatement.From[0].Value)
				toValue := db.ConvertToInt64(lastStatement.To[0].Value)
				fromClause := emitRangeClause(prefixValues, ">=", fromValue)
				toClause := emitRangeClause(prefixValues, "<=", toValue)
				whereStatements = append(whereStatements, boundWhereClause{
					Clause: fromClause.Clause + " AND " + toClause.Clause,
					Values: append(fromClause.Values, toClause.Values...),
				})
			case ">", ">=", "<", "<=":
				whereStatements = append(whereStatements, emitRangeClause(prefixValues, lastStatement.Operator, db.ConvertToInt64(lastStatement.Value)))
			default:
				return nil
			}
		}

		return whereStatements
	}

}
