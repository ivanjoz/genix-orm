package scylla

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

type packedIndexScope int8

const (
	packedIndexScopeLocal  packedIndexScope = 1
	packedIndexScopeGlobal packedIndexScope = 2
)

type packedIndexBuildConfig struct {
	scope packedIndexScope
	// schemaFieldName is used only for panic/error messages so callers can name the index family being compiled.
	schemaFieldName string
	// virtualColumnPrefix differentiates local/global packed columns (e.g. "zz_ixp_" vs "zz_gixp_").
	virtualColumnPrefix string
	// indexType is viewInfo.Type: 2 for local index, 1 for global index.
	indexType int8
	// requireInOnlyFirst restricts fan-out when using global indexes.
	// If true, IN is allowed only on the first component.
	requireInOnlyFirst bool
}

func isSupportedPackedIndexNumericFieldType(fieldType string) bool {
	// Packed indexes only support scalar integers. Slices are rejected by callers.
	switch fieldType {
	case "int8", "int16", "int32", "int64", "int":
		return true
	default:
		return false
	}
}

func registerPackedIndex(
	dbTable *ScyllaTable,
	idxCount *int8,
	indexColumns []Coln,
	cfg packedIndexBuildConfig,
) {
	// Build a stored packed numeric column plus an index on it.
	// Rationale: this enables composite predicates like "Status IN (...) + Updated BETWEEN/GT".
	if len(indexColumns) < 2 {
		panic(fmt.Sprintf(`Table "%v": %v entries must have at least 2 columns. Found: %v`, dbTable.Name, cfg.schemaFieldName, len(indexColumns)))
	}

	sourceColumns := make([]IColInfo, 0, len(indexColumns))
	sourceColumnNames := make([]string, 0, len(indexColumns))
	slotBitsPerColumn := make([]int64, 0, len(indexColumns))

	isInt32Packed := false

	for columnIndex, indexColumnConfig := range indexColumns {
		configInfo := indexColumnConfig.GetInfo()
		column := dbTable.ColumnsMap[configInfo.Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": %v column "%v" was not found`, dbTable.Name, cfg.schemaFieldName, configInfo.Name))
		}

		if column.GetType().IsComplexType || column.GetType().IsSlice || !isSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": %v packed column "%v" must be a scalar integer. Found: %v`, dbTable.Name, cfg.schemaFieldName, column.GetName(), column.GetType().FieldType))
		}

		if configInfo.UseInt32Packing {
			isInt32Packed = true
		}

		// Size rules:
		// - First component MUST NOT set Size() (it takes the bits the others leave).
		// - All remaining components MUST set Size().
		if columnIndex == 0 && configInfo.SlotBits > 0 {
			panic(fmt.Sprintf(`Table "%v": %v first column "%v" must not set Size()`, dbTable.Name, cfg.schemaFieldName, configInfo.Name))
		}
		if columnIndex > 0 && configInfo.SlotBits <= 0 {
			panic(fmt.Sprintf(`Table "%v": %v requires Size() for column "%v" (all columns after the first must set Size)`, dbTable.Name, cfg.schemaFieldName, configInfo.Name))
		}

		sourceColumns = append(sourceColumns, column)
		sourceColumnNames = append(sourceColumnNames, column.GetName())
		slotBitsPerColumn = append(slotBitsPerColumn, int64(configInfo.SlotBits)) // first column set after the budget calc
	}

	budgetBits := packedVirtualBudgetBits(isInt32Packed)
	firstSlotBits := budgetBits - sumSlotBits(slotBitsPerColumn, 1)
	if firstSlotBits <= 0 {
		panic(fmt.Sprintf(`Table "%v": %v trailing Size() slots take all %v bits, leaving none for "%v"`,
			dbTable.Name, cfg.schemaFieldName, budgetBits, sourceColumnNames[0]))
	}
	slotBitsPerColumn[0] = firstSlotBits

	virtualPackedColName := fmt.Sprintf("%s%s", cfg.virtualColumnPrefix, strings.Join(sourceColumnNames, "_"))
	if _, exists := dbTable.ColumnsMap[virtualPackedColName]; exists {
		panic(fmt.Sprintf(`Table "%v": generated packed column already exists: %v`, dbTable.Name, virtualPackedColName))
	}

	packedColumnTypeName := "int64"
	if isInt32Packed {
		packedColumnTypeName = "int32"
	}

	sourceColumnsLocal := slices.Clone(sourceColumns)
	slotBitsLocal := slices.Clone(slotBitsPerColumn)
	isInt32PackedLocal := isInt32Packed

	virtualPackedColumn := &columnInfo{
		ColInfo: colInfo{
			Name:      virtualPackedColName,
			FieldName: virtualPackedColName,
			IsVirtual: true,
			Idx:       dbTable.MaxColIdx,
		},
		ColType: db.GetColTypeByName(packedColumnTypeName),
	}
	virtualPackedColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
		componentValues := make([]int64, 0, len(sourceColumnsLocal))
		for _, sourceColumn := range sourceColumnsLocal {
			componentValues = append(componentValues, convertToInt64(sourceColumn.GetRawValue(ptr)))
		}
		packed := packRowValues(dbTable.Name, virtualPackedColName, sourceColumnsLocal, componentValues, slotBitsLocal)
		return storeVirtualPacked(packed, isInt32PackedLocal)
	}
	virtualPackedColumn.GetValueFn = virtualPackedColumn.GetRawValueFn

	dbTable.MaxColIdx++
	dbTable.ColumnsMap[virtualPackedColumn.GetName()] = virtualPackedColumn

	indexNameSuffix := "index_0"
	if cfg.indexType == 2 {
		indexNameSuffix = "index_1"
	}
	indexName := fmt.Sprintf(`%v__%v_%v`, dbTable.Name, virtualPackedColName, indexNameSuffix)
	if _, exists := dbTable.indexes[indexName]; exists {
		panic(fmt.Sprintf(`Table "%v": %v index name already exists: %v`, dbTable.Name, cfg.schemaFieldName, indexName))
	}

	index := &viewInfo{
		Type:               cfg.indexType,
		name:               indexName,
		idx:                *idxCount,
		column:             virtualPackedColumn,
		columns:            slices.Clone(sourceColumnNames),
		RequiresPostFilter: true,
	}

	switch cfg.scope {
	case packedIndexScopeLocal:
		partitionColumn := dbTable.GetPartKey()
		if partitionColumn == nil || partitionColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": %v requires a Partition column`, dbTable.Name, cfg.schemaFieldName))
		}
		partitionName := partitionColumn.GetName()

		index.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`, indexName, dbTable.GetFullName(), partitionName, virtualPackedColName)
		}

		dbTable.packedIndexes = append(dbTable.packedIndexes, &packedIndexInfo{
			indexName:           index.name,
			packedColumnName:    virtualPackedColName,
			sourceColumnNames:   slices.Clone(sourceColumnNames),
			partitionColumnName: partitionName,
			slotBitsPerColumn:   slices.Clone(slotBitsPerColumn),
			isInt32Packed:       isInt32Packed,
		})

	case packedIndexScopeGlobal:
		index.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, indexName, dbTable.GetFullName(), virtualPackedColName)
		}

		dbTable.packedIndexes = append(dbTable.packedIndexes, &packedIndexInfo{
			indexName:           index.name,
			packedColumnName:    virtualPackedColName,
			sourceColumnNames:   slices.Clone(sourceColumnNames),
			partitionColumnName: "",
			slotBitsPerColumn:   slices.Clone(slotBitsPerColumn),
			isInt32Packed:       isInt32Packed,
		})
	default:
		panic(fmt.Sprintf(`Table "%v": unknown packedIndexScope=%v`, dbTable.Name, cfg.scope))
	}

	packedColumnNameLocal := virtualPackedColName
	lastSourceColNameLocal := sourceColumnNames[len(sourceColumnNames)-1]
	index.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
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

			if cfg.requireInOnlyFirst && i > 0 && st.Operator == "IN" {
				return nil
			}

			values := []int64{}
			switch st.Operator {
			case "=":
				values = append(values, convertToInt64(st.Value))
			case "IN":
				for _, v := range st.Values {
					values = append(values, convertToInt64(v))
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
				whereStatements = append(whereStatements, emitRangeClause(prefixValues, "=", convertToInt64(lastStatement.Value)))
			case "BETWEEN":
				if len(lastStatement.From) == 0 || len(lastStatement.To) == 0 {
					return nil
				}
				fromValue := convertToInt64(lastStatement.From[0].Value)
				toValue := convertToInt64(lastStatement.To[0].Value)
				fromClause := emitRangeClause(prefixValues, ">=", fromValue)
				toClause := emitRangeClause(prefixValues, "<=", toValue)
				whereStatements = append(whereStatements, boundWhereClause{
					Clause: fromClause.Clause + " AND " + toClause.Clause,
					Values: append(fromClause.Values, toClause.Values...),
				})
			case ">", ">=", "<", "<=":
				whereStatements = append(whereStatements, emitRangeClause(prefixValues, lastStatement.Operator, convertToInt64(lastStatement.Value)))
			default:
				return nil
			}
		}

		return whereStatements
	}

	*idxCount = *idxCount + 1
	dbTable.indexes[index.name] = index

	fmt.Printf("Packed index registered: table=%s scope=%v index=%s packedCol=%s isInt32=%v slotBits=%v\n",
		dbTable.Name, cfg.scope, index.name, virtualPackedColName, isInt32Packed, slotBitsPerColumn)
}
