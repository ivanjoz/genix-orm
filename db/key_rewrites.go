package db

import "slices"

// The key rewrites turn predicates on the columns a smart primary key is built from (a
// concatenated text key, an int-packed key) into one predicate on the physical key column plus
// the statements left over. Values are neutral: the concatenated text, the packed int64.

// concatRangeBounds maps a one-sided operator on the last pinned concatenated column to the
// [from, to) bounds of the key text, given the prefix block's own floor and ceiling.
var concatRangeBounds = map[string]struct {
	from func(value, prefixMin, prefixMax string) string
	to   func(value, prefixMin, prefixMax string) string
}{
	">=": {
		from: func(value, prefixMin, prefixMax string) string { return value },
		to:   func(value, prefixMin, prefixMax string) string { return prefixMax },
	},
	">": {
		from: func(value, prefixMin, prefixMax string) string { return value + "￿" },
		to:   func(value, prefixMin, prefixMax string) string { return prefixMax },
	},
	"<=": {
		from: func(value, prefixMin, prefixMax string) string { return prefixMin },
		to:   func(value, prefixMin, prefixMax string) string { return value + "￿" },
	},
	"<": {
		from: func(value, prefixMin, prefixMax string) string { return prefixMin },
		to:   func(value, prefixMin, prefixMax string) string { return value },
	},
}

// smartKeyPrefix walks the smart-key source columns in order and collects the leading equalities,
// stopping at the first column that is absent or carries a range (which it also returns).
func smartKeyPrefix(sourceColumnNames []string, statements []ColumnStatement) ([]any, *ColumnStatement, map[string]bool) {
	prefixValues := []any{}
	var rangeStatement *ColumnStatement
	handledColumns := map[string]bool{}
	for _, columnName := range sourceColumnNames {
		found := false
		for statementIndex := range statements {
			statement := statements[statementIndex]
			if statement.Col != columnName {
				continue
			}
			if statement.Operator == "=" {
				prefixValues = append(prefixValues, statement.Value)
				handledColumns[statement.Col] = true
				found = true
				break
			}
			if isRangeOperator(statement.Operator) || statement.Operator == "BETWEEN" {
				rangeStatement = &statements[statementIndex]
				handledColumns[statement.Col] = true
				found = true
				break
			}
		}
		if !found || rangeStatement != nil {
			break
		}
	}
	return prefixValues, rangeStatement, handledColumns
}

// queriesKeyColumn tells whether the query already constrains the physical key column, in which
// case no rewrite applies.
func queriesKeyColumn(keyColumnName string, statements []ColumnStatement) bool {
	return slices.ContainsFunc(statements, func(statement ColumnStatement) bool { return statement.Col == keyColumnName })
}

// withUnhandled puts the rewritten key predicate first, then every statement it did not absorb.
func withUnhandled(rewrittenStatement ColumnStatement, statements []ColumnStatement, handledColumns map[string]bool) []ColumnStatement {
	rewrittenStatements := []ColumnStatement{rewrittenStatement}
	for _, statement := range statements {
		if !handledColumns[statement.Col] {
			rewrittenStatements = append(rewrittenStatements, statement)
		}
	}
	return rewrittenStatements
}

// BuildKeyConcatenatedStatements converts equality / range prefixes on a concatenated key's source
// columns into one equality or range on the key text. false when it does not apply.
func (core *TableCore) BuildKeyConcatenatedStatements(statements []ColumnStatement) ([]ColumnStatement, bool) {
	keyColumnName := core.Keys[0].GetName()
	if queriesKeyColumn(keyColumnName, statements) || len(core.KeyConcatenated) == 0 {
		return nil, false
	}
	sourceColumnNames := make([]string, 0, len(core.KeyConcatenated))
	for _, concatColumn := range core.KeyConcatenated {
		sourceColumnNames = append(sourceColumnNames, concatColumn.GetName())
	}
	prefixValues, rangeStatement, handledColumns := smartKeyPrefix(sourceColumnNames, statements)
	if len(prefixValues) == 0 && rangeStatement == nil {
		return nil, false
	}

	prefixValueText := ""
	if len(prefixValues) > 0 {
		prefixValueText = MakeKeyConcat(prefixValues...)
	}
	keyBetween := func(from, to []ColumnStatement) ColumnStatement {
		return ColumnStatement{Col: keyColumnName, Operator: "BETWEEN", From: from, To: to}
	}

	var rewrittenStatement ColumnStatement
	switch {
	case rangeStatement == nil && len(prefixValues) == len(core.KeyConcatenated):
		rewrittenStatement = ColumnStatement{Col: keyColumnName, Operator: "=", Value: prefixValueText}
	case rangeStatement == nil:
		// A partial prefix is every key starting with it.
		rewrittenStatement = keyBetween(
			[]ColumnStatement{{Col: keyColumnName, Value: prefixValueText + KeyConcatSeparator}},
			[]ColumnStatement{{Col: keyColumnName, Value: prefixValueText + KeyConcatSeparator + "￿"}})
	case rangeStatement.Operator == "BETWEEN":
		rewrittenStatement = keyBetween(
			[]ColumnStatement{{Col: keyColumnName, Value: MakeKeyConcat(append(prefixValues, rangeStatement.From[0].Value)...)}},
			[]ColumnStatement{{Col: keyColumnName, Value: MakeKeyConcat(append(prefixValues, rangeStatement.To[0].Value)...) + "￿"}})
	default:
		rangeBounds, isKnownOperator := concatRangeBounds[rangeStatement.Operator]
		if !isKnownOperator {
			break
		}
		rangeValue := MakeKeyConcat(append(prefixValues, rangeStatement.Value)...)
		prefixMin, prefixMax := "", "￿"
		if prefixValueText != "" {
			prefixMin = prefixValueText + KeyConcatSeparator
			prefixMax = prefixValueText + KeyConcatSeparator + "￿"
		}
		rewrittenStatement = ColumnStatement{Col: keyColumnName, Operator: "BETWEEN"}
		if fromValue := rangeBounds.from(rangeValue, prefixMin, prefixMax); fromValue != "" {
			rewrittenStatement.From = append(rewrittenStatement.From, ColumnStatement{Col: keyColumnName, Operator: ">=", Value: fromValue})
		}
		if toValue := rangeBounds.to(rangeValue, prefixMin, prefixMax); toValue != "" {
			rewrittenStatement.To = append(rewrittenStatement.To, ColumnStatement{Col: keyColumnName, Operator: "<", Value: toValue})
		}
	}
	return withUnhandled(rewrittenStatement, statements, handledColumns), true
}

// BuildKeyIntPackingStatements converts equality / range prefixes on an int-packed key's
// components into one equality or inclusive range on the packed key. false when it does not apply.
func (core *TableCore) BuildKeyIntPackingStatements(statements []ColumnStatement) ([]ColumnStatement, bool) {
	keyColumnName := core.Keys[0].GetName()
	if queriesKeyColumn(keyColumnName, statements) || len(core.KeyIntPacking) == 0 {
		return nil, false
	}
	sourceColumnNames := []string{}
	for _, packedColumn := range core.KeyIntPacking {
		if packedColumn.GetName() == "autoincrement_placeholder" {
			break
		}
		sourceColumnNames = append(sourceColumnNames, packedColumn.GetName())
	}
	prefixValues, rangeStatement, handledColumns := smartKeyPrefix(sourceColumnNames, statements)
	if len(prefixValues) == 0 && rangeStatement == nil {
		return nil, false
	}

	// The pinned prefix spans one contiguous block of the key; the bounds are inclusive.
	slotBits := core.KeyIntPackingSlotBits
	prefix := make([]int64, 0, len(slotBits))
	for _, value := range prefixValues {
		prefix = append(prefix, ConvertToInt64(value))
	}
	if len(prefix) == len(slotBits) {
		packedKey := int64(PackSlotValues(prefix, slotBits))
		return withUnhandled(ColumnStatement{Col: keyColumnName, Operator: "=", Value: packedKey}, statements, handledColumns), true
	}
	fromPrefix, toPrefix := prefix, prefix
	if rangeStatement != nil {
		if rangeStatement.Operator == "BETWEEN" {
			fromPrefix = append(slices.Clone(prefix), ConvertToInt64(rangeStatement.From[0].Value))
			toPrefix = append(slices.Clone(prefix), ConvertToInt64(rangeStatement.To[0].Value))
		} else {
			// Any other range operator scans the block of its own value.
			fromPrefix = append(slices.Clone(prefix), ConvertToInt64(rangeStatement.Value))
			toPrefix = fromPrefix
		}
	}
	rewrittenStatement := ColumnStatement{
		Col: keyColumnName, Operator: "BETWEEN",
		From: []ColumnStatement{{Col: keyColumnName, Value: int64(PackSlotPrefixBound(fromPrefix, slotBits, false))}},
		To:   []ColumnStatement{{Col: keyColumnName, Value: int64(PackSlotPrefixBound(toPrefix, slotBits, true))}},
	}
	return withUnhandled(rewrittenStatement, statements, handledColumns), true
}
