package db

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// QueryCapability is one predicate shape a table can answer without filtering: a signature of
// "column|op" pairs ("=" equality or IN, "~" range, "@" CONTAINS), the source that serves it (nil
// = the table's primary key) and a priority that breaks ties between equally covering sources.
type QueryCapability struct {
	Signature string
	Source    *IndexSource
	Priority  int
	IsKey     bool // served by the table's primary key (plain, int-packed or concatenated)
}

// CapabilityOpForStatement normalizes a query operator into a capability token, so matching treats
// every range operator alike and IN like an equality.
func CapabilityOpForStatement(statement ColumnStatement) string {
	if statement.Operator == "CONTAINS" {
		return "@"
	}
	if isRangeOperator(statement.Operator) || statement.Operator == "BETWEEN" {
		return "~"
	}
	return "="
}

// capabilityDefaultOpForColumn: a slice column is matched with CONTAINS semantics.
func capabilityDefaultOpForColumn(column IColInfo) string {
	if column != nil && column.GetType().IsSlice {
		return "@"
	}
	return "="
}

// isStandaloneIndexKind tells the indexes (as opposed to views) among the IndexSources.
func isStandaloneIndexKind(kind IndexKind) bool {
	return kind != IndexKindView && kind != IndexKindViewTable && kind != IndexKindDelta
}

// ComputeCapabilities lists every predicate shape the table's primary key and IndexSources serve.
// The planner matches a query against this list, so every driver picks the same source.
func (core *TableCore) ComputeCapabilities() []QueryCapability {
	capabilities := []QueryCapability{}
	partitionColumn := core.PartKey
	hasPartition := partitionColumn != nil && !partitionColumn.IsNil()

	// 1. The primary key: the partition alone, then each clustering key by equality or range.
	if hasPartition {
		currentSignature := fmt.Sprintf("%v|=", partitionColumn.GetName())
		capabilities = append(capabilities, QueryCapability{Signature: currentSignature, Priority: 100, IsKey: true})
		for keyIndex, keyColumn := range core.Keys {
			keyName := keyColumn.GetName()
			capabilities = append(capabilities,
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|=", keyName), Priority: 200 + keyIndex*20, IsKey: true},
				// Range is only supported on clustering keys.
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|~", keyName), Priority: 150 + keyIndex*20, IsKey: true},
			)
			currentSignature += fmt.Sprintf("|%v|=", keyName)
		}
	}

	// 2. Local (Type 2) and global (Type 1) indexes on one column. A slice column advertises
	// CONTAINS, so the signature matches CONTAINS queries.
	for _, source := range core.IndexSources {
		if !isStandaloneIndexKind(source.Kind) {
			continue
		}
		columnName := source.Column.GetName()
		columnOp := capabilityDefaultOpForColumn(source.Column)
		switch source.Type {
		case 2:
			partitionName := core.GetPartKey().GetName()
			capabilities = append(capabilities,
				QueryCapability{Signature: fmt.Sprintf("%v|=|%v|%v", partitionName, columnName, columnOp), Source: source, Priority: 120},
				QueryCapability{Signature: fmt.Sprintf("%v|=|%v|~", partitionName, columnName), Source: source, Priority: 110},
			)
		case 1:
			capabilities = append(capabilities, QueryCapability{Signature: fmt.Sprintf("%v|%v", columnName, columnOp), Source: source, Priority: 100})
		}
	}

	// 2.5 Packed indexes (local: ((partition), packed); global: (packed)) are matched on their
	// source columns, not on the packed column.
	for _, packedIndex := range core.PackedIndexes {
		if len(packedIndex.SourceColumnNames) == 0 {
			continue
		}
		source := core.IndexSourceNamed(packedIndex.IndexName)
		if source == nil {
			continue
		}
		signatureParts := []string{}
		isLocal := packedIndex.PartitionColumnName != ""
		if isLocal {
			signatureParts = append(signatureParts, packedIndex.PartitionColumnName, "=")
		}
		joinPrefix := func() string {
			prefix := strings.Join(signatureParts, "|")
			if prefix != "" {
				prefix += "|"
			}
			return prefix
		}

		if len(packedIndex.SourceColumnNames) == 1 {
			columnName := packedIndex.SourceColumnNames[0]
			capabilities = append(capabilities,
				QueryCapability{Signature: joinPrefix() + columnName + "|=", Source: source, Priority: 140},
				QueryCapability{Signature: joinPrefix() + columnName + "|~", Source: source, Priority: 130},
			)
			continue
		}

		// Equality on every prefix column, then equality or range on the last one.
		lastColumnName := packedIndex.SourceColumnNames[len(packedIndex.SourceColumnNames)-1]
		for _, columnName := range packedIndex.SourceColumnNames[:len(packedIndex.SourceColumnNames)-1] {
			signatureParts = append(signatureParts, columnName, "=")
		}
		priorityBase := 200
		if isLocal {
			// A local packed index is more selective when the partition is provided.
			priorityBase = 260
		}
		sizeBonus := len(packedIndex.SourceColumnNames) * 20
		capabilities = append(capabilities, QueryCapability{Signature: joinPrefix() + lastColumnName + "|=", Source: source, Priority: priorityBase + sizeBonus})
		// A global secondary index is not reliable for range scans (Scylla can demand ALLOW
		// FILTERING), so only a local packed index advertises a range.
		if isLocal {
			capabilities = append(capabilities, QueryCapability{Signature: joinPrefix() + lastColumnName + "|~", Source: source, Priority: priorityBase - 20 + sizeBonus})
		}
	}

	// 3. Views and cache-group hashes, by name so equal-priority ties break the same way every run.
	sourcesByName := slices.Clone(core.IndexSources)
	slices.SortFunc(sourcesByName, func(left, right *IndexSource) int { return cmp.Compare(left.Name, right.Name) })
	for _, source := range sourcesByName {
		switch source.Type {
		case 3, 7:
			capabilities = append(capabilities, core.hashSourceCapabilities(source)...)
		case 6, 9:
			capabilities = append(capabilities, core.keyPrefixSourceCapabilities(source)...)
		case 8:
			capabilities = append(capabilities, packedRangeSourceCapabilities(source)...)
		}
	}

	// 4. KeyIntPacking: the packed primary key answers equality / range on each packed component.
	if len(core.KeyIntPacking) > 0 && hasPartition {
		currentSignature := fmt.Sprintf("%v|=", partitionColumn.GetName())
		for packedIndex, packedColumn := range core.KeyIntPacking {
			columnName := packedColumn.GetName()
			if columnName == "autoincrement_placeholder" {
				continue
			}
			capabilities = append(capabilities,
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|=", columnName), Priority: 400 + packedIndex*20, IsKey: true},
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|~", columnName), Priority: 350 + packedIndex*20, IsKey: true},
			)
			currentSignature += fmt.Sprintf("|%v|=", columnName)
		}
	}

	// 5. KeyConcatenated: equality on a prefix of the concatenated columns is an equality (all of
	// them) or a prefix range on the primary key.
	if len(core.KeyConcatenated) > 0 && hasPartition {
		currentSignature := fmt.Sprintf("%v|=", partitionColumn.GetName())
		for concatIndex, concatColumn := range core.KeyConcatenated {
			columnName := concatColumn.GetName()
			capabilities = append(capabilities,
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|=", columnName), Priority: 250 + concatIndex*20, IsKey: true},
				QueryCapability{Signature: currentSignature + fmt.Sprintf("|%v|~", columnName), Priority: 200 + concatIndex*20, IsKey: true},
			)
			currentSignature += fmt.Sprintf("|%v|=", columnName)
		}
	}
	return capabilities
}

// hashSourceCapabilities: a hash view or cache group answers its full column set by equality, and a
// cache group also a range on any one non-slice column (enumerated into one hash per value).
func (core *TableCore) hashSourceCapabilities(source *IndexSource) []QueryCapability {
	equalityParts := []string{}
	for _, columnName := range source.Columns {
		equalityParts = append(equalityParts, columnName+"|"+capabilityDefaultOpForColumn(core.ColumnsMap[columnName]))
	}
	capabilities := []QueryCapability{{Signature: strings.Join(equalityParts, "|"), Source: source, Priority: 300 + len(source.Columns)*20}}

	for rangeColumnIndex, rangeColumnName := range source.Columns {
		if rangeColumn := core.ColumnsMap[rangeColumnName]; rangeColumn != nil && rangeColumn.GetType().IsSlice {
			continue // a slice column only supports CONTAINS
		}
		rangeParts := make([]string, 0, len(source.Columns)*2)
		for columnIndex, columnName := range source.Columns {
			if columnIndex == rangeColumnIndex {
				rangeParts = append(rangeParts, columnName, "~")
			} else {
				rangeParts = append(rangeParts, columnName, capabilityDefaultOpForColumn(core.ColumnsMap[columnName]))
			}
		}
		// Slightly below full equality.
		capabilities = append(capabilities, QueryCapability{Signature: strings.Join(rangeParts, "|"), Source: source, Priority: 280 + len(source.Columns)*20})
	}
	return capabilities
}

// keyPrefixSourceCapabilities: a simple view (6) or view table (9) answers every prefix of its key
// by equality, and a range on any clustering column of it. A view table's slice key matches CONTAINS.
func (core *TableCore) keyPrefixSourceCapabilities(source *IndexSource) []QueryCapability {
	capabilities := []QueryCapability{}
	currentSignature := ""
	for columnIndex, columnName := range source.Columns {
		if columnIndex > 0 {
			currentSignature += "|"
		}
		operatorToken := "="
		if source.Type == 9 {
			operatorToken = capabilityDefaultOpForColumn(core.ColumnsMap[columnName])
		}
		currentSignature += columnName + "|" + operatorToken

		priority := 100
		if columnIndex > 0 {
			priority = 250 + (columnIndex-1)*20
		}
		capabilities = append(capabilities, QueryCapability{Signature: currentSignature, Source: source, Priority: priority})
		if columnIndex > 0 {
			capabilities = append(capabilities, QueryCapability{Signature: currentSignature[:len(currentSignature)-1] + "~", Source: source, Priority: priority - 50})
		}
	}
	return capabilities
}

// packedRangeSourceCapabilities: a packed key is prefix-searchable, so equality on columns[1..i-1]
// plus equality or a range on columns[i] is one contiguous range, and the view serves its full key
// and every shorter prefix. Only the full key gets the view's own tier: a partial prefix sits above
// the bare-partition plan (100) but below a local index equality (120), because a narrow index is
// more selective than a broad prefix scan.
func packedRangeSourceCapabilities(source *IndexSource) []QueryCapability {
	capabilities := []QueryCapability{}
	signatureParts := []string{}
	for columnIndex, columnName := range source.Columns {
		if columnIndex == 0 {
			// The partition alone is served better by the base table.
			signatureParts = append(signatureParts, columnName+"|=")
			continue
		}
		prefix := strings.Join(signatureParts, "|")
		if prefix != "" {
			prefix += "|"
		}
		equalityPriority := 360 + len(source.Columns)*20
		if columnIndex < len(source.Columns)-1 {
			// Longer prefixes still outrank shorter ones; matching ranks by coverage first anyway.
			equalityPriority = min(101+columnIndex*2, 108)
		}
		capabilities = append(capabilities,
			QueryCapability{Signature: prefix + columnName + "|=", Source: source, Priority: equalityPriority},
			QueryCapability{Signature: prefix + columnName + "|~", Source: source, Priority: equalityPriority - 1},
		)
		signatureParts = append(signatureParts, columnName+"|=")
	}
	return capabilities
}

// MatchQueryCapability picks the capability whose every signature column the query constrains with
// a compatible operator, preferring the one covering the most predicates, then the highest priority.
func MatchQueryCapability(statements []ColumnStatement, capabilities []QueryCapability) *QueryCapability {
	queryOpsByColumn := make(map[string][]string)
	for _, statement := range statements {
		queryOpsByColumn[statement.Col] = append(queryOpsByColumn[statement.Col], CapabilityOpForStatement(statement))
	}

	var bestMatch *QueryCapability
	bestCoveredPredicates := 0
	for capabilityIndex := range capabilities {
		capability := &capabilities[capabilityIndex]
		parts := strings.Split(capability.Signature, "|")
		isMatch := true
		consumedOpsByColumn := make(map[string]int)
		for partIndex := 0; partIndex < len(parts); partIndex += 2 {
			columnName, wantedOp := parts[partIndex], parts[partIndex+1]
			queryOps, hasColumn := queryOpsByColumn[columnName]
			if !hasColumn {
				isMatch = false
				break
			}
			// A range slot also accepts an equality.
			found := false
			for opIndex := consumedOpsByColumn[columnName]; opIndex < len(queryOps); opIndex++ {
				if queryOps[opIndex] == wantedOp || (wantedOp == "~" && queryOps[opIndex] == "=") {
					found = true
					consumedOpsByColumn[columnName] = opIndex + 1
					break
				}
			}
			if !found {
				isMatch = false
				break
			}
		}
		if !isMatch {
			continue
		}
		// Coverage before priority: a capability that leaves a predicate out hands it to the
		// database as an unindexed filter, which Scylla refuses. A packed key turning ProjectID into
		// an ID range (priority 400) must not beat the view that also covers Status.
		coveredPredicates := len(parts) / 2
		if bestMatch == nil || coveredPredicates > bestCoveredPredicates ||
			(coveredPredicates == bestCoveredPredicates && capability.Priority > bestMatch.Priority) {
			bestMatch, bestCoveredPredicates = capability, coveredPredicates
		}
	}

	if bestMatch != nil && ShouldLog() {
		sourceName := "base-key"
		if bestMatch.Source != nil {
			sourceName = bestMatch.Source.Name
		}
		fmt.Printf("Capability selected: signature=%s priority=%d source=%s isKey=%v\n",
			bestMatch.Signature, bestMatch.Priority, sourceName, bestMatch.IsKey)
	}
	return bestMatch
}

// maxFixedValueFanoutPerColumn is the widest declared value set the planner enumerates to fill a gap
// in an index key prefix.
const maxFixedValueFanoutPerColumn = 8

// MaxFixedValueFanoutQueries caps the product of the enumerated sets, which is how many parallel
// reads the fan-out costs. It matches the read concurrency limit, so a fanned-out read never
// queues against itself.
const MaxFixedValueFanoutQueries = 8

// buildFixedValueFanoutStatements synthesizes an IN predicate for every unconstrained column whose
// FixedValues enumerate a small, closed set. An index whose key prefix has a gap at such a column is
// still reachable: enumerating the column is logically identical to leaving it open, and each value
// group is one contiguous range over the key.
func (core *TableCore) buildFixedValueFanoutStatements(statements []ColumnStatement) []ColumnStatement {
	if len(core.FixedValueRanges) == 0 {
		return nil
	}
	constrainedColumns := map[string]bool{}
	for _, statement := range statements {
		constrainedColumns[statement.Col] = true
		for _, betweenStatement := range statement.From {
			constrainedColumns[betweenStatement.Col] = true
		}
	}

	fanoutStatements := []ColumnStatement{}
	plannedQueryCount := 1
	// Walking the table's columns rather than the FixedValueRanges map keeps the synthesized order
	// stable, which cached statement indexes depend on.
	for _, column := range core.Columns {
		columnName := column.GetName()
		if constrainedColumns[columnName] {
			continue
		}
		valueRange, isDeclared := core.FixedValueRanges[columnName]
		if !isDeclared || len(valueRange.DeclaredValues) == 0 ||
			len(valueRange.DeclaredValues) > maxFixedValueFanoutPerColumn ||
			plannedQueryCount*len(valueRange.DeclaredValues) > MaxFixedValueFanoutQueries {
			continue
		}
		plannedQueryCount *= len(valueRange.DeclaredValues)
		fanoutValues := make([]any, 0, len(valueRange.DeclaredValues))
		for _, declaredValue := range valueRange.DeclaredValues {
			fanoutValues = append(fanoutValues, NarrowInt64ToColumnWidth(declaredValue, column))
		}
		fanoutStatements = append(fanoutStatements, ColumnStatement{Col: columnName, Operator: "IN", Values: fanoutValues})
	}
	return fanoutStatements
}

// countSignatureCoveredQueryColumns counts the query's own predicate columns a capability
// constrains. Synthesized fan-out columns are not the caller's, so they buy no selectivity.
func countSignatureCoveredQueryColumns(capability *QueryCapability, statements []ColumnStatement) int {
	if capability == nil {
		return 0
	}
	signatureParts := strings.Split(capability.Signature, "|")
	coveredColumns := make(map[string]bool, len(signatureParts)/2)
	for partIndex := 0; partIndex < len(signatureParts); partIndex += 2 {
		coveredColumns[signatureParts[partIndex]] = true
	}
	coveredCount := 0
	for _, statement := range statements {
		if coveredColumns[statement.Col] {
			coveredCount++
		}
	}
	return coveredCount
}

// ApplyFixedValueFanout retries capability matching with the synthesized predicates and keeps them
// only when they let an index bind strictly more of the query's own predicates. It returns the
// capability to plan against and the predicates to append to the statement list.
func (core *TableCore) ApplyFixedValueFanout(statements []ColumnStatement, baseCapability *QueryCapability,
	capabilities []QueryCapability) (*QueryCapability, []ColumnStatement) {

	candidateStatements := core.buildFixedValueFanoutStatements(statements)
	if len(candidateStatements) == 0 {
		return baseCapability, nil
	}
	fannedCapability := MatchQueryCapability(append(slices.Clone(statements), candidateStatements...), capabilities)
	// A base-key capability is excluded: the key rewrites read equality and range predicates only,
	// so a synthesized IN would survive as a plain filter.
	if fannedCapability == nil || fannedCapability.Source == nil {
		return baseCapability, nil
	}
	// Coverage, not priority, decides: reaching a longer packed prefix by enumerating a gap costs
	// several reads and returns the very same rows. The fan-out only pays when it rescues a
	// predicate that would otherwise be left as a filter.
	if countSignatureCoveredQueryColumns(fannedCapability, statements) <=
		countSignatureCoveredQueryColumns(baseCapability, statements) {
		return baseCapability, nil
	}

	// A synthesized predicate the chosen source cannot bind would land among the remaining
	// statements as the very filter this avoids, so only the bindable ones are kept.
	fanoutStatements := []ColumnStatement{}
	fanoutColumnNames := []string{}
	for _, candidateStatement := range candidateStatements {
		if slices.Contains(fannedCapability.Source.Columns, candidateStatement.Col) {
			fanoutStatements = append(fanoutStatements, candidateStatement)
			fanoutColumnNames = append(fanoutColumnNames, candidateStatement.Col)
		}
	}
	if len(fanoutStatements) == 0 {
		return baseCapability, nil
	}

	fanoutQueryCount := 1
	for _, fanoutStatement := range fanoutStatements {
		fanoutQueryCount *= len(fanoutStatement.Values)
	}
	baseSignature := "none"
	if baseCapability != nil {
		baseSignature = baseCapability.Signature
	}
	fmt.Printf("FixedValues fanout applied: table=%s columns=%v queries=%d signature=%s (was %s, covering %d of %d predicates)\n",
		core.Name, fanoutColumnNames, fanoutQueryCount, fannedCapability.Signature, baseSignature,
		countSignatureCoveredQueryColumns(baseCapability, statements), len(statements))
	return fannedCapability, fanoutStatements
}
