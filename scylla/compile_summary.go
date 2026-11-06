package scylla

import (
	"fmt"
	"slices"
	"strings"
)

// CompileSummary renders everything a table compiled to — columns, indexes and views with their
// CQL, the query capabilities and the neutral index state — as stable text. It exists to diff a
// compile before and after a refactor (backend/exec TestWriteCompileSummaries) and to debug how a
// schema was understood; nothing reads it at runtime.
func CompileSummary(table ScyllaTable) string {
	var summary strings.Builder
	fmt.Fprintf(&summary, "TABLE %v id=%v namespace=%v\n", table.Name, table.ID, table.Namespace)

	columnLines := []string{}
	for _, column := range table.ColumnsMap {
		columnType := column.GetType()
		columnLines = append(columnLines, fmt.Sprintf("  column %v idx=%v field=%v db=%v virtual=%v computed=%v",
			column.GetName(), column.GetInfo().Idx, columnType.FieldType, columnType.DBType, column.GetInfo().IsVirtual, column.GetInfo().IsComputed))
	}
	slices.Sort(columnLines)
	summary.WriteString(strings.Join(columnLines, "\n") + "\n")

	for _, view := range table.indexViews {
		fmt.Fprintf(&summary, "  source %v kind=%v type=%v idx=%v columns=%v noPart=%v columnsIdx=%v available=%v operators=%v postFilter=%v delta=%v fanout=%v packedSlots=%v packedInt32=%v\n",
			view.Name, view.Kind, view.Type, view.Idx, view.Columns, view.ColumnsNoPart, view.ColumnsIdx, view.AvailableColumns,
			view.Operators, view.RequiresPostFilter, view.IsDeltaView, view.FanoutColumnName, view.PackedSlotBitsPerColumn, view.PackedIsInt32)
		if view.getCreateScript != nil {
			fmt.Fprintf(&summary, "    create %v\n", view.getCreateScript())
		}
		if view.getExpectedColumns != nil {
			fmt.Fprintf(&summary, "    expects %v\n", view.getExpectedColumns())
		}
		if view.PackedIndex != nil {
			fmt.Fprintf(&summary, "    packed %+v\n", *view.PackedIndex)
		}
	}
	for _, capability := range table.capabilities {
		sourceName := ""
		if capability.Source != nil {
			sourceName = capability.Source.Name
		}
		fmt.Fprintf(&summary, "  capability %v source=%v priority=%v key=%v\n", capability.Signature, sourceName, capability.Priority, capability.IsKey)
	}
	for _, deltaIndex := range table.DeltaIndexes {
		fmt.Fprintf(&summary, "  delta %v pinnedSlots=%v view=%v threshold=%v\n", deltaIndex.Tag, deltaIndex.PinnedSlotBits, deltaIndex.ViewName, deltaIndex.ReplaceThreshold)
	}
	for _, group := range table.IndexGroups {
		baseGroupName := ""
		if group.BaseGroup != nil {
			baseGroupName = group.BaseGroup.Name
		}
		fmt.Fprintf(&summary, "  group %v id=%v virtual=%v collection=%v fromKey=%v s3=%v base=%v\n", group.Name, group.IndexID, group.VirtualColumn.GetName(), group.UsesCollectionValues, group.InheritFromKey, group.SnapshotInS3, baseGroupName)
	}
	for _, composite := range table.CompositeBucketIndexes {
		fmt.Fprintf(&summary, "  composite %v sizes=%v week=%v\n", composite.Name, composite.BucketSizes, composite.BucketIsWeek)
	}
	fmt.Fprintf(&summary, "  keyIntPackingSlots=%v keyConcatenated=%v\n", table.KeyIntPackingSlotBits, len(table.KeyConcatenated))
	return summary.String()
}
