package db

import (
	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/dataframe/framesql"
)

// FrameSource is the FrameSQL Source of one partition of a table's DataFrame
// (dataframe/framesql/FRAMESQL.md). It always reads fresh: an answer includes the records written
// since the last compaction, and the read compacts the partition when its CompactOn is due, as any
// other fresh read does.
func FrameSource[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	partition int32, frameName string,
) framesql.Source {
	return func(selection dataframe.Selection) ([]dataframe.Row, error) {
		query := QueryFrame[RecordT, TableT, D](partition, frameName).Fresh()
		query.request.Selection = &selection
		rows, _, err := query.ExecRows()
		return rows, err
	}
}
