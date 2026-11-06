package ydbdriver

import "testing"

// Every statement ddl.go can generate must be accepted by the real server, not only match a golden string.
func TestGeneratedDDLIsAcceptedByYdb(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)

	productsPath := directory + "/products"
	products := tableSpec{
		path: productsPath,
		columns: []columnSpec{
			{"company_id", "Int32", true}, {"id", "Int64", true}, {"category_id", "Int32", false},
			{"name", "Utf8", false}, {"updated", "Int32", false}, {"zz_dlt_ss", "Uint64", false},
		},
		primaryKey: []string{"company_id", "id"},
		indexes: []nativeIndexSpec{
			{name: "ix_category", keyColumns: []string{"company_id", "category_id"}, coverColumns: []string{"name"}},
			{name: "ix_delta", keyColumns: []string{"company_id", "zz_dlt_ss"}, coverColumns: []string{"category_id", "name", "updated"}},
		},
	}
	spikeMustExec(t, connection, createTableStatement(products))
	for _, index := range products.indexes {
		spikeMustExec(t, connection, alterIndexPartitioningStatement(productsPath, index.name))
	}
	spikeMustExec(t, connection, alterTablePartitioningStatement(productsPath))
	spikeMustExec(t, connection, alterFamilyCompressionStatement(productsPath))
	spikeMustExec(t, connection, addColumnStatement(productsPath, columnSpec{"extra", "Utf8", false}))

	partition := columnSpec{"company_id", "Int32", true}
	sideTables := []tableSpec{
		sliceSideTableSpec(productsPath, "tags", &partition, "Int32", []columnSpec{{"status", "Int16", true}}, []columnSpec{{"id", "Int64", true}}),
		sliceSideTableSpec(productsPath, "global_tags", nil, "Utf8", nil, []columnSpec{partition, {"id", "Int64", true}}),
		deltaMovesTableSpec(productsPath, "Int64"),
		indexUpdatedTableSpec(productsPath),
		dataFrameLogTableSpec(productsPath, "Int64"),
		sequencesTableSpec(directory),
	}
	for _, sideTable := range sideTables {
		spikeMustExec(t, connection, createTableStatement(sideTable))
	}

	// DescribeTable must read back what the DDL asked for (the basis of homologation, §8.6).
	description := spikeDescribe(t, connection, productsPath)
	if len(description.Indexes) != 2 || description.ColumnFamilies[0].Compression.String() != "lz4" {
		t.Fatalf("describe: indexes=%d families=%+v", len(description.Indexes), description.ColumnFamilies)
	}
	if description.PartitioningSettings.MaxPartitionsCount != 50 {
		t.Fatalf("partitioning = %+v", description.PartitioningSettings)
	}
	spikeLog(t, "ddl: every generated statement accepted; describe reads back 2 indexes, lz4, max 50 partitions")
}
