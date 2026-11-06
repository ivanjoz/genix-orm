package ydbdriver

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

// Deploy and homologation (docs/YDB_PLAN.md §8.6). Every controller's table is compiled (its panics
// surface here, at boot), then each YDB object it needs is created or brought to its declaration:
// missing columns and indexes are added, undeclared indexes dropped, partitioning and compression
// reset. Extra columns and extra side tables are reported, never dropped. A changed column type or
// primary key fails the deploy: pre-alpha, the data is wiped and reseeded instead.

// deployStatementTimeout bounds one DDL statement. An ADD INDEX returns once the index is built,
// so on a populated table it can take minutes.
const deployStatementTimeout = 15 * time.Minute

// Deploy creates or homologates the tables of every controller, plus the keyspace's _sequences. A
// second run changes nothing and prints only the summary.
func Deploy(controllers ...db.Controller) error {
	connection, err := sharedConnection()
	if err != nil {
		return err
	}
	tableSpecs := []tableSpec{sequencesTableSpec(Keyspace())}
	mainTableNames := []string{}
	for _, controller := range controllers {
		ydbTable, isYdbTable := controller.GetTable().(*YdbTable)
		if !isYdbTable {
			return fmt.Errorf("ydbdriver: deploy: %q was not compiled by the ydb driver", controller.GetTableName())
		}
		tableSpecs = append(tableSpecs, ydbTable.mainTable)
		tableSpecs = append(tableSpecs, ydbTable.sideTables...)
		mainTableNames = append(mainTableNames, ydbTable.Name)
	}

	existingTableNames, err := listKeyspaceTables(connection, Keyspace())
	if err != nil {
		return err
	}
	createdCount, alteredCount := 0, 0
	for _, spec := range tableSpecs {
		tableName := strings.TrimPrefix(spec.path, Keyspace()+"/")
		wasCreated, wasAltered, err := homologateTable(connection, spec, existingTableNames[tableName])
		if err != nil {
			return err
		}
		if wasCreated {
			createdCount++
		} else if wasAltered {
			alteredCount++
		}
	}

	// A side table nothing declares any more is reported, not dropped: it may hold the only copy of
	// something a renamed index still needs.
	declaredTableNames := map[string]bool{}
	for _, spec := range tableSpecs {
		declaredTableNames[strings.TrimPrefix(spec.path, Keyspace()+"/")] = true
	}
	for existingName := range existingTableNames {
		for _, mainTableName := range mainTableNames {
			if strings.HasPrefix(existingName, mainTableName+"__") && !declaredTableNames[existingName] {
				fmt.Printf("ydbdriver deploy: side table %q is not declared any more (kept)\n", existingName)
			}
		}
	}
	fmt.Printf("ydbdriver deploy: %d tables, %d created, %d altered, %d unchanged\n",
		len(tableSpecs), createdCount, alteredCount, len(tableSpecs)-createdCount-alteredCount)
	return nil
}

// listKeyspaceTables names the tables of the keyspace directory; none when it does not exist yet
// (CREATE TABLE creates it).
func listKeyspaceTables(connection *ydb.Driver, keyspace string) (map[string]bool, error) {
	listContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	tableNames := map[string]bool{}
	listing, err := connection.Scheme().ListDirectory(listContext, connection.Name()+"/"+keyspace)
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			return tableNames, nil
		}
		return nil, fmt.Errorf("ydbdriver: deploy: listing %s: %w", keyspace, err)
	}
	for _, child := range listing.Children {
		if child.IsTable() {
			tableNames[child.Name] = true
		}
	}
	return tableNames, nil
}

// homologateTable creates a table that does not exist, or brings an existing one to its spec.
func homologateTable(connection *ydb.Driver, spec tableSpec, exists bool) (wasCreated, wasAltered bool, err error) {
	if !exists {
		statements := []string{createTableStatement(spec)}
		for _, index := range spec.indexes {
			statements = append(statements, alterIndexPartitioningStatement(spec.path, index.name))
		}
		fmt.Printf("ydbdriver deploy: creating %s (%d indexes)\n", spec.path, len(spec.indexes))
		return true, false, executeDeployStatements(connection, statements)
	}

	description, err := describeTablePath(connection, spec.path)
	if err != nil {
		return false, false, err
	}
	statements, err := homologationStatements(connection, spec, description)
	if err != nil || len(statements) == 0 {
		return false, false, err
	}
	for _, statement := range statements {
		fmt.Printf("ydbdriver deploy: %s\n", statement)
	}
	return false, true, executeDeployStatements(connection, statements)
}

// homologationStatements are the DDL statements that bring a described table to its spec.
func homologationStatements(connection *ydb.Driver, spec tableSpec, description options.Description) ([]string, error) {
	if !slices.Equal(description.PrimaryKey, spec.primaryKey) {
		return nil, fmt.Errorf("ydbdriver: deploy: %s has primary key (%s), the declaration needs (%s): wipe and reseed the table",
			spec.path, strings.Join(description.PrimaryKey, ", "), strings.Join(spec.primaryKey, ", "))
	}

	statements := []string{}
	describedTypeByColumn := map[string]string{}
	for _, column := range description.Columns {
		describedTypeByColumn[column.Name] = strings.TrimSuffix(strings.TrimPrefix(column.Type.Yql(), "Optional<"), ">")
	}
	for _, column := range spec.columns {
		describedType, isDescribed := describedTypeByColumn[column.name]
		switch {
		case !isDescribed:
			statements = append(statements, addColumnStatement(spec.path, column))
		case describedType != column.ydbType:
			return nil, fmt.Errorf("ydbdriver: deploy: %s.%s is %s, the declaration needs %s: wipe and reseed the table",
				spec.path, column.name, describedType, column.ydbType)
		}
	}
	for _, column := range description.Columns {
		if !slices.ContainsFunc(spec.columns, func(declared columnSpec) bool { return declared.name == column.Name }) {
			fmt.Printf("ydbdriver deploy: %s.%s is not declared any more (kept)\n", spec.path, column.Name)
		}
	}

	if !slices.ContainsFunc(description.ColumnFamilies, func(family options.ColumnFamily) bool {
		return family.Name == "default" && family.Compression == options.ColumnFamilyCompressionLZ4
	}) {
		statements = append(statements, alterFamilyCompressionStatement(spec.path))
	}
	if !hasDeclaredPartitioning(description.PartitioningSettings) {
		statements = append(statements, alterTablePartitioningStatement(spec.path))
	}

	// An index cannot be altered in place (§8.3): a changed one is dropped and added again.
	describedIndexes := map[string]options.IndexDescription{}
	for _, describedIndex := range description.Indexes {
		describedIndexes[describedIndex.Name] = describedIndex
	}
	for _, index := range spec.indexes {
		describedIndex, isDescribed := describedIndexes[index.name]
		if isDescribed && indexMatchesDeclaration(describedIndex, index) {
			indexPartitioning, err := describeTablePath(connection, spec.path+"/"+index.name+"/indexImplTable")
			if err != nil {
				return nil, err
			}
			if !hasDeclaredPartitioning(indexPartitioning.PartitioningSettings) {
				statements = append(statements, alterIndexPartitioningStatement(spec.path, index.name))
			}
			continue
		}
		if isDescribed {
			statements = append(statements, dropIndexStatement(spec.path, index.name))
		}
		statements = append(statements, addIndexStatement(spec.path, index), alterIndexPartitioningStatement(spec.path, index.name))
	}
	for indexName := range describedIndexes {
		if !slices.ContainsFunc(spec.indexes, func(index nativeIndexSpec) bool { return index.name == indexName }) {
			statements = append(statements, dropIndexStatement(spec.path, indexName))
		}
	}
	return statements, nil
}

// indexMatchesDeclaration: same key in order, same covered columns in any order, async (Y6).
func indexMatchesDeclaration(describedIndex options.IndexDescription, index nativeIndexSpec) bool {
	describedCover, declaredCover := slices.Clone(describedIndex.DataColumns), slices.Clone(index.coverColumns)
	slices.Sort(describedCover)
	slices.Sort(declaredCover)
	return describedIndex.Type == options.IndexTypeGlobalAsync &&
		slices.Equal(describedIndex.IndexColumns, index.keyColumns) && slices.Equal(describedCover, declaredCover)
}

// hasDeclaredPartitioning compares described settings with Y20 (partitioningSettings in ddl.go).
func hasDeclaredPartitioning(settings options.PartitioningSettings) bool {
	return settings.PartitioningBySize == options.FeatureEnabled && settings.PartitioningByLoad == options.FeatureEnabled &&
		settings.PartitionSizeMb == 2000 && settings.MinPartitionsCount == 1 && settings.MaxPartitionsCount == 50
}

func describeTablePath(connection *ydb.Driver, relativePath string) (options.Description, error) {
	describeContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	var description options.Description
	err := connection.Table().Do(describeContext, func(sessionContext context.Context, session table.Session) (err error) {
		description, err = session.DescribeTable(sessionContext, connection.Name()+"/"+relativePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		return description, fmt.Errorf("ydbdriver: deploy: describing %s: %w", relativePath, err)
	}
	return description, nil
}

func executeDeployStatements(connection *ydb.Driver, statements []string) error {
	for _, statement := range statements {
		statementContext, cancel := context.WithTimeout(context.Background(), deployStatementTimeout)
		err := connection.Query().Exec(statementContext, statement)
		cancel()
		if err != nil {
			return fmt.Errorf("ydbdriver: deploy: %w\nstatement: %s", err, statement)
		}
	}
	return nil
}
