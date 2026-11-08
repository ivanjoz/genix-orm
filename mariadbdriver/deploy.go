package mariadbdriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// Deploy and homologation. Every controller's table is compiled (its panics surface here, at boot),
// then each MariaDB object it needs is created or brought to its declaration: missing columns and
// indexes are added, changed indexes rebuilt, undeclared indexes dropped. Extra columns and extra side
// tables are reported, never dropped. A changed column type, primary key or collation fails the
// deploy: pre-alpha, the data is wiped and reseeded instead.

// deployStatementTimeout bounds one DDL statement. An ADD INDEX returns once the index is built,
// so on a populated table it can take minutes.
const deployStatementTimeout = 15 * time.Minute

// Deploy creates or homologates the tables of every controller, plus each database's _sequences. A
// second run changes nothing and prints only the summary.
func Deploy(controllers ...db.Controller) error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	tableSpecsByDatabase := map[string][]tableSpec{connParams.Database: {sequencesTableSpec(connParams.Database)}}
	mainTableNamesByDatabase := map[string][]string{}
	tablesWithFrames := []*MariaTable{}
	for _, controller := range controllers {
		mariaTable, isObTable := controller.GetTable().(*MariaTable)
		if !isObTable {
			return fmt.Errorf("mariadbdriver: deploy: %q was not compiled by the mariadb driver", controller.GetTableName())
		}
		database := mariaTable.mainTable.database
		tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], mariaTable.mainTable)
		tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], mariaTable.sideTables...)
		mainTableNamesByDatabase[database] = append(mainTableNamesByDatabase[database], mariaTable.Name)
		if mariaTable.CacheByIDs && !slices.ContainsFunc(tableSpecsByDatabase[database], func(spec tableSpec) bool { return spec.name == cacheSlotsTableName }) {
			tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], cacheSlotsTableSpec(database))
		}
		if mariaTable.dataFrames != nil {
			tablesWithFrames = append(tablesWithFrames, mariaTable)
			if !slices.ContainsFunc(tableSpecsByDatabase[database], func(spec tableSpec) bool { return spec.name == "_frame_shapes" }) {
				tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], frameShapesTableSpec(database))
			}
		}
	}

	if err := requireLogStorageEngine(pool, tableSpecsByDatabase); err != nil {
		return err
	}
	tableCount, createdCount, alteredCount := 0, 0, 0
	for database, tableSpecs := range tableSpecsByDatabase {
		if err := executeDeployStatements(pool, []string{createDatabaseStatement(database)}); err != nil {
			return err
		}
		describedTables, err := describeDatabase(pool, database)
		if err != nil {
			return err
		}
		for _, spec := range tableSpecs {
			wasCreated, wasAltered, err := homologateTable(pool, spec, describedTables[spec.name])
			if err != nil {
				return err
			}
			tableCount++
			if wasCreated {
				createdCount++
			} else if wasAltered {
				alteredCount++
			}
		}
		// A side table nothing declares any more is reported, not dropped: it may hold the only copy
		// of something a renamed index still needs.
		for existingName := range describedTables {
			isDeclared := slices.ContainsFunc(tableSpecs, func(spec tableSpec) bool { return spec.name == existingName })
			for _, mainTableName := range mainTableNamesByDatabase[database] {
				if !isDeclared && strings.HasPrefix(existingName, mainTableName+"__") {
					fmt.Printf("mariadbdriver deploy: side table %s.%s is not declared any more (kept)\n", database, existingName)
				}
			}
		}
	}
	fmt.Printf("mariadbdriver deploy: %d tables, %d created, %d altered, %d unchanged\n",
		tableCount, createdCount, alteredCount, tableCount-createdCount-alteredCount)
	// The aggregate tables are homologated: refill the ones whose frame changed shape.
	for _, mariaTable := range tablesWithFrames {
		if err := mariaTable.deployFrameAggregates(pool); err != nil {
			return err
		}
	}
	return nil
}

// describedTable is what MariaDB reports about one existing table.
type describedTable struct {
	columnTypes   map[string]string // normalized, see normalizeColumnType
	columnOrder   []string
	collation     string
	engine        string
	comment       string
	primaryKey    []string
	indexKeyParts map[string][]string
}

// describeDatabase reads every table of a database in three statements.
func describeDatabase(pool *sql.DB, database string) (map[string]*describedTable, error) {
	describedTables := map[string]*describedTable{}
	queryRows := func(statement string, scanRow func(rows *sql.Rows) error) error {
		rows, err := pool.Query(statement, database)
		if err != nil {
			return fmt.Errorf("mariadbdriver: deploy: describing %s: %w", database, err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := scanRow(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	err := queryRows("SELECT table_name, table_collation, engine, table_comment FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
		func(rows *sql.Rows) error {
			var tableName string
			var collation, engine, comment sql.NullString
			if err := rows.Scan(&tableName, &collation, &engine, &comment); err != nil {
				return err
			}
			describedTables[tableName] = &describedTable{columnTypes: map[string]string{}, collation: collation.String,
				engine: engine.String, comment: comment.String, indexKeyParts: map[string][]string{}}
			return nil
		})
	if err != nil {
		return nil, err
	}
	err = queryRows("SELECT table_name, column_name, column_type FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position",
		func(rows *sql.Rows) error {
			var tableName, columnName, columnType string
			if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
				return err
			}
			if described := describedTables[tableName]; described != nil {
				described.columnTypes[columnName] = normalizeColumnType(columnType)
				described.columnOrder = append(described.columnOrder, columnName)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	err = queryRows("SELECT table_name, index_name, column_name FROM information_schema.statistics WHERE table_schema = ? ORDER BY table_name, index_name, seq_in_index",
		func(rows *sql.Rows) error {
			var tableName, indexName, columnName string
			if err := rows.Scan(&tableName, &indexName, &columnName); err != nil {
				return err
			}
			described := describedTables[tableName]
			if described == nil {
				return nil
			}
			if indexName == "PRIMARY" {
				described.primaryKey = append(described.primaryKey, columnName)
			} else {
				described.indexKeyParts[indexName] = append(described.indexKeyParts[indexName], columnName)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return describedTables, err
}

var integerDisplayWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\(\d+\)`)

// normalizeColumnType makes a declared type and an information_schema COLUMN_TYPE comparable:
// lower case, no integer display width, BOOL for tinyint(1), and JSON as the LONGTEXT MariaDB
// stores it as.
func normalizeColumnType(columnType string) string {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	switch columnType {
	case "tinyint(1)", "boolean":
		return "bool"
	case "json":
		return "longtext"
	}
	return integerDisplayWidth.ReplaceAllString(columnType, "$1")
}

// homologateTable creates a table that does not exist, or brings an existing one to its spec.
func homologateTable(pool *sql.DB, spec tableSpec, described *describedTable) (wasCreated, wasAltered bool, err error) {
	if described == nil {
		fmt.Printf("mariadbdriver deploy: creating %s (%d indexes)\n", spec.qualifiedName(), len(spec.indexes))
		return true, false, executeDeployStatements(pool, []string{createTableStatement(spec)})
	}
	statements, err := homologationStatements(spec, described)
	if err != nil || len(statements) == 0 {
		return false, false, err
	}
	for _, statement := range statements {
		fmt.Printf("mariadbdriver deploy: %s\n", statement)
	}
	return false, true, executeDeployStatements(pool, statements)
}

// homologationStatements are the DDL statements that bring a described table to its spec, or an
// error when the difference needs a wipe and reseed.
func homologationStatements(spec tableSpec, described *describedTable) ([]string, error) {
	tableName := spec.qualifiedName()
	if !slices.Equal(described.primaryKey, spec.primaryKey) {
		return nil, fmt.Errorf("mariadbdriver: deploy: %s has primary key (%s), the declaration needs (%s): wipe and reseed the table",
			tableName, strings.Join(described.primaryKey, ", "), strings.Join(spec.primaryKey, ", "))
	}
	if described.collation != tableCollation {
		return nil, fmt.Errorf("mariadbdriver: deploy: %s has collation %s, the declaration needs %s: wipe and reseed the table",
			tableName, described.collation, tableCollation)
	}

	statements := []string{}
	// A Log table on another engine (created before it was declared one) or with another retention is
	// moved in place: its rows are a log, copying them is cheap and nothing reads them transactionally.
	if spec.isLog() && (!strings.EqualFold(described.engine, logStorageEngine) || described.comment != spec.logTableComment()) {
		statements = append(statements, logStorageStatement(spec))
	}
	for _, column := range spec.columns {
		describedType, isDescribed := described.columnTypes[column.name]
		switch {
		case !isDescribed:
			statements = append(statements, addColumnStatement(spec, column))
		case describedType != normalizeColumnType(column.sqlType):
			return nil, fmt.Errorf("mariadbdriver: deploy: %s.%s is %s, the declaration needs %s: wipe and reseed the table",
				tableName, column.name, describedType, strings.ToLower(column.sqlType))
		}
	}
	for _, columnName := range described.columnOrder {
		if !slices.ContainsFunc(spec.columns, func(declared columnSpec) bool { return declared.name == columnName }) {
			fmt.Printf("mariadbdriver deploy: %s.%s is not declared any more (kept)\n", tableName, columnName)
		}
	}

	// A changed index is dropped and added again (§8.3).
	for _, index := range spec.indexes {
		describedKeyParts, isDescribed := described.indexKeyParts[index.name]
		if isDescribed && slices.Equal(describedKeyParts, index.columnNames()) {
			continue
		}
		if isDescribed {
			statements = append(statements, dropIndexStatement(spec, index.name))
		}
		statements = append(statements, addIndexStatement(spec, index))
	}
	for indexName := range described.indexKeyParts {
		if !slices.ContainsFunc(spec.indexes, func(index indexSpec) bool { return index.name == indexName }) {
			statements = append(statements, dropIndexStatement(spec, indexName))
		}
	}
	return statements, nil
}

func executeDeployStatements(pool *sql.DB, statements []string) error {
	for _, statement := range statements {
		statementContext, cancel := context.WithTimeout(context.Background(), deployStatementTimeout)
		_, err := pool.ExecContext(statementContext, statement)
		cancel()
		if err != nil {
			return fmt.Errorf("mariadbdriver: deploy: %w\nstatement: %s", err, statement)
		}
	}
	return nil
}

// requireLogStorageEngine fails the deploy when a Log table is declared and the server has no MyRocks:
// created as InnoDB instead, the table would be uncompressed and its rows would never expire.
func requireLogStorageEngine(pool *sql.DB, tableSpecsByDatabase map[string][]tableSpec) error {
	logTableName := ""
	for _, tableSpecs := range tableSpecsByDatabase {
		for _, spec := range tableSpecs {
			if spec.isLog() {
				logTableName = spec.qualifiedName()
			}
		}
	}
	if logTableName == "" {
		return nil
	}
	var support string
	err := pool.QueryRow("SELECT support FROM information_schema.engines WHERE engine = ?", logStorageEngine).Scan(&support)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("mariadbdriver: deploy: reading the storage engines: %w", err)
	}
	if support != "YES" && support != "DEFAULT" {
		return fmt.Errorf("mariadbdriver: deploy: %s is a log table and needs the %s storage engine, which this server does not have: "+
			"install MariaDB's RocksDB plugin (dnf install mariadb-rocksdb-engine, apt-get install mariadb-plugin-rocksdb) and restart MariaDB",
			logTableName, logStorageEngine)
	}
	return nil
}
