package obdriver

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// Deploy and homologation (docs/OCEANBASE_PLAN.md §8.5). Every controller's table is compiled (its
// panics surface here, at boot), then each OceanBase object it needs is created or brought to its
// declaration: missing columns and indexes are added, changed indexes rebuilt, undeclared indexes
// dropped, compression and TTL reset. Extra columns and extra side tables are reported, never
// dropped. A changed column type, primary key, collation or partition count fails the deploy:
// pre-alpha, the data is wiped and reseeded instead.

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
	if connParams.Engine != "" && connParams.Engine != EngineSeekDB {
		return fmt.Errorf("obdriver: deploy: unknown engine %q (\"\" or %q)", connParams.Engine, EngineSeekDB)
	}
	tableSpecsByDatabase := map[string][]tableSpec{connParams.Database: {sequencesTableSpec(connParams.Database)}}
	mainTableNamesByDatabase := map[string][]string{}
	for _, controller := range controllers {
		obTable, isObTable := controller.GetTable().(*ObTable)
		if !isObTable {
			return fmt.Errorf("obdriver: deploy: %q was not compiled by the oceanbase driver", controller.GetTableName())
		}
		database := obTable.mainTable.database
		tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], obTable.mainTable)
		tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], obTable.sideTables...)
		mainTableNamesByDatabase[database] = append(mainTableNamesByDatabase[database], obTable.Name)
		if obTable.CacheByIDs && !slices.ContainsFunc(tableSpecsByDatabase[database], func(spec tableSpec) bool { return spec.name == cacheSlotsTableName }) {
			tableSpecsByDatabase[database] = append(tableSpecsByDatabase[database], cacheSlotsTableSpec(database))
		}
	}
	needsTTL := false
	for _, tableSpecs := range tableSpecsByDatabase {
		for position := range tableSpecs {
			tableSpecs[position] = fitToEngine(tableSpecs[position])
			needsTTL = needsTTL || tableSpecs[position].ttlColumn != ""
		}
	}
	if needsTTL {
		if err := assertTenantTTLEnabled(pool); err != nil {
			return err
		}
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
					fmt.Printf("obdriver deploy: side table %s.%s is not declared any more (kept)\n", database, existingName)
				}
			}
		}
	}
	fmt.Printf("obdriver deploy: %d tables, %d created, %d altered, %d unchanged\n",
		tableCount, createdCount, alteredCount, tableCount-createdCount-alteredCount)
	return nil
}

// fitToEngine drops from a table what the connected engine cannot create: on seekdb, the column
// store and the TTL.
func fitToEngine(spec tableSpec) tableSpec {
	if connParams.Engine == EngineSeekDB {
		spec.isColumnStore = false
		spec.ttlColumn = ""
	}
	return spec
}

// assertTenantTTLEnabled is D4: the DataFrame log relies on table TTL, which deletes rows only when
// both tenant parameters are on.
func assertTenantTTLEnabled(pool *sql.DB) error {
	rows, err := pool.Query("SELECT name, value FROM oceanbase.GV$OB_PARAMETERS WHERE name IN ('enable_ttl', 'enable_kv_ttl')")
	if err != nil {
		return fmt.Errorf("obdriver: deploy: reading TTL parameters: %w", err)
	}
	defer rows.Close()
	enabledParameters := map[string]bool{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		enabledParameters[name] = strings.EqualFold(value, "true")
	}
	if !enabledParameters["enable_ttl"] || !enabledParameters["enable_kv_ttl"] {
		return fmt.Errorf("obdriver: deploy: the DataFrame log needs table TTL; run on the tenant: ALTER SYSTEM SET enable_ttl = true; ALTER SYSTEM SET enable_kv_ttl = true")
	}
	return rows.Err()
}

// describedTable is what OceanBase reports about one existing table.
type describedTable struct {
	columnTypes    map[string]string // normalized, see normalizeColumnType
	columnOrder    []string
	collation      string
	partitionCount int
	primaryKey     []string
	// indexKeyParts are normalized (see normalizeKeyPart) and keep the hidden trailing parts a
	// multi-value index reports; indexMatchesDeclaration ignores those.
	indexKeyParts map[string][]string
}

// describeDatabase reads every table of a database in four statements. STORING, GLOBAL, compression
// and TTL appear only in SHOW CREATE TABLE, which homologateTable reads per table.
func describeDatabase(pool *sql.DB, database string) (map[string]*describedTable, error) {
	describedTables := map[string]*describedTable{}
	queryRows := func(statement string, scanRow func(rows *sql.Rows) error) error {
		rows, err := pool.Query(statement, database)
		if err != nil {
			return fmt.Errorf("obdriver: deploy: describing %s: %w", database, err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := scanRow(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	err := queryRows("SELECT table_name, table_collation FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
		func(rows *sql.Rows) error {
			var tableName string
			var collation sql.NullString
			if err := rows.Scan(&tableName, &collation); err != nil {
				return err
			}
			describedTables[tableName] = &describedTable{columnTypes: map[string]string{}, collation: collation.String, indexKeyParts: map[string][]string{}}
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
	err = queryRows("SELECT table_name, index_name, column_name, expression FROM information_schema.statistics WHERE table_schema = ? ORDER BY table_name, index_name, seq_in_index",
		func(rows *sql.Rows) error {
			var tableName, indexName, columnName string
			var expression sql.NullString
			if err := rows.Scan(&tableName, &indexName, &columnName, &expression); err != nil {
				return err
			}
			described := describedTables[tableName]
			if described == nil {
				return nil
			}
			if indexName == "PRIMARY" {
				described.primaryKey = append(described.primaryKey, columnName)
			} else {
				described.indexKeyParts[indexName] = append(described.indexKeyParts[indexName], normalizeKeyPart(columnName, expression.String))
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	err = queryRows("SELECT table_name, COUNT(*) FROM information_schema.partitions WHERE table_schema = ? GROUP BY table_name",
		func(rows *sql.Rows) error {
			var tableName string
			var partitionCount int
			if err := rows.Scan(&tableName, &partitionCount); err != nil {
				return err
			}
			if described := describedTables[tableName]; described != nil {
				described.partitionCount = partitionCount
			}
			return nil
		})
	return describedTables, err
}

var (
	integerDisplayWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\(\d+\)`)
	// A multi-value key part is reported as json_query(`col`,'$' RETURNING <type> WITHOUT ARRAY WRAPPER …).
	multiValueExpression = regexp.MustCompile("^json_query\\(`([^`]+)`,'\\$' RETURNING (\\w+)")
)

// normalizeColumnType makes a declared type and an information_schema COLUMN_TYPE comparable:
// lower case, no integer display width, BOOL for tinyint(1).
func normalizeColumnType(columnType string) string {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	if columnType == "tinyint(1)" {
		return "bool"
	}
	return integerDisplayWidth.ReplaceAllString(columnType, "$1")
}

// normalizeKeyPart names a described key part like declaredKeyPart names a declared one.
func normalizeKeyPart(columnName, expression string) string {
	if match := multiValueExpression.FindStringSubmatch(expression); match != nil {
		return "mv:" + match[1] + ":" + strings.ToLower(match[2])
	}
	return columnName
}

func declaredKeyPart(part indexKeyPart) string {
	if part.multiValueCast == "" {
		return part.column
	}
	return "mv:" + part.column + ":" + strings.ToLower(part.multiValueCast)
}

// createTableDetails is what only SHOW CREATE TABLE reports.
type createTableDetails struct {
	compression     string
	hasTTL          bool
	isColumnStore   bool
	storingByIndex  map[string][]string
	isGlobalByIndex map[string]bool
}

var (
	compressionOption = regexp.MustCompile(`COMPRESSION = '([^']*)'`)
	indexLine         = regexp.MustCompile("^\\s*(?:UNIQUE\\s+)?(?:KEY|INDEX)\\s+`([^`]+)`")
	storingClause     = regexp.MustCompile(`STORING \(([^)]*)\)`)
	globalSuffix      = regexp.MustCompile(`\sGLOBAL,?$`)
)

func readCreateTableDetails(pool *sql.DB, spec tableSpec) (createTableDetails, error) {
	var tableName, createStatement string
	if err := pool.QueryRow("SHOW CREATE TABLE "+spec.qualifiedName()).Scan(&tableName, &createStatement); err != nil {
		return createTableDetails{}, fmt.Errorf("obdriver: deploy: SHOW CREATE TABLE %s: %w", spec.qualifiedName(), err)
	}
	details := createTableDetails{storingByIndex: map[string][]string{}, isGlobalByIndex: map[string]bool{}}
	if match := compressionOption.FindStringSubmatch(createStatement); match != nil {
		details.compression = match[1]
	}
	details.hasTTL = strings.Contains(createStatement, " TTL = ")
	details.isColumnStore = strings.Contains(createStatement, "WITH COLUMN GROUP(")
	for _, line := range strings.Split(createStatement, "\n") {
		match := indexLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		if storing := storingClause.FindStringSubmatch(line); storing != nil {
			for _, columnName := range strings.Split(storing[1], ",") {
				details.storingByIndex[match[1]] = append(details.storingByIndex[match[1]], strings.Trim(strings.TrimSpace(columnName), "`"))
			}
		}
		details.isGlobalByIndex[match[1]] = globalSuffix.MatchString(strings.TrimSpace(line))
	}
	return details, nil
}

// homologateTable creates a table that does not exist, or brings an existing one to its spec.
func homologateTable(pool *sql.DB, spec tableSpec, described *describedTable) (wasCreated, wasAltered bool, err error) {
	if described == nil {
		fmt.Printf("obdriver deploy: creating %s (%d indexes)\n", spec.qualifiedName(), len(spec.indexes))
		return true, false, executeDeployStatements(pool, []string{createTableStatement(spec)})
	}
	details, err := readCreateTableDetails(pool, spec)
	if err != nil {
		return false, false, err
	}
	statements, err := homologationStatements(spec, described, details)
	if err != nil || len(statements) == 0 {
		return false, false, err
	}
	for _, statement := range statements {
		fmt.Printf("obdriver deploy: %s\n", statement)
	}
	return false, true, executeDeployStatements(pool, statements)
}

// homologationStatements are the DDL statements that bring a described table to its spec, or an
// error when the difference needs a wipe and reseed.
func homologationStatements(spec tableSpec, described *describedTable, details createTableDetails) ([]string, error) {
	tableName := spec.qualifiedName()
	if !slices.Equal(described.primaryKey, spec.primaryKey) {
		return nil, fmt.Errorf("obdriver: deploy: %s has primary key (%s), the declaration needs (%s): wipe and reseed the table",
			tableName, strings.Join(described.primaryKey, ", "), strings.Join(spec.primaryKey, ", "))
	}
	if described.collation != tableCollation {
		return nil, fmt.Errorf("obdriver: deploy: %s has collation %s, the declaration needs %s: wipe and reseed the table",
			tableName, described.collation, tableCollation)
	}
	declaredPartitionCount := 1
	if spec.partitionColumn != "" && spec.partitionCount > 1 {
		declaredPartitionCount = spec.partitionCount
	}
	if described.partitionCount != declaredPartitionCount {
		return nil, fmt.Errorf("obdriver: deploy: %s has %d partitions, the configuration needs %d: wipe and reseed the table",
			tableName, described.partitionCount, declaredPartitionCount)
	}

	statements := []string{}
	for _, column := range spec.columns {
		describedType, isDescribed := described.columnTypes[column.name]
		switch {
		case !isDescribed:
			statements = append(statements, addColumnStatement(spec, column))
		case describedType != normalizeColumnType(column.sqlType):
			return nil, fmt.Errorf("obdriver: deploy: %s.%s is %s, the declaration needs %s: wipe and reseed the table",
				tableName, column.name, describedType, strings.ToLower(column.sqlType))
		}
	}
	for _, columnName := range described.columnOrder {
		if !slices.ContainsFunc(spec.columns, func(declared columnSpec) bool { return declared.name == columnName }) {
			fmt.Printf("obdriver deploy: %s.%s is not declared any more (kept)\n", tableName, columnName)
		}
	}
	if details.compression != tableCompression {
		statements = append(statements, alterCompressionStatement(spec))
	}
	if spec.ttlColumn != "" && !details.hasTTL {
		statements = append(statements, alterTTLStatement(spec))
	}
	switch {
	case spec.isColumnStore && !details.isColumnStore:
		statements = append(statements, addColumnGroupStatement(spec))
	case !spec.isColumnStore && details.isColumnStore:
		return nil, fmt.Errorf("obdriver: deploy: %s keeps a column store the declaration dropped: wipe and reseed the table", tableName)
	}

	// A changed index is dropped and added again (§8.3).
	for _, index := range spec.indexes {
		describedKeyParts, isDescribed := described.indexKeyParts[index.name]
		if isDescribed && indexMatchesDeclaration(index, describedKeyParts, details, spec.primaryKey) {
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

// indexMatchesDeclaration: the declared key parts in order, then only the hidden parts a multi-value
// index appends (primary-key columns and its __doc_id); the same STORING columns in any order; the
// same LOCAL / GLOBAL.
func indexMatchesDeclaration(index indexSpec, describedKeyParts []string, details createTableDetails, primaryKey []string) bool {
	if len(describedKeyParts) < len(index.keyParts) {
		return false
	}
	for partIndex, part := range index.keyParts {
		if describedKeyParts[partIndex] != declaredKeyPart(part) {
			return false
		}
	}
	for _, trailingPart := range describedKeyParts[len(index.keyParts):] {
		if !slices.Contains(primaryKey, trailingPart) && !strings.HasPrefix(trailingPart, "__doc_id") {
			return false
		}
	}
	describedStoring, declaredStoring := slices.Clone(details.storingByIndex[index.name]), slices.Clone(index.storingColumns)
	slices.Sort(describedStoring)
	slices.Sort(declaredStoring)
	return slices.Equal(describedStoring, declaredStoring) && details.isGlobalByIndex[index.name] == index.isGlobal
}

func executeDeployStatements(pool *sql.DB, statements []string) error {
	for _, statement := range statements {
		statementContext, cancel := context.WithTimeout(context.Background(), deployStatementTimeout)
		_, err := pool.ExecContext(statementContext, statement)
		cancel()
		if err != nil {
			return fmt.Errorf("obdriver: deploy: %w\nstatement: %s", err, statement)
		}
	}
	return nil
}
