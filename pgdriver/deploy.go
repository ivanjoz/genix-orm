package pgdriver

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// Deploy and homologation. Every controller's table is compiled (its panics surface here, at boot),
// then each PostgreSQL object it needs is created or brought to its declaration: missing columns and
// indexes are added, undeclared indexes dropped. A changed column type or primary key fails the
// deploy, and a changed index keeps its old definition (an index is matched by name): pre-alpha, the
// data is wiped and reseeded instead.

// deployStatementTimeout bounds one DDL statement: an index built on a populated table takes time.
const deployStatementTimeout = 15 * time.Minute

// Deploy creates or homologates the tables of every controller, plus each schema's _sequences. A
// second run changes nothing and prints only the summary.
func Deploy(controllers ...db.Controller) error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	tableSpecsBySchema := map[string][]tableSpec{connParams.Database: {sequencesTableSpec(connParams.Database)}}
	tablesWithFrames := []*PgTable{}
	for _, controller := range controllers {
		pgTable, isPgTable := controller.GetTable().(*PgTable)
		if !isPgTable {
			return fmt.Errorf("pgdriver: deploy: %q was not compiled by the postgres driver", controller.GetTableName())
		}
		schema := pgTable.mainTable.database
		tableSpecsBySchema[schema] = append(tableSpecsBySchema[schema], pgTable.mainTable)
		tableSpecsBySchema[schema] = append(tableSpecsBySchema[schema], pgTable.sideTables...)
		if pgTable.CacheByIDs && !slices.ContainsFunc(tableSpecsBySchema[schema], func(spec tableSpec) bool { return spec.name == cacheSlotsTableName }) {
			tableSpecsBySchema[schema] = append(tableSpecsBySchema[schema], cacheSlotsTableSpec(schema))
		}
		if pgTable.dataFrames != nil {
			tablesWithFrames = append(tablesWithFrames, pgTable)
			if !slices.ContainsFunc(tableSpecsBySchema[schema], func(spec tableSpec) bool { return spec.name == "_frame_shapes" }) {
				tableSpecsBySchema[schema] = append(tableSpecsBySchema[schema], frameShapesTableSpec(schema))
			}
		}
	}

	tableCount, createdCount, alteredCount := 0, 0, 0
	for schema, tableSpecs := range tableSpecsBySchema {
		if err := executeDeployStatements(pool, []string{createSchemaStatement(schema)}); err != nil {
			return err
		}
		describedTables, err := describeSchema(pool, schema)
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
	}
	fmt.Printf("pgdriver deploy: %d tables, %d created, %d altered, %d unchanged\n",
		tableCount, createdCount, alteredCount, tableCount-createdCount-alteredCount)
	// The aggregate tables are homologated: refill the ones whose frame changed shape.
	for _, pgTable := range tablesWithFrames {
		if err := pgTable.deployFrameAggregates(pool); err != nil {
			return err
		}
	}
	return nil
}

// describedTable is what PostgreSQL reports about one existing table.
type describedTable struct {
	columnTypes map[string]string
	primaryKey  []string
	indexNames  []string
}

// describeSchema reads every table of a schema: columns, primary key and index names.
func describeSchema(pool *sql.DB, schema string) (map[string]*describedTable, error) {
	describedTables := map[string]*describedTable{}
	tableOf := func(tableName string) *describedTable {
		if describedTables[tableName] == nil {
			describedTables[tableName] = &describedTable{columnTypes: map[string]string{}}
		}
		return describedTables[tableName]
	}
	queryRows := func(statement string, scanRow func(rows *sql.Rows) error) error {
		rows, err := queryStatement(pool, context.Background(), statement, schema)
		if err != nil {
			return fmt.Errorf("pgdriver: deploy: describing %s: %w", schema, err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := scanRow(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	err := queryRows(`SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = ? AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY c.relname, a.attnum`,
		func(rows *sql.Rows) error {
			var tableName, columnName, columnType string
			if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
				return err
			}
			tableOf(tableName).columnTypes[columnName] = columnType
			return nil
		})
	if err != nil {
		return nil, err
	}
	err = queryRows(`SELECT c.relname, i.relname, ix.indisprimary,
			COALESCE((SELECT array_to_string(array_agg(a.attname ORDER BY k.ordinality), ',')
				FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality)
				JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum), '')
		FROM pg_index ix JOIN pg_class c ON c.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ?`,
		func(rows *sql.Rows) error {
			var tableName, indexName, keyColumns string
			var isPrimary bool
			if err := rows.Scan(&tableName, &indexName, &isPrimary, &keyColumns); err != nil {
				return err
			}
			described := tableOf(tableName)
			if isPrimary {
				described.primaryKey = strings.Split(keyColumns, ",")
			} else {
				described.indexNames = append(described.indexNames, indexName)
			}
			return nil
		})
	return describedTables, err
}

// homologateTable creates a table that does not exist, or brings an existing one to its spec.
func homologateTable(pool *sql.DB, spec tableSpec, described *describedTable) (wasCreated, wasAltered bool, err error) {
	if described == nil {
		fmt.Printf("pgdriver deploy: creating %s (%d indexes)\n", spec.qualifiedName(), len(spec.indexes))
		return true, false, executeDeployStatements(pool, createTableStatements(spec))
	}
	statements, err := homologationStatements(spec, described)
	if err != nil || len(statements) == 0 {
		return false, false, err
	}
	for _, statement := range statements {
		fmt.Printf("pgdriver deploy: %s\n", statement)
	}
	return false, true, executeDeployStatements(pool, statements)
}

// homologationStatements are the DDL statements that bring a described table to its spec, or an
// error when the difference needs a wipe and reseed.
func homologationStatements(spec tableSpec, described *describedTable) ([]string, error) {
	tableName := spec.qualifiedName()
	if !slices.Equal(described.primaryKey, spec.primaryKey) {
		return nil, fmt.Errorf("pgdriver: deploy: %s has primary key (%s), the declaration needs (%s): wipe and reseed the table",
			tableName, strings.Join(described.primaryKey, ", "), strings.Join(spec.primaryKey, ", "))
	}
	statements := []string{}
	for _, column := range spec.columns {
		describedType, isDescribed := described.columnTypes[column.name]
		declaredType := strings.ToLower(strings.TrimSuffix(column.sqlType, ` COLLATE "C"`))
		if declaredType == "timestamptz" {
			declaredType = "timestamp with time zone"
		}
		switch {
		case !isDescribed:
			statements = append(statements, addColumnStatement(spec, column))
		case describedType != declaredType:
			return nil, fmt.Errorf("pgdriver: deploy: %s.%s is %s, the declaration needs %s: wipe and reseed the table",
				tableName, column.name, describedType, declaredType)
		}
	}
	for _, index := range spec.indexes {
		if !slices.Contains(described.indexNames, index.name) {
			statements = append(statements, createIndexStatement(spec, index))
		}
	}
	for _, indexName := range described.indexNames {
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
			return fmt.Errorf("pgdriver: deploy: %w\nstatement: %s", err, statement)
		}
	}
	return nil
}
