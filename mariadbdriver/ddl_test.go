package mariadbdriver

import (
	"strings"
	"testing"
)

func logTableSpec() tableSpec {
	return tableSpec{
		database: "genix", name: "user_logs",
		columns:             []columnSpec{{"date", "SMALLINT", true}, {"request_id", "BIGINT", true}},
		primaryKey:          []string{"date", "request_id"},
		logRetentionSeconds: 30 * 86_400,
	}
}

func TestALogTableIsCreatedInMyRocksWithItsRetention(t *testing.T) {
	statement := createTableStatement(logTableSpec())
	want := ") ENGINE = ROCKSDB COMMENT = 'ttl_duration=2592000;' DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin"
	if !strings.HasSuffix(statement, want) {
		t.Fatalf("statement:\n%s", statement)
	}
	plain := logTableSpec()
	plain.logRetentionSeconds = 0
	if strings.Contains(createTableStatement(plain), "ENGINE") {
		t.Fatal("a regular table names an engine")
	}
}

func TestALogTableIsMovedToMyRocksOrItsNewRetention(t *testing.T) {
	spec := logTableSpec()
	described := func(engine, comment string) *describedTable {
		return &describedTable{
			columnTypes: map[string]string{"date": "smallint", "request_id": "bigint"},
			columnOrder: []string{"date", "request_id"}, collation: tableCollation,
			engine: engine, comment: comment, primaryKey: spec.primaryKey, indexKeyParts: map[string][]string{},
		}
	}
	alter := "ALTER TABLE `genix`.`user_logs` ENGINE = ROCKSDB COMMENT = 'ttl_duration=2592000;'"
	for _, testCase := range []struct {
		engine, comment string
		want            []string
	}{
		{"InnoDB", "", []string{alter}},
		{"ROCKSDB", "ttl_duration=86400;", []string{alter}},
		{"ROCKSDB", "ttl_duration=2592000;", []string{}},
	} {
		statements, err := homologationStatements(spec, described(testCase.engine, testCase.comment))
		if err != nil || strings.Join(statements, "|") != strings.Join(testCase.want, "|") {
			t.Fatalf("%s %q: %v %v", testCase.engine, testCase.comment, statements, err)
		}
	}
}
