package obdriver

import "testing"

func productsSpec(partitionCount int) tableSpec {
	return tableSpec{
		database: "genix", name: "products",
		columns: []columnSpec{
			{"company_id", "INT", true}, {"id", "BIGINT", true}, {"category_id", "INT", false},
			{"code", keyedStringType, false}, {"tags", "JSON", false},
		},
		primaryKey: []string{"company_id", "id"},
		indexes: []indexSpec{
			{name: "ix_category", keyParts: []indexKeyPart{{column: "company_id"}, {column: "category_id"}}, storingColumns: []string{"code"}},
			{name: "ix_tags", keyParts: []indexKeyPart{{column: "company_id"}, {column: "tags", multiValueCast: "SIGNED"}}},
			{name: "ix_code", keyParts: []indexKeyPart{{column: "code"}}, isGlobal: true},
		},
		partitionColumn: "company_id", partitionCount: partitionCount,
	}
}

func TestCreateTableStatementGolden(t *testing.T) {
	const productsBody = "CREATE TABLE `genix`.`products` (\n" +
		"    `company_id` INT NOT NULL,\n" +
		"    `id` BIGINT NOT NULL,\n" +
		"    `category_id` INT,\n" +
		"    `code` VARCHAR(512),\n" +
		"    `tags` JSON,\n" +
		"    PRIMARY KEY (`company_id`, `id`),\n" +
		"    INDEX `ix_category` (`company_id`, `category_id`) STORING (`code`) LOCAL,\n" +
		"    INDEX `ix_tags` (`company_id`, (CAST(`tags`->'$' AS SIGNED ARRAY))) LOCAL,\n" +
		"    INDEX `ix_code` (`code`) GLOBAL\n" +
		") DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'"
	goldenStatements := map[string]string{
		"unpartitioned":                createTableStatement(productsSpec(1)),
		"8 partitions":                 createTableStatement(productsSpec(8)),
		"data frame log, 8 partitions": createTableStatement(dataFrameLogTableSpec("genix", "products", "BIGINT", 8)),
		"sequences":                    createTableStatement(sequencesTableSpec("genix")),
		"add index":                    addIndexStatement(productsSpec(1), productsSpec(1).indexes[1]),
		"drop index":                   dropIndexStatement(productsSpec(1), "ix_code"),
		"add column":                   addColumnStatement(productsSpec(1), columnSpec{"price", "DOUBLE", false}),
		"compression":                  alterCompressionStatement(productsSpec(1)),
	}
	expectedStatements := map[string]string{
		"unpartitioned": productsBody,
		"8 partitions":  productsBody + " PARTITION BY KEY(`company_id`) PARTITIONS 8",
		"data frame log, 8 partitions": "CREATE TABLE `genix`.`products__dtf_log` (\n" +
			"    `half_hour` INT NOT NULL,\n" +
			"    `partition_id` INT NOT NULL,\n" +
			"    `ts` INT NOT NULL,\n" +
			"    `record_id` BIGINT NOT NULL,\n" +
			"    `changes` LONGBLOB,\n" +
			"    `written_at` TIMESTAMP NOT NULL,\n" +
			"    PRIMARY KEY (`half_hour`, `partition_id`, `ts`, `record_id`)\n" +
			") DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'" +
			" TTL (`written_at` + INTERVAL 48 HOUR) PARTITION BY KEY(`partition_id`) PARTITIONS 8",
		"sequences": "CREATE TABLE `genix`.`_sequences` (\n" +
			"    `name` VARCHAR(512) NOT NULL,\n" +
			"    `value` BIGINT NOT NULL,\n" +
			"    PRIMARY KEY (`name`)\n" +
			") DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'",
		"add index":   "ALTER TABLE `genix`.`products` ADD INDEX `ix_tags` (`company_id`, (CAST(`tags`->'$' AS SIGNED ARRAY))) LOCAL",
		"drop index":  "ALTER TABLE `genix`.`products` DROP INDEX `ix_code`",
		"add column":  "ALTER TABLE `genix`.`products` ADD COLUMN `price` DOUBLE",
		"compression": "ALTER TABLE `genix`.`products` COMPRESSION = 'zstd_1.3.8'",
	}
	for name, expected := range expectedStatements {
		if goldenStatements[name] != expected {
			t.Errorf("%s:\n got: %s\nwant: %s", name, goldenStatements[name], expected)
		}
	}
}

// The deploy diff compares declared DDL with what information_schema reports.
func TestMetadataNormalization(t *testing.T) {
	columnTypes := map[string]string{
		"int(11)": "int", "bigint(20) unsigned": "bigint unsigned", "tinyint(1)": "bool", "BOOL": "bool",
		"tinyint(4)": "tinyint", "varchar(512)": "varchar(512)", "VARCHAR(512)": "varchar(512)", "longtext": "longtext",
		"INT UNSIGNED": "int unsigned", "int(10) unsigned": "int unsigned",
	}
	for described, expected := range columnTypes {
		if normalized := normalizeColumnType(described); normalized != expected {
			t.Errorf("normalizeColumnType(%q) = %q, want %q", described, normalized, expected)
		}
	}
	multiValuePart := normalizeKeyPart("SYS_NC_mvi_30",
		"json_query(`tags`,'$' RETURNING signed WITHOUT ARRAY WRAPPER asis error on error null on empty null on mismatch)")
	if multiValuePart != declaredKeyPart(indexKeyPart{column: "tags", multiValueCast: "SIGNED"}) || normalizeKeyPart("company_id", "") != "company_id" {
		t.Errorf("key part normalization: %q", multiValuePart)
	}
	// A multi-value index reports trailing primary-key parts and its __doc_id; they still match.
	multiValueIndex := productsSpec(1).indexes[1]
	if !indexMatchesDeclaration(multiValueIndex, []string{"company_id", "mv:tags:signed", "id", "__doc_id_17939"},
		createTableDetails{storingByIndex: map[string][]string{}, isGlobalByIndex: map[string]bool{}}, []string{"company_id", "id"}) {
		t.Error("a multi-value index with its hidden trailing parts must match its declaration")
	}
}

// Generated names longer than OceanBase's 64-character limit are cut and hashed, deterministically.
func TestBoundedIdentifier(t *testing.T) {
	const longName = "ix_warehouse_product_movement__pk_date_product_id_type_sub_divisor_rng_view"
	bounded := boundedIdentifier(longName)
	if len(bounded) != maxIdentifierLength || bounded[:55] != longName[:55] || bounded != boundedIdentifier(longName) ||
		bounded == boundedIdentifier(longName+"2") || boundedIdentifier("ix_short") != "ix_short" {
		t.Errorf("boundedIdentifier(%q) = %q", longName, bounded)
	}
}
