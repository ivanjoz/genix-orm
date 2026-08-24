package scylla

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// A table declared entirely through the shared db package, which is the shape app
// code migrates to. Only TableStruct still comes from this driver, because query
// execution is not behind the driver interface yet.
type sharedSchemaRecord struct {
	TableStruct[sharedSchemaTable, sharedSchemaRecord]
	EmpresaID int32
	ID        int32
	Nombre    string
	Tags      []string
	Status    int8
	Updated   int32
}

type sharedSchemaTable struct {
	TableStruct[sharedSchemaTable, sharedSchemaRecord]
	EmpresaID db.Col[*sharedSchemaTable, int32]
	ID        db.Col[*sharedSchemaTable, int32]
	Nombre    db.Col[*sharedSchemaTable, string]
	Tags      db.ColSlice[*sharedSchemaTable, string]
	Status    db.Col[*sharedSchemaTable, int8]
	Updated   db.Col[*sharedSchemaTable, int32]
}

func (e sharedSchemaTable) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        10024,
		Name:      "shared_schema_probe",
		Partition: e.EmpresaID,
		Keys:      db.Cols(e.ID.Autoincrement(0)),
		Indexes: []db.Index{
			// The first view key carries no DecimalSize: the ORM infers its width from
			// the remaining columns and rejects an explicit one.
			{Type: db.TypeView, Keys: db.Cols(e.Status, e.Updated.DecimalSize(10))},
			{Type: db.TypeLocalIndex, Keys: db.Cols(e.Nombre)},
		},
	}
}

// Compiling the table is what resolves every column and index, so this asserts the
// shared declaration types produce the same Scylla metadata the driver-local ones did.
func TestSchemaDeclaredWithSharedTypesCompiles(t *testing.T) {
	table := MakeScyllaTable[sharedSchemaRecord, sharedSchemaTable]()

	if table.GetName() != "shared_schema_probe" {
		t.Fatalf("table name = %q", table.GetName())
	}
	if partitionKey := table.GetPartKey(); partitionKey == nil || partitionKey.GetName() != "empresa_id" {
		t.Fatalf("partition column = %v", partitionKey)
	}
	if keys := table.GetKeys(); len(keys) != 1 || keys[0].GetName() != "id" {
		t.Fatalf("key columns = %v", keys)
	}

	// A ColSlice column must land as an addressable CQL collection, not a frozen blob.
	tagsColumn := table.GetColumns()["tags"]
	if tagsColumn == nil {
		t.Fatal(`column "tags" was not compiled`)
	}
	if got := tagsColumn.GetType().DBType; got != "set<text>" {
		t.Errorf("tags DBType = %q, want set<text>", got)
	}

	// The compiled table must satisfy the shared Table interface, which is what
	// controllers and the name registry will depend on instead of ScyllaTable.
	var _ db.Table = table
}
