package scylla

import (
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

func withMemoryFileStore(t *testing.T) *utils.MemoryFileStore {
	t.Helper()
	store := utils.NewMemoryFileStore()
	cache.SetFileStore(store)
	t.Cleanup(func() { cache.SetFileStore(nil) })
	return store
}

func TestTableIDCollisionIsRejected(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected two tables sharing one TableSchema.ID to panic")
		}
		if !strings.Contains(strings.ToLower(toString(recovered)), "declared by two tables") {
			t.Fatalf("expected the panic to name the collision, got: %v", recovered)
		}
	}()

	MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	MakeScyllaTable[tableIDCollisionRecord, tableIDCollisionSchema]()
}

func toString(value any) string {
	if err, isError := value.(error); isError {
		return err.Error()
	}
	if text, isText := value.(string); isText {
		return text
	}
	return ""
}

// Declares the same ID as fixedViewViewSchema, which must be refused at compile time.
type tableIDCollisionRecord struct {
	TableStruct[tableIDCollisionSchema, tableIDCollisionRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
}

type tableIDCollisionSchema struct {
	TableStruct[tableIDCollisionSchema, tableIDCollisionRecord]
	CompanyID Col[*tableIDCollisionSchema, int32]
	ID        Col[*tableIDCollisionSchema, int32]
}

func (e tableIDCollisionSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10008,
		Name:      "table_id_collision_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
	}
}
