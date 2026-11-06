package ydbdriver

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Every ORM type ID the framework declares must resolve to a YDB type; a missing one would
// silently produce an empty column type in the DDL.
func TestEveryOrmTypeIDHasYdbType(t *testing.T) {
	for _, typeID := range []int8{1, 2, 3, 4, 5, 6, 7, 8, db.TypeBlob, 11, 12, 13, 14, 15, 16, 17,
		21, 22, 23, 24, 25, 26, 27, 31, 32, 33, 34, 35, 36, 37} {
		if ydbTypeNames[typeID] == "" {
			t.Errorf("ORM type ID %d has no YDB column type", typeID)
		}
	}
}
