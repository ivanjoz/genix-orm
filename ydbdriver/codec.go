package ydbdriver

// ydbTypeNames maps each ORM type ID (db/coltype.go) to its YDB column type (docs/YDB_PLAN.md §7.1).
// Pointers are the same column type, nullable. YDB row tables have no container types, so every
// slice column is a String holding the colbin encoding (Y10).
var ydbTypeNames = map[int8]string{
	1: "Utf8", 2: "Int64", 3: "Int32", 4: "Int16", 5: "Int8", 6: "Float", 7: "Double", 8: "Bool",
	9:  "String",
	11: "String", 12: "String", 13: "String", 14: "String", 15: "String", 16: "String", 17: "String",
	21: "Utf8", 22: "Int64", 23: "Int32", 24: "Int16", 25: "Int8", 26: "Float", 27: "Double",
	31: "String", 32: "String", 33: "String", 34: "String", 35: "String", 36: "String", 37: "String",
}
