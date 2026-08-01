package db

import (
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/viant/xunsafe"
)

// Record metadata is immutable: it depends only on the record struct's type, not
// on any query. Building it once per type is what keeps InitStructTable cheap
// enough to call on every query.
type structFieldMetadataCacheEntry struct {
	recordType          reflect.Type
	fieldMetadataByName map[string]ColumnInfo
}

var structFieldMetadataCache sync.Map

func getOrBuildStructFieldMetadata(recordType reflect.Type) *structFieldMetadataCacheEntry {
	if cachedEntry, cacheHit := structFieldMetadataCache.Load(recordType); cacheHit {
		return cachedEntry.(*structFieldMetadataCacheEntry)
	}

	metadataByFieldName := map[string]ColumnInfo{}
	for fieldIndex := 0; fieldIndex < recordType.NumField(); fieldIndex++ {
		recordField := recordType.Field(fieldIndex)
		if recordField.Name == "TableStruct" {
			continue
		}

		unsafeField := xunsafe.FieldByName(recordType, recordField.Name)
		columnType := GetColTypeByName(recordField.Type.String())
		if columnType.Type == 0 {
			// Anything with no native mapping is stored as an opaque blob.
			columnType = GetColTypeByID(TypeBlob)
		}

		tag := ParseDBTag(recordField.Tag.Get("db"))
		if codec != nil {
			columnType = codec.ApplyCollectionOptions(recordType.Name(), recordField.Name, columnType, tag)
		}

		metadataByFieldName[recordField.Name] = ColumnInfo{
			ColInfo: ColInfo{
				Name:      tag.ColumnName,
				FieldIdx:  fieldIndex,
				FieldName: recordField.Name,
				RefType:   recordField.Type,
				Field:     unsafeField,
			},
			ColType:                 columnType,
			HasCollectionTagOptions: tag.HasCollectionOptions(),
		}
	}

	metadataEntry := &structFieldMetadataCacheEntry{
		recordType:          recordType,
		fieldMetadataByName: metadataByFieldName,
	}

	actualEntry, _ := structFieldMetadataCache.LoadOrStore(recordType, metadataEntry)
	return actualEntry.(*structFieldMetadataCacheEntry)
}

// ResetMetadataCacheForTesting clears the record metadata cache so benchmarks and
// tests measure a cold build.
func ResetMetadataCacheForTesting() {
	structFieldMetadataCache = sync.Map{}
}

// DBTag is the parsed `db:"..."` tag of a record field: the column name plus the
// collection options. What "list", "set" and "frozen" mean physically is the
// driver's business (see Codec.ApplyCollectionOptions); parsing them is not.
type DBTag struct {
	ColumnName string
	IsFrozen   bool
	IsList     bool
	IsSet      bool
}

func ParseDBTag(dbTagRaw string) DBTag {
	tag := DBTag{}
	if dbTagRaw == "" {
		return tag
	}

	tagParts := strings.Split(dbTagRaw, ",")
	tag.ColumnName = strings.TrimSpace(tagParts[0])

	// Normalize option flags so callers can check options in a case-insensitive way.
	for optionIndex := 1; optionIndex < len(tagParts); optionIndex++ {
		switch strings.ToLower(strings.TrimSpace(tagParts[optionIndex])) {
		case "frozen":
			tag.IsFrozen = true
		case "list":
			tag.IsList = true
		case "set":
			tag.IsSet = true
		}
	}

	return tag
}

func (tag DBTag) HasCollectionOptions() bool {
	return tag.IsList || tag.IsSet || tag.IsFrozen
}

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
	// matchUnderline collapses the doubled separators the two passes above can
	// produce around single-letter words ("_a_" -> "_a").
	matchUnderline = regexp.MustCompile("_([a-z0-9])_")
)

// ToSnakeCase derives the default column name from a Go field name.
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	res := strings.ToLower(snake)

	for matchUnderline.MatchString(res) {
		res = matchUnderline.ReplaceAllString(res, "_$1")
	}

	return res
}
