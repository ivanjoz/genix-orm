package db

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// This file is the ORM's name-addressed surface: what a script, an admin tool or an agent needs to
// read and write a table it can only identify by a string. The typed API stays the default for
// application code — this one exists because Go generics cannot resolve a table from a runtime
// name, and without it tooling would fall back to hand-written CQL and lose every managed column
// the ORM maintains (autoincrement keys, updated_version, virtual columns, views, text index).

// FilterSpec is one predicate expressed with strings and JSON-native values. Column accepts either
// the storage column name ("company_id") or the Go field name ("CompanyID"), because a caller that
// reads records as JSON only ever sees the second spelling.
type FilterSpec struct {
	Column   string `json:"col"`
	Operator string `json:"op"`
	Value    any    `json:"value,omitempty"`
	Values   []any  `json:"values,omitempty"`
	From     any    `json:"from,omitempty"`
	To       any    `json:"to,omitempty"`
}

// QuerySpec is a whole read described without naming a single Go type.
type QuerySpec struct {
	Filters     []FilterSpec `json:"filters,omitempty"`
	Columns     []string     `json:"columns,omitempty"`
	Limit       int32        `json:"limit,omitempty"`
	AllowFilter bool         `json:"allow_filter,omitempty"`
	OrderDesc   bool         `json:"order_desc,omitempty"`
}

// ColumnDescription names one column in all three vocabularies that a caller may hold it in: the
// storage column, the Go field, and the JSON key of a serialized record.
type ColumnDescription struct {
	Name        string `json:"name"`
	FieldName   string `json:"field"`
	JSONKey     string `json:"json,omitempty"`
	GoType      string `json:"type"`
	IsPartition bool   `json:"is_partition,omitempty"`
	IsKey       bool   `json:"is_key,omitempty"`
	IsVirtual   bool   `json:"is_virtual,omitempty"`
	// IsManaged marks a column the ORM writes on the caller's behalf, so a write payload must
	// leave it out: created, updated and updated_version.
	IsManaged bool `json:"is_managed,omitempty"`
}

// TableDescription is one table's schema in serializable form — what to read before writing a
// filter. QueryShapes is the decisive field: it lists the predicate combinations the compiled
// table can serve through a key, an index or a view, so anything else needs ALLOW FILTERING.
type TableDescription struct {
	Name               string              `json:"name"`
	Namespace          string              `json:"namespace,omitempty"`
	ID                 int16               `json:"id"`
	Partition          string              `json:"partition,omitempty"`
	Keys               []string            `json:"keys"`
	Columns            []ColumnDescription `json:"columns"`
	QueryShapes        []string            `json:"query_shapes"`
	SaveUpdatedVersion bool                `json:"save_updated_version,omitempty"`
}

// namedCol re-wraps a compiled column as the Coln handle the write API takes. Insert and Update
// only read GetName() off it and look the compiled column back up themselves, so this carries no
// state beyond the descriptor.
type namedCol struct{ info ColumnInfo }

func (c namedCol) GetInfo() ColumnInfo { return c.info }
func (c namedCol) GetName() string     { return c.info.Name }

// ResolveColumn finds a column by storage name and, failing that, by Go field name. The error
// lists the available columns, because a caller that guessed wrong has no other way to find out.
func ResolveColumn(table Table, nameOrField string) (IColInfo, error) {
	columns := table.GetColumns()
	if column, found := columns[nameOrField]; found && column != nil && !column.IsNil() {
		return column, nil
	}

	lowerName := strings.ToLower(nameOrField)
	availableNames := make([]string, 0, len(columns))
	for columnName, column := range columns {
		if column == nil || column.IsNil() {
			continue
		}
		availableNames = append(availableNames, columnName)
		info := column.GetInfo()
		if strings.EqualFold(info.FieldName, nameOrField) || strings.ToLower(columnName) == lowerName {
			return column, nil
		}
	}

	return nil, fmt.Errorf("table %q has no column %q. Available: %v",
		table.GetName(), nameOrField, strings.Join(availableNames, ", "))
}

// ColnByName resolves a column name into the handle Insert/Update expect.
func ColnByName(table Table, nameOrField string) (Coln, error) {
	column, err := ResolveColumn(table, nameOrField)
	if err != nil {
		return nil, err
	}
	columnInfo, isFullDescriptor := column.(*ColumnInfo)
	if !isFullDescriptor {
		return nil, fmt.Errorf("column %q of table %q carries no full descriptor",
			nameOrField, table.GetName())
	}
	return namedCol{info: *columnInfo}, nil
}

// ColnsByName resolves a whole list, failing on the first name that does not exist.
func ColnsByName(table Table, namesOrFields []string) ([]Coln, error) {
	resolved := make([]Coln, 0, len(namesOrFields))
	for _, name := range namesOrFields {
		column, err := ColnByName(table, name)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, column)
	}
	return resolved, nil
}

// CoerceToColumn converts a value straight out of JSON into the column's real Go type. It is not
// optional: encoding/json decodes every number as float64, and the accessor engine's ToInt64
// returns 0 for a float64 — so an uncoerced {"col":"id","value":123} would silently query id = 0.
func CoerceToColumn(column IColInfo, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	columnType := column.GetType()
	// Slice and pointer type IDs are their scalar ID plus a multiple of ten (int32 is 3, []int32
	// is 13, *int32 is 23), so the units digit names the type a predicate compares against: a
	// CONTAINS takes one element, and a pointer column is compared as its pointee.
	return coerceToTypeID(columnType.Type%10, column.GetName(), value)
}

func coerceToTypeID(typeID int8, columnName string, value any) (any, error) {
	switch typeID {
	case 1:
		text, isText := value.(string)
		if !isText {
			return nil, valueTypeError(columnName, "string", value)
		}
		return text, nil
	case 2, 3, 4, 5:
		wholeNumber, err := toInt64Exact(value)
		if err != nil {
			return nil, valueTypeError(columnName, "integer", value)
		}
		switch typeID {
		case 2:
			return wholeNumber, nil
		case 3:
			return int32(wholeNumber), nil
		case 4:
			return int16(wholeNumber), nil
		}
		return int8(wholeNumber), nil
	case 6, 7:
		realNumber, err := toFloat64Exact(value)
		if err != nil {
			return nil, valueTypeError(columnName, "number", value)
		}
		if typeID == 6 {
			return float32(realNumber), nil
		}
		return realNumber, nil
	case 8:
		boolean, isBoolean := value.(bool)
		if !isBoolean {
			return nil, valueTypeError(columnName, "bool", value)
		}
		return boolean, nil
	}
	return nil, fmt.Errorf("column %q holds a complex/blob type and cannot be filtered on", columnName)
}

func valueTypeError(columnName, expectedType string, value any) error {
	return fmt.Errorf("column %q expects a %v, got %v (%T)", columnName, expectedType, value, value)
}

// toInt64Exact refuses a fractional or oversized number rather than truncating it, because a
// silently rounded key would query the wrong row.
func toInt64Exact(value any) (int64, error) {
	switch typedValue := value.(type) {
	case json.Number:
		return typedValue.Int64()
	case string:
		return strconv.ParseInt(typedValue, 10, 64)
	case float64:
		if typedValue != math.Trunc(typedValue) || math.Abs(typedValue) > math.MaxInt64 {
			return 0, fmt.Errorf("%v is not a whole number", typedValue)
		}
		return int64(typedValue), nil
	case bool:
		return 0, fmt.Errorf("%v is not a number", typedValue)
	}
	if widened := ToInt64(value); widened != 0 {
		return widened, nil
	}
	// ToInt64 cannot tell a real zero from an unsupported type, so confirm the kind before
	// accepting the zero it returned.
	switch reflect.ValueOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return 0, nil
	}
	return 0, fmt.Errorf("%v is not a number", value)
}

func toFloat64Exact(value any) (float64, error) {
	switch typedValue := value.(type) {
	case json.Number:
		return typedValue.Float64()
	case string:
		return strconv.ParseFloat(typedValue, 64)
	case float64:
		return typedValue, nil
	case float32:
		return float64(typedValue), nil
	case bool:
		return 0, fmt.Errorf("%v is not a number", typedValue)
	}
	if widened := ToInt64(value); widened != 0 {
		return float64(widened), nil
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return 0, nil
	}
	return 0, fmt.Errorf("%v is not a number", value)
}

// ApplyFilters binds a string-described filter list onto a query built at runtime. It lives here
// rather than in a driver because nothing about it is storage-specific: names resolve against the
// compiled table and values coerce against the column type, and what reaches the driver is the
// same ColumnStatement the typed fluent API produces.
func ApplyFilters[T any](query TableGenericQuery[T], table Table, filters []FilterSpec) error {
	for _, filter := range filters {
		column, err := ResolveColumn(table, filter.Column)
		if err != nil {
			return err
		}
		columnName := column.GetName()
		operator := strings.ToUpper(strings.TrimSpace(filter.Operator))

		switch operator {
		case "IN":
			if len(filter.Values) == 0 {
				return fmt.Errorf(`filter on %q with operator IN needs a non-empty "values"`, columnName)
			}
			coercedValues := make([]any, 0, len(filter.Values))
			for _, rawValue := range filter.Values {
				coercedValue, err := CoerceToColumn(column, rawValue)
				if err != nil {
					return err
				}
				coercedValues = append(coercedValues, coercedValue)
			}
			query.SetWhereIn(columnName, coercedValues)

		case "BETWEEN":
			from, err := CoerceToColumn(column, filter.From)
			if err != nil {
				return err
			}
			to, err := CoerceToColumn(column, filter.To)
			if err != nil {
				return err
			}
			if from == nil || to == nil {
				return fmt.Errorf(`filter on %q with operator BETWEEN needs "from" and "to"`, columnName)
			}
			query.SetBetween(columnName, from, to)

		case "=", "!=", ">", ">=", "<", "<=", "CONTAINS":
			value, err := CoerceToColumn(column, filter.Value)
			if err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf(`filter on %q with operator %v needs a "value"`, columnName, operator)
			}
			query.SetWhere(columnName, operator, value)

		default:
			return fmt.Errorf("unknown operator %q on column %q. Valid: = != > >= < <= IN BETWEEN CONTAINS",
				filter.Operator, columnName)
		}
	}
	return nil
}

// DescribeColumns renders a compiled table's columns, in declaration order, into the serializable
// form. recordType is the record struct, consulted only for the json tag of each field; pass a nil
// type to leave JSONKey empty.
func DescribeColumns(columns []IColInfo, keys []IColInfo, partition IColInfo,
	managed []IColInfo, recordType reflect.Type,
) []ColumnDescription {

	isNamed := func(candidates []IColInfo, name string) bool {
		for _, candidate := range candidates {
			if candidate != nil && !candidate.IsNil() && candidate.GetName() == name {
				return true
			}
		}
		return false
	}

	described := make([]ColumnDescription, 0, len(columns))
	for _, column := range columns {
		if column == nil || column.IsNil() {
			continue
		}
		info := column.GetInfo()
		described = append(described, ColumnDescription{
			Name:        column.GetName(),
			FieldName:   info.FieldName,
			JSONKey:     jsonKeyOfField(recordType, info.FieldName),
			GoType:      column.GetType().FieldType,
			IsPartition: partition != nil && !partition.IsNil() && partition.GetName() == column.GetName(),
			IsKey:       isNamed(keys, column.GetName()),
			IsVirtual:   info.IsVirtual,
			IsManaged:   isNamed(managed, column.GetName()),
		})
	}
	// Compiled column order comes out of the driver's own bookkeeping and is not the declaration
	// order, so sort by name: a stable listing is what makes two describes comparable.
	sort.Slice(described, func(a, b int) bool { return described[a].Name < described[b].Name })
	return described
}

// jsonKeyOfField resolves the key a field serializes under, which is what a write payload must
// use. An empty result means the field is not serialized at all (`json:"-"`).
func jsonKeyOfField(recordType reflect.Type, fieldName string) string {
	if recordType == nil || fieldName == "" {
		return ""
	}
	if recordType.Kind() == reflect.Pointer {
		recordType = recordType.Elem()
	}
	field, found := recordType.FieldByName(fieldName)
	if !found {
		return ""
	}
	tag := field.Tag.Get("json")
	if tag == "-" {
		return ""
	}
	if name, _, _ := strings.Cut(tag, ","); name != "" {
		return name
	}
	return field.Name
}
