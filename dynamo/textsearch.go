package dynamo

import (
	"context"
	"fmt"
	"maps"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/genix-orm/db"
)

// ─────────────────────────────────────────────────────────────────────────────
// Text search
//
// An entity that declares TextSearch keeps a text index in the backend installed
// with db.SetTextSearchBackend (genix-s3, or the GenixSearch daemon), the same one
// the Scylla ORM uses:
//
//	TextSearch: &dynamo.TextSearch{Columns: dynamo.Cols(t.Name, t.Brand)}
//
//	hits, err := Products.SearchText(&out, "polo rojo", 20, 0, companyID)
//
// One index per (entity, partition): the collection is the entity, the partition
// is named after the record's Partition values ("all" without any, "p12" for one,
// as Scylla names them; "p3_p12" for two). The document ID is the record's Keys,
// which must be a single integer column that fits int32.
//
// Only active records are indexed: a record whose Status is 0 leaves the index,
// and writing it back with another Status indexes it again. Every write knows the
// record as stored before it (an entity with TextSearch reads its stored version,
// readsStoredVersion), so the index is written only when the text or the active
// state changed. ReprocessRecords and CopyRecordsFrom write silently: reindex after
// them.
// ─────────────────────────────────────────────────────────────────────────────

// TextSearch declares an entity's text index.
type TextSearch struct {
	// Columns are the string columns whose values, joined by a space, are the
	// indexed text.
	Columns []Coln
	// Manual turns off the sync on write: the index changes only through the
	// ReindexTextSearch family.
	Manual bool
}

// textSearchMeta is the compiled TextSearch of an entity.
type textSearchMeta struct {
	columns []*colAccessor
	id      keyCol // the single integer Keys column
	manual  bool
}

// textSearchName is the rule both backends accept for collection and partition
// names.
var textSearchName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// resolveTextSearch validates the TextSearch declaration against the compiled keys.
func resolveTextSearch(schema Schema, recordType reflect.Type, meta *tableMeta) *textSearchMeta {
	declared := schema.TextSearch
	name := recordType.Name()
	if !textSearchName.MatchString(meta.entity) {
		panic(fmt.Sprintf("db: %s uses TextSearch, so its Entity %q must match %s", name, meta.entity, textSearchName))
	}
	if len(declared.Columns) == 0 {
		panic(fmt.Sprintf("db: %s TextSearch declares no Columns", name))
	}
	if len(meta.keys) != 1 || !meta.keys[0].kind.isInteger() {
		panic(fmt.Sprintf("db: %s uses TextSearch, so its Keys must be a single integer column (the document ID)", name))
	}
	textSearch := &textSearchMeta{id: meta.keys[0], manual: declared.Manual}
	for _, column := range declared.Columns {
		fieldName := column.col().fieldName
		accessor, isField := meta.accessors[fieldName]
		if !isField || accessor.kind != kindString {
			panic(fmt.Sprintf("db: %s TextSearch column %q must be a string field", name, fieldName))
		}
		textSearch.columns = append(textSearch.columns, accessor)
	}
	return textSearch
}

// syncsTextSearch reports whether writes update the entity's text index.
func (m *tableMeta) syncsTextSearch() bool { return m.textSearch != nil && !m.textSearch.manual }

// checkTextSearchBackend fails a write before it lands when it should update the
// index but no backend is installed.
func (m *tableMeta) checkTextSearchBackend() error {
	if !m.syncsTextSearch() {
		return nil
	}
	_, err := db.GetTextSearchBackend()
	return err
}

// textDoc is a record's index state.
type textDoc struct {
	id     int32
	text   string
	active bool
}

func (m *tableMeta) textDocOf(ptr unsafe.Pointer) (textDoc, error) {
	id := m.textSearch.id.acc.getI64(ptr)
	if id < 0 || id > math.MaxInt32 {
		return textDoc{}, fmt.Errorf("db: text search ID %d does not fit int32", id)
	}
	texts := make([]string, len(m.textSearch.columns))
	for i, accessor := range m.textSearch.columns {
		texts[i] = accessor.getStr(ptr)
	}
	isActive := m.status == nil || m.status.getI64(ptr) != 0
	return textDoc{id: int32(id), text: db.JoinSearchText(texts...), active: isActive}, nil
}

// textPartitionName names the index partition of a record's Partition values.
func textPartitionName(partitionValues []uint64) string {
	if len(partitionValues) == 0 {
		return "all"
	}
	names := make([]string, len(partitionValues))
	for i, value := range partitionValues {
		names[i] = db.TextSearchPartition(int64(value))
	}
	return strings.Join(names, "_")
}

func (m *tableMeta) textPartitionOf(ptr unsafe.Pointer) string {
	values := make([]uint64, len(m.partition))
	for i, partitionColumn := range m.partition {
		values[i] = partitionColumn.acc.getU64(ptr)
	}
	return textPartitionName(values)
}

// partitionValuesOfPK reads the Partition values back from a base pk: TableID, then
// each column zero-padded to its decimal width (keys.go).
func (m *tableMeta) partitionValuesOfPK(pk string) ([]uint64, error) {
	digits, isEntityPK := strings.CutPrefix(pk, m.tableID)
	if !isEntityPK || len(digits) != m.partitionDigits {
		return nil, fmt.Errorf("db: pk %q is not a base pk of %s", pk, m.entity)
	}
	values := make([]uint64, len(m.partition))
	for i, partitionColumn := range m.partition {
		width := decimalWidth(partitionColumn.bits)
		value, err := strconv.ParseUint(digits[:width], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("db: pk %q is not a base pk of %s", pk, m.entity)
		}
		values[i], digits = value, digits[width:]
	}
	return values, nil
}

// syncTextSearch updates the index after writes landed: each write is the record as
// stored before it (nil when new) and as written (nil for a delete), as for the
// delta logs. One Apply per partition; the last write of an ID wins.
func (m *tableMeta) syncTextSearch(writes []deltaLogWrite) error {
	if !m.syncsTextSearch() || len(writes) == 0 {
		return nil
	}
	type partitionChanges struct {
		puts map[int32]string
		dels map[int32]bool
	}
	var partitionNames []string
	changesByPartition := map[string]*partitionChanges{}
	for _, write := range writes {
		var before, after textDoc
		var err error
		if write.storedPtr != nil {
			if before, err = m.textDocOf(write.storedPtr); err != nil {
				return err
			}
		}
		if write.writtenPtr != nil {
			if after, err = m.textDocOf(write.writtenPtr); err != nil {
				return err
			}
		}
		wasIndexed := write.storedPtr != nil && before.active
		isIndexed := write.writtenPtr != nil && after.active
		if wasIndexed == isIndexed && (!isIndexed || before.text == after.text) {
			continue
		}
		keyPtr := write.writtenPtr
		if keyPtr == nil {
			keyPtr = write.storedPtr
		}
		partitionName := m.textPartitionOf(keyPtr)
		changes := changesByPartition[partitionName]
		if changes == nil {
			changes = &partitionChanges{puts: map[int32]string{}, dels: map[int32]bool{}}
			changesByPartition[partitionName] = changes
			partitionNames = append(partitionNames, partitionName)
		}
		if isIndexed {
			changes.puts[after.id] = after.text
			delete(changes.dels, after.id)
		} else {
			changes.dels[before.id] = true
			delete(changes.puts, before.id)
		}
	}
	if len(partitionNames) == 0 {
		return nil
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	for _, partitionName := range partitionNames {
		changes := changesByPartition[partitionName]
		puts := make([]db.TextDoc, 0, len(changes.puts))
		for _, id := range slices.Sorted(maps.Keys(changes.puts)) {
			puts = append(puts, db.TextDoc{ID: id, Text: changes.puts[id]})
		}
		if err := backend.Apply(context.Background(), m.entity, partitionName, puts, slices.Sorted(maps.Keys(changes.dels))); err != nil {
			return fmt.Errorf("db: text index of %s/%s: %w", m.entity, partitionName, err)
		}
	}
	return nil
}

// ── Search ──────────────────────────────────────────────────────────────────

// textSearchPartitionOf names the partition of one value per Partition column, as in TopN.
func (r *Repo[T, E]) textSearchPartitionOf(partitionValues []any) (string, error) {
	if r.meta.textSearch == nil {
		return "", fmt.Errorf("db: %s does not declare TextSearch", r.meta.recordType.Name())
	}
	if len(partitionValues) != len(r.meta.partition) {
		return "", fmt.Errorf("db: %s has %d partition column(s), got %d value(s)",
			r.meta.recordType.Name(), len(r.meta.partition), len(partitionValues))
	}
	values := make([]uint64, len(partitionValues))
	for i, value := range partitionValues {
		values[i] = uint64(valueToInt64(value, r.meta.partition[i].fieldName))
	}
	return textPartitionName(values), nil
}

// SearchTextIDs queries the text index of one partition (one value per Partition
// column, as in TopN) and returns the matching active records' IDs, best first,
// without reading DynamoDB. limit <= 0 lets the backend apply its default.
func (r *Repo[T, E]) SearchTextIDs(query string, limit, offset int, partitionValues ...any) ([]db.TextHit, error) {
	partitionName, err := r.textSearchPartitionOf(partitionValues)
	if err != nil {
		return nil, err
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return nil, err
	}
	return backend.Search(context.Background(), r.meta.entity, partitionName, query, limit, offset)
}

// SearchText runs SearchTextIDs, then reads the records (GetMany) and appends them
// to *dst in the order of the hits. A hit the table no longer holds is absent from
// *dst. The hits are returned for their scores.
func (r *Repo[T, E]) SearchText(dst *[]E, query string, limit, offset int, partitionValues ...any) ([]db.TextHit, error) {
	hits, err := r.SearchTextIDs(query, limit, offset, partitionValues...)
	if err != nil || len(hits) == 0 {
		return hits, err
	}
	keys := make([]E, len(hits))
	for i, hit := range hits {
		keyPtr := unsafe.Pointer(&keys[i])
		for partitionIndex, partitionColumn := range r.meta.partition {
			partitionColumn.acc.setI64(keyPtr, valueToInt64(partitionValues[partitionIndex], partitionColumn.fieldName))
		}
		r.meta.textSearch.id.acc.setI64(keyPtr, int64(hit.ID))
	}
	records, err := r.GetMany(keys)
	if err != nil {
		return hits, err
	}
	recordByID := make(map[int64]E, len(records))
	for _, record := range records {
		recordByID[r.meta.textSearch.id.acc.getI64(unsafe.Pointer(&record))] = record
	}
	for _, hit := range hits {
		if record, isStored := recordByID[int64(hit.ID)]; isStored {
			*dst = append(*dst, record)
		}
	}
	return hits, nil
}

// ── Maintenance ─────────────────────────────────────────────────────────────

// ReindexTextSearch rebuilds one partition's text index from its active records.
// Writes that land while it runs are kept.
func (r *Repo[T, E]) ReindexTextSearch(partitionValues ...any) error {
	if r.meta.textSearch == nil {
		return nil
	}
	partitionName, err := r.textSearchPartitionOf(partitionValues)
	if err != nil {
		return err
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	return backend.Rebuild(context.Background(), r.meta.entity, partitionName, r.textSource(partitionValues))
}

// ReindexTextSearchAll rebuilds the text index of every partition that has records:
// it lists the entity's pks first, then reads each partition inside its rebuild.
func (r *Repo[T, E]) ReindexTextSearchAll() error {
	if r.meta.textSearch == nil {
		return nil
	}
	partitionsValues, err := r.storedPartitionsValues()
	if err != nil {
		return err
	}
	for _, values := range partitionsValues {
		partitionValues := make([]any, len(values))
		for i, value := range values {
			partitionValues[i] = value
		}
		if err := r.ReindexTextSearch(partitionValues...); err != nil {
			return fmt.Errorf("db: reindex of %s/%s: %w", r.meta.entity, textPartitionName(values), err)
		}
	}
	return nil
}

// CompactTextSearch folds the partition's pending index writes, for backends with
// a write log.
func (r *Repo[T, E]) CompactTextSearch(partitionValues ...any) error {
	if r.meta.textSearch == nil {
		return nil
	}
	partitionName, err := r.textSearchPartitionOf(partitionValues)
	if err != nil {
		return err
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	return backend.Compact(context.Background(), r.meta.entity, partitionName)
}

// DropTextSearch empties the partition's text index.
func (r *Repo[T, E]) DropTextSearch(partitionValues ...any) error {
	if r.meta.textSearch == nil {
		return nil
	}
	partitionName, err := r.textSearchPartitionOf(partitionValues)
	if err != nil {
		return err
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	return backend.Drop(context.Background(), r.meta.entity, partitionName)
}

// textSource lists the active records of one partition, as TopN reads it.
func (r *Repo[T, E]) textSource(partitionValues []any) db.TextSource {
	return func(ctx context.Context, yield func(db.TextDoc) error) error {
		records, err := r.TopN(0, partitionValues...)
		if err != nil {
			return err
		}
		for i := range records {
			doc, err := r.meta.textDocOf(unsafe.Pointer(&records[i]))
			if err != nil {
				return err
			}
			if !doc.active {
				continue
			}
			if err := yield(db.TextDoc{ID: doc.id, Text: doc.text}); err != nil {
				return err
			}
		}
		return nil
	}
}

// storedPartitionsValues lists the Partition values of every base pk the entity holds.
func (r *Repo[T, E]) storedPartitionsValues() ([][]uint64, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	isListed := map[string]bool{}
	var partitionsValues [][]uint64
	err = scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange()}, []string{"pk"}, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			pk := item["pk"].(*types.AttributeValueMemberN).Value
			if isListed[pk] {
				continue
			}
			isListed[pk] = true
			values, err := r.meta.partitionValuesOfPK(pk)
			if err != nil {
				return err
			}
			partitionsValues = append(partitionsValues, values)
		}
		return nil
	})
	return partitionsValues, err
}
