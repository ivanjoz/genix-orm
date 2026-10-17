package dynamo

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/genix-orm/db"
)

// ── Entities ─────────────────────────────────────────────────────────────────

type searchItem struct {
	CompanyID int32  `cb:"1"`
	ID        int32  `cb:"2"`
	Name      string `cb:"3"`
	Brand     string `cb:"4"`
	Price     int64  `cb:"5"`
	Status    int8   `cb:"6"`
	Version   int16  `json:"ver,omitempty" cb:"-"`
}

type searchItemTable struct {
	Model[searchItemTable, searchItem]
	CompanyID Col[*searchItemTable, int32]
	ID        Col[*searchItemTable, int32]
	Name      Col[*searchItemTable, string]
	Brand     Col[*searchItemTable, string]
	Price     Col[*searchItemTable, int64]
	Status    Col[*searchItemTable, int8]
}

func (t searchItemTable) GetSchema() Schema {
	return Schema{
		Entity:     "search_item",
		Partition:  Cols(t.CompanyID.Size(16)),
		Keys:       Cols(t.ID.Size(31)),
		TextSearch: &TextSearch{Columns: Cols(t.Name, t.Brand)},
	}
}

type searchSetting struct {
	ID      int32  `cb:"1"`
	Name    string `cb:"2"`
	Version int16  `json:"ver,omitempty" cb:"-"`
}

type searchSettingTable struct {
	Model[searchSettingTable, searchSetting]
	ID   Col[*searchSettingTable, int32]
	Name Col[*searchSettingTable, string]
}

func (t searchSettingTable) GetSchema() Schema {
	return Schema{Entity: "search_setting", Keys: Cols(t.ID.Size(31)), TextSearch: &TextSearch{Columns: Cols(t.Name)}}
}

type badSearchKeys struct {
	Created int64  `cb:"1"`
	ID      int32  `cb:"2"`
	Name    string `cb:"3"`
	Version int16  `json:"ver,omitempty" cb:"-"`
}

type badSearchKeysTable struct {
	Model[badSearchKeysTable, badSearchKeys]
	Created Col[*badSearchKeysTable, int64]
	ID      Col[*badSearchKeysTable, int32]
	Name    Col[*badSearchKeysTable, string]
}

func (t badSearchKeysTable) GetSchema() Schema {
	return Schema{Entity: "bad_search_keys", Keys: Cols(t.Created.Size(40), t.ID.Size(31)), TextSearch: &TextSearch{Columns: Cols(t.Name)}}
}

type badSearchColumn struct {
	ID      int32 `cb:"1"`
	Price   int64 `cb:"2"`
	Version int16 `json:"ver,omitempty" cb:"-"`
}

type badSearchColumnTable struct {
	Model[badSearchColumnTable, badSearchColumn]
	ID    Col[*badSearchColumnTable, int32]
	Price Col[*badSearchColumnTable, int64]
}

func (t badSearchColumnTable) GetSchema() Schema {
	return Schema{Entity: "bad_search_column", Keys: Cols(t.ID.Size(31)), TextSearch: &TextSearch{Columns: Cols(t.Price)}}
}

// ── An in-memory backend ─────────────────────────────────────────────────────

type memTextSearch struct {
	mutex   sync.Mutex
	parts   map[string]map[int32]string
	applies int
}

func installMemTextSearch(t *testing.T) *memTextSearch {
	backend := &memTextSearch{parts: map[string]map[int32]string{}}
	db.SetTextSearchBackend(backend)
	t.Cleanup(func() { db.SetTextSearchBackend(nil) })
	return backend
}

func (m *memTextSearch) part(table, partition string) map[int32]string {
	key := table + "/" + partition
	if m.parts[key] == nil {
		m.parts[key] = map[int32]string{}
	}
	return m.parts[key]
}

func (m *memTextSearch) Apply(_ context.Context, table, partition string, puts []db.TextDoc, dels []int32) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.applies++
	part := m.part(table, partition)
	for _, doc := range puts {
		part[doc.ID] = doc.Text
	}
	for _, id := range dels {
		delete(part, id)
	}
	return nil
}

func (m *memTextSearch) Search(_ context.Context, table, partition, query string, _, _ int) ([]db.TextHit, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var hits []db.TextHit
	part := m.part(table, partition)
	// Highest ID first, so the order differs from the table's.
	for _, id := range slices.Backward(slices.Sorted(maps.Keys(part))) {
		if strings.Contains(strings.ToLower(part[id]), strings.ToLower(query)) {
			hits = append(hits, db.TextHit{ID: id, Score: 1})
		}
	}
	return hits, nil
}

func (m *memTextSearch) Compact(context.Context, string, string) error { return nil }

func (m *memTextSearch) Rebuild(ctx context.Context, table, partition string, source db.TextSource) error {
	docs := map[int32]string{}
	if err := source(ctx, func(doc db.TextDoc) error { docs[doc.ID] = doc.Text; return nil }); err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.parts[table+"/"+partition] = docs
	return nil
}

func (m *memTextSearch) Drop(_ context.Context, table, partition string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.parts, table+"/"+partition)
	return nil
}

func (m *memTextSearch) state(table, partition string) map[int32]string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return maps.Clone(m.parts[table+"/"+partition])
}

func (m *memTextSearch) applyCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.applies
}

// ── Unit tests ───────────────────────────────────────────────────────────────

func TestTextSearchPartitionNames(t *testing.T) {
	items := NewRepo[searchItemTable, searchItem]()
	if got := items.meta.textPartitionOf(unsafe.Pointer(&searchItem{CompanyID: 12})); got != "p12" {
		t.Errorf("an integer partition: %q; want p12, like Scylla", got)
	}
	values, err := items.meta.partitionValuesOfPK(items.meta.pkValue(unsafe.Pointer(&searchItem{CompanyID: 12})))
	if err != nil || !slices.Equal(values, []uint64{12}) {
		t.Errorf("the partition read back from its pk: %v %v", values, err)
	}
	if got := textPartitionName([]uint64{3, 12}); got != "p3_p12" {
		t.Errorf("two partition columns: %q", got)
	}
	settings := NewRepo[searchSettingTable, searchSetting]()
	if got := settings.meta.textPartitionOf(unsafe.Pointer(&searchSetting{})); got != "all" {
		t.Errorf("no partition: %q", got)
	}
	if _, err := items.SearchTextIDs("polo", 10, 0); err == nil {
		t.Error("a search without the partition value was accepted")
	}
}

// syncTextSearch writes to the index only when the text or the active state changed.
func TestSyncTextSearchSendsOnlyChanges(t *testing.T) {
	backend := installMemTextSearch(t)
	meta := NewRepo[searchItemTable, searchItem]().meta
	item := func(name string, status int8) *searchItem {
		return &searchItem{CompanyID: 1, ID: 5, Name: name, Brand: "Nike", Status: status}
	}
	cases := []struct {
		name            string
		stored, written *searchItem
		want            map[int32]string
		isApplied       bool
	}{
		{"new active", nil, item("Polo", 1), map[int32]string{5: "Polo Nike"}, true},
		{"new inactive", nil, item("Polo", 0), map[int32]string{}, false},
		{"same text", item("Polo", 2), item("Polo", 1), map[int32]string{5: "Polo Nike"}, false},
		{"renamed", item("Polo", 1), item("Polo Pique", 1), map[int32]string{5: "Polo Pique Nike"}, true},
		{"deactivated", item("Polo", 1), item("Polo", 0), map[int32]string{}, true},
		{"reactivated", item("Polo", 0), item("Polo", 1), map[int32]string{5: "Polo Nike"}, true},
		{"renamed while inactive", item("Polo", 0), item("Gorra", 0), map[int32]string{}, false},
		{"deleted", item("Polo", 1), nil, map[int32]string{}, true},
	}
	for _, testCase := range cases {
		// The index holds what the stored record put in it.
		backend.parts = map[string]map[int32]string{"search_item/p1": {}}
		if testCase.stored != nil && testCase.stored.Status != 0 {
			backend.parts["search_item/p1"][5] = "Polo Nike"
		}
		applies := backend.applyCount()
		write := deltaLogWrite{}
		if testCase.stored != nil {
			write.storedPtr = unsafe.Pointer(testCase.stored)
		}
		if testCase.written != nil {
			write.writtenPtr = unsafe.Pointer(testCase.written)
		}
		if err := meta.syncTextSearch([]deltaLogWrite{write}); err != nil {
			t.Fatal(err)
		}
		if isApplied := backend.applyCount() != applies; isApplied != testCase.isApplied {
			t.Errorf("%s: applied %t, want %t", testCase.name, isApplied, testCase.isApplied)
		}
		if got := backend.state("search_item", "p1"); !maps.Equal(got, testCase.want) {
			t.Errorf("%s: index %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

func TestTextSearchSchemaValidation(t *testing.T) {
	for name, compile := range map[string]func(){
		"composite Keys":    func() { NewRepo[badSearchKeysTable, badSearchKeys]() },
		"non-string column": func() { NewRepo[badSearchColumnTable, badSearchColumn]() },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: compiled", name)
				}
			}()
			compile()
		}()
	}
}

func TestWritesFailWithoutABackend(t *testing.T) {
	db.SetTextSearchBackend(nil)
	err := NewRepo[searchItemTable, searchItem]().Put(&searchItem{CompanyID: 1, ID: 1, Name: "x", Status: 1})
	if err == nil || !strings.Contains(err.Error(), "no text search backend") {
		t.Errorf("Put without a backend: %v", err)
	}
}

// ── Against DynamoDB Local ───────────────────────────────────────────────────

// GENIX_ORM_DYNAMO_TEST is the endpoint of a DynamoDB Local, e.g.
// podman run -d -p 127.0.0.1:18000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory.
func dynamoForTextSearchTest(t *testing.T) {
	t.Helper()
	endpoint := os.Getenv("GENIX_ORM_DYNAMO_TEST")
	if endpoint == "" {
		t.Skip("GENIX_ORM_DYNAMO_TEST is not set")
	}
	t.Setenv("DYNAMO_ENDPOINT", endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "local")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local")
	client, err := Client()
	if err != nil {
		t.Fatal(err)
	}
	TableName = fmt.Sprintf("genix-orm-it-%d", time.Now().UnixNano())
	t.Cleanup(func() { TableName = "" })
	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(TableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeB},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTextSearchOnDynamo(t *testing.T) {
	dynamoForTextSearchTest(t)
	backend := installMemTextSearch(t)
	items := NewRepo[searchItemTable, searchItem]()

	polo := searchItem{CompanyID: 1, ID: 1, Name: "Polo", Brand: "Nike", Price: 10, Status: 1}
	if err := items.Put(&polo); err != nil {
		t.Fatal(err)
	}
	if got := backend.state("search_item", "p1"); !maps.Equal(got, map[int32]string{1: "Polo Nike"}) {
		t.Fatalf("after the first Put: %v", got)
	}

	// A new price doesn't touch the index.
	applies := backend.applyCount()
	polo.Price = 12
	if err := items.Put(&polo); err != nil {
		t.Fatal(err)
	}
	if backend.applyCount() != applies {
		t.Error("a Put that did not change the text wrote to the index")
	}

	if _, err := items.Modify(searchItem{CompanyID: 1, ID: 1}, func(item *searchItem, exists bool) error {
		item.Name = "Polo Pique"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := backend.state("search_item", "p1")[1]; got != "Polo Pique Nike" {
		t.Errorf("after the rename: %q", got)
	}

	storedPolo, err := items.Get(searchItem{CompanyID: 1, ID: 1})
	if err != nil || storedPolo == nil {
		t.Fatalf("reading the renamed polo: %v", err)
	}
	polo = *storedPolo
	polo.Status = 0
	if err := items.Put(&polo); err != nil {
		t.Fatal(err)
	}
	if _, isIndexed := backend.state("search_item", "p1")[1]; isIndexed {
		t.Error("still indexed after Status 0")
	}
	polo.Status = 1
	if err := items.Put(&polo); err != nil {
		t.Fatal(err)
	}

	batch := []searchItem{
		{CompanyID: 1, ID: 2, Name: "Gorra", Brand: "Puma", Status: 1},
		{CompanyID: 1, ID: 3, Name: "Casaca", Brand: "Adidas", Status: 0},
		{CompanyID: 1, ID: 4, Name: "Polo", Brand: "Lacoste", Status: 1},
		{CompanyID: 2, ID: 1, Name: "Polo", Brand: "Tommy", Status: 1},
	}
	if err := items.PutMany(batch); err != nil {
		t.Fatal(err)
	}
	want := map[int32]string{1: "Polo Pique Nike", 2: "Gorra Puma", 4: "Polo Lacoste"}
	if got := backend.state("search_item", "p1"); !maps.Equal(got, want) {
		t.Errorf("after PutMany, p1 = %v; want %v", got, want)
	}
	if got := backend.state("search_item", "p2"); !maps.Equal(got, map[int32]string{1: "Polo Tommy"}) {
		t.Errorf("after PutMany, p2 = %v", got)
	}

	if err := items.Delete(&searchItem{CompanyID: 1, ID: 2}); err != nil {
		t.Fatal(err)
	}
	if _, isIndexed := backend.state("search_item", "p1")[2]; isIndexed {
		t.Error("2 is still indexed after Delete")
	}

	var found []searchItem
	hits, err := items.SearchText(&found, "polo", 10, 0, int32(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || len(found) != 2 || found[0].ID != 4 || found[1].ID != 1 || found[1].Brand != "Nike" {
		t.Errorf("SearchText polo: hits %+v, records %+v", hits, found)
	}

	// The index lost everything: the reindex restores it from the table.
	backend.parts = map[string]map[int32]string{}
	if err := items.ReindexTextSearchAll(); err != nil {
		t.Fatal(err)
	}
	if got := backend.state("search_item", "p1"); !maps.Equal(got, map[int32]string{1: "Polo Pique Nike", 4: "Polo Lacoste"}) {
		t.Errorf("after ReindexTextSearchAll, p1 = %v", got)
	}
	if got := backend.state("search_item", "p2"); !maps.Equal(got, map[int32]string{1: "Polo Tommy"}) {
		t.Errorf("after ReindexTextSearchAll, p2 = %v", got)
	}

	if _, err := items.DeleteRecordsAll(); err != nil {
		t.Fatal(err)
	}
	if len(backend.state("search_item", "p1")) != 0 || len(backend.state("search_item", "p2")) != 0 {
		t.Error("DeleteRecordsAll left the indexes of the partitions it emptied")
	}
}
