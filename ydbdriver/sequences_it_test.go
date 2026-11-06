package ydbdriver

import (
	"sync"
	"testing"
)

func TestSequencesReserveRangesWithoutDuplicates(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	connParams.Keyspace = directory // same package: avoids SetConnection closing the driver this test holds
	spikeMustExec(t, connection, createTableStatement(sequencesTableSpec(directory)))

	if first, err := GetAutoincrementID("orders", 5); err != nil || first != 1 {
		t.Fatalf("first range = %d, %v (want 1)", first, err)
	}
	if first, err := GetAutoincrementID("orders", 3); err != nil || first != 6 {
		t.Fatalf("second range = %d, %v (want 6)", first, err)
	}
	if previous, err := SetCounter("orders", 1000); err != nil || previous != 8 {
		t.Fatalf("SetCounter previous = %d, %v (want 8)", previous, err)
	}
	if first, _ := GetAutoincrementID("orders", 1); first != 1001 {
		t.Fatalf("after SetCounter first = %d (want 1001)", first)
	}
	if _, err := GetAutoincrementID("invoices", 1); err != nil {
		t.Fatal(err)
	}
	counterNames, err := ListCounters()
	if err != nil || len(counterNames) != 2 || counterNames[0] != "invoices" || counterNames[1] != "orders" {
		t.Fatalf("ListCounters = %v, %v", counterNames, err)
	}
	if _, err := GetAutoincrementID("orders", 0); err == nil {
		t.Fatal("a zero-size reservation must fail")
	}

	// 32 writers reserving 10 IDs each, 10 times: every range must be disjoint.
	var mutex sync.Mutex
	firstIDs := map[int64]bool{}
	var group sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 10; iteration++ {
				first, err := GetAutoincrementID("concurrent", 10)
				if err != nil {
					t.Error(err)
					return
				}
				mutex.Lock()
				if firstIDs[first] {
					t.Errorf("range starting at %d was handed out twice", first)
				}
				firstIDs[first] = true
				mutex.Unlock()
			}
		}()
	}
	group.Wait()
	if len(firstIDs) != 320 {
		t.Fatalf("got %d distinct ranges, want 320", len(firstIDs))
	}
}
