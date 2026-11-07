package obdriver

import (
	"sync"
	"testing"
)

// Concurrent reservations on one counter never overlap and leave no gap; SetCounter answers the
// value it replaced.
func TestSequencesUnderConcurrency(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	database := spikeDatabase(t, pool)
	if _, _, err := homologateTable(pool, sequencesTableSpec(database), nil); err != nil {
		t.Fatal(err)
	}
	const writers, reservationsPerWriter, idsPerReservation = 16, 20, 5
	var mutex sync.Mutex
	isTaken := map[int64]bool{}
	var waitGroup sync.WaitGroup
	for range writers {
		waitGroup.Go(func() {
			for range reservationsPerWriter {
				firstID, err := reserveSequence(database, "x1_products_0", idsPerReservation)
				if err != nil {
					t.Error(err)
					return
				}
				mutex.Lock()
				for id := firstID; id < firstID+idsPerReservation; id++ {
					if isTaken[id] {
						t.Errorf("ID %d reserved twice", id)
					}
					isTaken[id] = true
				}
				mutex.Unlock()
			}
		})
	}
	waitGroup.Wait()
	const totalIDs = writers * reservationsPerWriter * idsPerReservation
	for id := int64(1); id <= totalIDs; id++ {
		if !isTaken[id] {
			t.Fatalf("ID %d was never reserved (%d reserved)", id, len(isTaken))
		}
	}
	previousValue, err := advanceSequence(database, "x1_products_0", func(int64) int64 { return 7 })
	if err != nil || previousValue != totalIDs {
		t.Fatalf("SetCounter answered %d (err %v), want %d", previousValue, err, totalIDs)
	}
	spikeLog(t, "sequences: %d concurrent reservations of %d IDs, all distinct and gapless; SetCounter answers the replaced value",
		writers*reservationsPerWriter, idsPerReservation)
}

// With blocks on, concurrent writers of one process take distinct, dense keys, and the counter moves
// one block at a time; a request larger than a block takes a block of its own size.
func TestTableKeyBlocks(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	database := spikeDatabase(t, pool)
	if _, _, err := homologateTable(pool, sequencesTableSpec(database), nil); err != nil {
		t.Fatal(err)
	}
	useTableKeyBlocks = true
	t.Cleanup(func() { useTableKeyBlocks = false; tableKeyBlocks.Clear() })
	const writers, reservationsPerWriter = 8, 40
	var mutex sync.Mutex
	isTaken := map[int64]bool{}
	var waitGroup sync.WaitGroup
	for range writers {
		waitGroup.Go(func() {
			for range reservationsPerWriter {
				firstKey, err := reserveTableKeys(database, "x1_sale_order_v3_line_0", 3)
				if err != nil {
					t.Error(err)
					return
				}
				mutex.Lock()
				for key := firstKey; key < firstKey+3; key++ {
					if isTaken[key] {
						t.Errorf("key %d reserved twice", key)
					}
					isTaken[key] = true
				}
				mutex.Unlock()
			}
		})
	}
	waitGroup.Wait()
	// A block of 64 serves 21 requests of 3 (one key left over): 320 requests take 16 blocks.
	const requestsPerBlock = tableKeyBlockSize / 3
	const blocksTaken = (writers*reservationsPerWriter + requestsPerBlock - 1) / requestsPerBlock
	counterValue, err := advanceSequence(database, "x1_sale_order_v3_line_0", func(previous int64) int64 { return previous })
	if err != nil || counterValue != blocksTaken*tableKeyBlockSize {
		t.Fatalf("the counter holds %d (err %v), want %d", counterValue, err, blocksTaken*tableKeyBlockSize)
	}
	firstKey, err := reserveTableKeys(database, "x1_sale_order_v3_line_0", 100)
	if err != nil || firstKey != counterValue+1 {
		t.Fatalf("a 100-key request started at %d (err %v), want %d", firstKey, err, counterValue+1)
	}
	spikeLog(t, "table key blocks: %d concurrent requests, distinct keys, %d sequence transactions", writers*reservationsPerWriter, blocksTaken)
}
