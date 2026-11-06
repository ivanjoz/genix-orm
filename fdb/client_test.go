package fdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

// Integration tests need a running fdbgate (see container/): GENIX_FDB_IT=1 and
// FDBGATE_SOCKET=/run/user/<uid>/genix-fdb/fdbgate.sock.
func dialForTest(t testing.TB) *Client {
	t.Helper()
	if os.Getenv("GENIX_FDB_IT") != "1" && os.Getenv("GENIX_FDB_BENCH") != "1" {
		t.Skip("set GENIX_FDB_IT=1 (and FDBGATE_SOCKET) to run against a live fdbgate")
	}
	client, dialError := Dial(os.Getenv("FDBGATE_SOCKET"), DefaultPoolSize)
	if dialError != nil {
		t.Fatal(dialError)
	}
	t.Cleanup(client.Close)
	return client
}

func testKey(testName string, parts ...string) []byte {
	key := []byte("it/" + testName + "/")
	for _, part := range parts {
		key = append(key, part...)
	}
	return key
}

func prefixEnd(prefix []byte) []byte { return append(bytes.Clone(prefix), 0xFF) }

func TestFrameRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	if writeError := WriteFrame(&buffer, uint8(OpGetMany), 7, []byte("payload")); writeError != nil {
		t.Fatal(writeError)
	}
	lead, correlation, payload, readError := ReadFrame(&buffer)
	if readError != nil || lead != uint8(OpGetMany) || correlation != 7 || string(payload) != "payload" {
		t.Fatalf("round trip mismatch: %d %d %q %v", lead, correlation, payload, readError)
	}
}

func TestReadFrameRejectsOversizedPayload(t *testing.T) {
	header := []byte{0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, _, _, readError := ReadFrame(bytes.NewReader(header)); readError == nil {
		t.Fatal("expected an error for a payload above MaxFrameBytes")
	}
}

func TestMessagesRoundTripThroughColbin(t *testing.T) {
	original := CommitRequest{
		Conditions: []Condition{{Kind: ConditionKeyAbsent, Key: []byte("k")}},
		Mutations: []Mutation{
			{Kind: MutationSet, Key: []byte("a"), Value: []byte("1")},
			{Kind: MutationClearRange, Key: []byte("b"), EndKey: []byte("c")},
		},
	}
	encoded, encodeError := Encode(&original)
	if encodeError != nil {
		t.Fatal(encodeError)
	}
	var decoded CommitRequest
	if decodeError := Decode(encoded, &decoded); decodeError != nil {
		t.Fatal(decodeError)
	}
	if len(decoded.Mutations) != 2 || string(decoded.Mutations[1].EndKey) != "c" || decoded.Conditions[0].Kind != ConditionKeyAbsent {
		t.Fatalf("decoded mismatch: %+v", decoded)
	}
}

func TestLivePingGetManyAndCommit(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	if _, pingError := client.Ping(ctx); pingError != nil {
		t.Fatal(pingError)
	}

	presentKey, absentKey := testKey(t.Name(), "present"), testKey(t.Name(), "absent")
	if _, commitError := client.Commit(ctx, nil, []Mutation{
		{Kind: MutationClear, Key: absentKey},
		{Kind: MutationSet, Key: presentKey, Value: []byte{}}, // empty value must stay "present"
	}); commitError != nil {
		t.Fatal(commitError)
	}
	values, present, getError := client.GetMany(ctx, [][]byte{presentKey, absentKey}, false)
	if getError != nil {
		t.Fatal(getError)
	}
	if !present[0] || present[1] || len(values[0]) != 0 {
		t.Fatalf("presence mismatch: present=%v values=%q", present, values)
	}
}

func TestLiveEveryConditionKind(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	storedKey, missingKey := testKey(t.Name(), "stored"), testKey(t.Name(), "missing")
	if _, commitError := client.Commit(ctx, nil, []Mutation{
		{Kind: MutationClear, Key: missingKey},
		{Kind: MutationSet, Key: storedKey, Value: []byte("abcdef")},
	}); commitError != nil {
		t.Fatal(commitError)
	}
	marker := []Mutation{{Kind: MutationSet, Key: testKey(t.Name(), "marker"), Value: []byte("x")}}

	holding := [][]Condition{
		{{Kind: ConditionKeyAbsent, Key: missingKey}},
		{{Kind: ConditionKeyPresent, Key: storedKey}},
		{{Kind: ConditionValueEquals, Key: storedKey, Value: []byte("abcdef")}},
		{{Kind: ConditionValuePrefixEquals, Key: storedKey, Value: []byte("abc")}},
	}
	for caseIndex, conditions := range holding {
		if _, commitError := client.Commit(ctx, conditions, marker); commitError != nil {
			t.Fatalf("holding case %d: %v", caseIndex, commitError)
		}
	}

	failing := [][]Condition{
		{{Kind: ConditionKeyAbsent, Key: storedKey}},
		{{Kind: ConditionKeyPresent, Key: missingKey}},
		{{Kind: ConditionValueEquals, Key: storedKey, Value: []byte("abc")}},
		{{Kind: ConditionValuePrefixEquals, Key: storedKey, Value: []byte("xyz")}},
	}
	for caseIndex, conditions := range failing {
		// Put the failing condition second so the reported index proves which one failed.
		withLeadingPass := append([]Condition{{Kind: ConditionKeyPresent, Key: storedKey}}, conditions...)
		_, commitError := client.Commit(ctx, withLeadingPass, marker)
		var failed *ErrConditionFailed
		if !errors.As(commitError, &failed) || failed.Index != 1 {
			t.Fatalf("failing case %d: want condition 1 failed, got %v", caseIndex, commitError)
		}
	}
}

func TestLiveGetRangesPaginatesWithContinuation(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	rangePrefix := testKey(t.Name(), "row/")
	mutations := []Mutation{{Kind: MutationClearRange, Key: rangePrefix, EndKey: prefixEnd(rangePrefix)}}
	for rowIndex := range 25 {
		mutations = append(mutations, Mutation{Kind: MutationSet, Key: append(bytes.Clone(rangePrefix), fmt.Sprintf("%03d", rowIndex)...), Value: []byte{byte(rowIndex)}})
	}
	if _, commitError := client.Commit(ctx, nil, mutations); commitError != nil {
		t.Fatal(commitError)
	}

	for _, reverse := range []bool{false, true} {
		begin, end := rangePrefix, prefixEnd(rangePrefix)
		var collected []byte
		for pageCount := 0; ; pageCount++ {
			results, rangeError := client.GetRanges(ctx, []Range{{Begin: begin, End: end, Limit: 10, Reverse: reverse}})
			if rangeError != nil {
				t.Fatal(rangeError)
			}
			for _, value := range results[0].Values.Slices() {
				collected = append(collected, value[0])
			}
			if !results[0].More {
				if pageCount != 2 {
					t.Fatalf("reverse=%v: want 3 pages, got %d", reverse, pageCount+1)
				}
				break
			}
			if reverse {
				end = results[0].NextBegin
			} else {
				begin = results[0].NextBegin
			}
		}
		if len(collected) != 25 {
			t.Fatalf("reverse=%v: collected %d rows", reverse, len(collected))
		}
		for position, rowValue := range collected {
			wantValue := byte(position)
			if reverse {
				wantValue = byte(24 - position)
			}
			if rowValue != wantValue {
				t.Fatalf("reverse=%v: position %d holds %d, want %d", reverse, position, rowValue, wantValue)
			}
		}
	}
}

func TestLiveIndexRangeGetFollowsEntriesToRecords(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	recordPrefix, indexPrefix := testKey(t.Name(), "r/"), testKey(t.Name(), "i/")
	if _, commitError := client.Commit(ctx, nil, []Mutation{
		{Kind: MutationSet, Key: append(bytes.Clone(recordPrefix), "pk1"...), Value: []byte("record-one")},
		{Kind: MutationSet, Key: append(bytes.Clone(recordPrefix), "pk2"...), Value: []byte("record-two")},
		{Kind: MutationSet, Key: append(bytes.Clone(indexPrefix), "a"...), Value: []byte("pk1")},
		{Kind: MutationSet, Key: append(bytes.Clone(indexPrefix), "b"...), Value: []byte("pk2")},
		{Kind: MutationSet, Key: append(bytes.Clone(indexPrefix), "c"...), Value: []byte("dangling")},
	}); commitError != nil {
		t.Fatal(commitError)
	}
	var reply IndexRangeGetReply
	if callError := client.callOK(ctx, OpIndexRangeGet, &IndexRangeGetRequest{
		Ranges:         []Range{{Begin: indexPrefix, End: prefixEnd(indexPrefix)}},
		RecordPrefixes: NewByteList([][]byte{recordPrefix}),
	}, &reply); callError != nil {
		t.Fatal(callError)
	}
	records := reply.Records.Slices()
	if len(records) != 3 || string(records[0]) != "record-one" || string(records[1]) != "record-two" || !reply.Records.Absent(2) {
		t.Fatalf("records mismatch: %q", records)
	}
}

func TestLiveConcurrentReserveRangeNeverOverlaps(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	counterKey := testKey(t.Name(), "counter")
	if callError := client.callOK(ctx, OpSetCounter, &SetCounterRequest{CounterKey: counterKey, Value: 0}, &SetCounterReply{}); callError != nil {
		t.Fatal(callError)
	}

	const workers, reservationsPerWorker, blockSize = 16, 10, 5
	var waitGroup sync.WaitGroup
	var mutex sync.Mutex
	taken := map[int64]bool{}
	for range workers {
		waitGroup.Go(func() {
			for range reservationsPerWorker {
				var reply ReserveRangeReply
				if callError := client.callOK(ctx, OpReserveRange, &ReserveRangeRequest{CounterKey: counterKey, Count: blockSize}, &reply); callError != nil {
					t.Error(callError)
					return
				}
				mutex.Lock()
				for id := reply.First; id < reply.First+blockSize; id++ {
					if taken[id] {
						t.Errorf("id %d reserved twice", id)
					}
					taken[id] = true
				}
				mutex.Unlock()
			}
		})
	}
	waitGroup.Wait()
	if len(taken) != workers*reservationsPerWorker*blockSize {
		t.Fatalf("reserved %d ids, want %d", len(taken), workers*reservationsPerWorker*blockSize)
	}
}

func TestLiveClearPrefixRepeatsUntilDone(t *testing.T) {
	client := dialForTest(t)
	ctx := context.Background()
	clearedPrefix := testKey(t.Name(), "bulk/")
	// More than one 10,000-key chunk, written in several commits to stay under the size limit.
	const totalKeys = 12_000
	for batchStart := 0; batchStart < totalKeys; batchStart += 2000 {
		mutations := make([]Mutation, 0, 2000)
		for keyIndex := batchStart; keyIndex < batchStart+2000; keyIndex++ {
			mutations = append(mutations, Mutation{Kind: MutationSet, Key: append(bytes.Clone(clearedPrefix), fmt.Sprintf("%06d", keyIndex)...), Value: []byte("v")})
		}
		if _, commitError := client.Commit(ctx, nil, mutations); commitError != nil {
			t.Fatal(commitError)
		}
	}
	callCount := 0
	for done := false; !done; callCount++ {
		var reply ClearPrefixReply
		if callError := client.callOK(ctx, OpClearPrefix, &ClearPrefixRequest{Prefix: clearedPrefix}, &reply); callError != nil {
			t.Fatal(callError)
		}
		done = reply.Done
	}
	if callCount != 2 {
		t.Fatalf("want 2 chunks for %d keys, got %d", totalKeys, callCount)
	}
	results, rangeError := client.GetRanges(ctx, []Range{{Begin: clearedPrefix, End: prefixEnd(clearedPrefix), Limit: 1}})
	if rangeError != nil {
		t.Fatal(rangeError)
	}
	if results[0].Keys.Len() != 0 {
		t.Fatalf("prefix not empty after clear: %d keys left", results[0].Keys.Len())
	}
}

func TestLiveOversizedCommitAnswersTooLarge(t *testing.T) {
	client := dialForTest(t)
	// FDB's transaction size limit is 10 MB: 120 values of 90 KB is ~10.8 MB.
	mutations := make([]Mutation, 0, 120)
	for keyIndex := range 120 {
		mutations = append(mutations, Mutation{Kind: MutationSet, Key: testKey(t.Name(), fmt.Sprint(keyIndex)), Value: bytes.Repeat([]byte{1}, 90_000)})
	}
	_, commitError := client.Commit(context.Background(), nil, mutations)
	var frameError *Error
	if !errors.As(commitError, &frameError) || frameError.Status != StatusTooLarge {
		t.Fatalf("want StatusTooLarge, got %v", commitError)
	}
}
