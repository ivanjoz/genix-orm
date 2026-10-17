package dynamo

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// By-IDs cache (Schema.CacheByIDs). A client asks "give me records [12, 87, 412]
// of partition 5" and gets back only the ones that moved since the value it holds.
//
// It works on slots, not records: a record belongs to slot uint8(ID), and each
// partition (base pk) has, per year cycle, one 768-byte file in the file store
// (file_store.go, S3) holding per slot the year tick of its last write:
//
//	key = ids-cache/<cycle year>/<base pk>   256 × uint24 LE, slot i at [3i, 3i+3); missing = zeros
//
// A year cycle starts on January 1 at 07:00 UTC. A year tick counts 2 seconds
// from it, one Updated (SUnixTime) unit (a leap year is 15,811,200 ticks, under
// uint24's 16,777,215):
//
//	0   unknown: nothing held, or a slot too young to trust. Always forces a read
//	1   no write to the slot in this cycle (the file stores 0)
//	≥2  Updated − cycleStart + 2, from the written record's Updated
//
// The client holds ticks as Updated values: toUpdated(t) = cycleStart + t − 1, the
// end of the tick's window, and converts them back with upd − cycleStart + 1. A
// value of a past cycle converts to 0, so on January 1 every record is read once
// more: that is the yearly reset, with no job.
//
// Write path: after the records are written, each touched file is read with its
// ETag, its touched slots raised to max(stored, tick), and written back with
// If-Match (retried on a lost race). Setting it after the write keeps it race-free:
// a reader that sees the new tick then reads the records consistently.
//
// Read path: one GET per requested partition, then one consistent BatchGetItem of
// the IDs whose held tick differs from their slot's; their Updated is overwritten
// with toUpdated(slot tick), the value the client sends back next time.
//
// A tick is a 2-second window, and a late write lands after a read that already
// returned its tick. So a slot tick is trusted only once its window ended
// slotTrustTicks ago: a younger one is returned as 0, which forces a read on the
// client's next revalidation.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// slotTrustTicks covers a write still in flight and the clock skew between Lambdas: 8 seconds.
	slotTrustTicks = 4
	slotFileSize   = 256 * 3
	// A slot file is raised by a GET + conditional PUT, one winner per round: 30 jittered attempts
	// serialize 20 concurrent writers of one partition.
	slotPutAttempts  = 30
	slotPutPauseStep = 50 * time.Millisecond
	// slotReadConcurrency caps the parallel slot file GETs of one request.
	slotReadConcurrency = 10
	// slotFilesFolder is the slot files' folder in the file store (file_store.go).
	slotFilesFolder = "ids-cache/"
)

// CachedID is one ID of a by-IDs request, with the year tick the client holds for its slot. 0 means
// "nothing held" and always forces a read. Partition is the value of the table's single Partition
// column, 0 for a table without one.
type CachedID struct {
	Partition int64
	ID        int64
	YearTick  uint32
}

// CachedIDsRequest is a decoded by-IDs request (DecodeCachedIDsRequest). CycleYear is the year cycle
// the client computed its ticks in: when it isn't the server's, every tick is ignored.
type CachedIDsRequest struct {
	CycleYear int
	IDs       []CachedID
}

// CachedPartition is the response of a by-IDs request: the records of one partition that moved.
// minijson omits a zero Partition, so the client reads `p || 0`.
type CachedPartition[E any] struct {
	Partition int64 `json:"p"`
	Records   []E   `json:"r"`
}

type cacheByIDsConfig struct {
	idColumn keyCol // the single integer Keys column; its value picks the slot
}

// resolveCacheByIDs validates CacheByIDs' requirements at compile time. The Updated field itself is
// validated by resolveUpdated; every Partition column is an integer already.
func resolveCacheByIDs(recordType reflect.Type, partition, keys []keyCol) *cacheByIDsConfig {
	if len(keys) != 1 || !keys[0].kind.isInteger() {
		panic(fmt.Sprintf("db: %s uses CacheByIDs and needs exactly one integer Keys column (the ID)", recordType.Name()))
	}
	if len(partition) > 1 {
		panic(fmt.Sprintf("db: %s uses CacheByIDs and allows at most one Partition column: a request names its partition by one integer", recordType.Name()))
	}
	return &cacheByIDsConfig{idColumn: keys[0]}
}

// slotOfRecordID buckets a record into one of 256 slots; every path must agree on it.
func slotOfRecordID(recordID int64) uint8 { return uint8(recordID) }

// cycleStartTime is when the year cycle of a calendar year starts.
func cycleStartTime(cycleYear int) time.Time { return time.Date(cycleYear, 1, 1, 7, 0, 0, 0, time.UTC) }

// yearCycleOf is the year cycle holding moment: the calendar year, until its 07:00 UTC January 1.
func yearCycleOf(moment time.Time) int {
	year := moment.UTC().Year()
	if moment.Before(cycleStartTime(year)) {
		return year - 1
	}
	return year
}

// CurrentYearCycle is the year cycle a client must compute its ticks in.
func CurrentYearCycle() int { return yearCycleOf(Now()) }

// writtenYearTick is the tick a write stamped updated sets on its slot, in the cycle starting at
// cycleStart (Updated units).
func writtenYearTick(updated, cycleStart int64) uint32 {
	return uint32(updated-cycleStart) + 2
}

// yearTickToUpdated is the Updated value a client holds for a tick: the end of its window, the cycle
// start for tick 1, and 0 for an unknown tick.
func yearTickToUpdated(tick uint32, cycleStart int64) int64 {
	if tick == 0 {
		return 0
	}
	return cycleStart + int64(tick) - 1
}

func slotFileKey(cycleYear int, basePK string) string {
	return fmt.Sprintf("%s%d/%s", slotFilesFolder, cycleYear, basePK)
}

func slotTickOf(slotFile []byte, slot uint8) uint32 {
	offset := int(slot) * 3
	return uint32(slotFile[offset]) | uint32(slotFile[offset+1])<<8 | uint32(slotFile[offset+2])<<16
}

func setSlotTick(slotFile []byte, slot uint8, tick uint32) {
	offset := int(slot) * 3
	slotFile[offset], slotFile[offset+1], slotFile[offset+2] = byte(tick), byte(tick>>8), byte(tick>>16)
}

// readSlotFile returns a partition's slot file and its ETag; a missing one is all zeros with no ETag.
func readSlotFile(key string) ([]byte, string, error) {
	content, etag, err := fileStore.GetFile(key)
	if errors.Is(err, utils.ErrFileNotFound) {
		return make([]byte, slotFileSize), "", nil
	}
	if err != nil {
		return nil, "", err
	}
	slotFile := make([]byte, slotFileSize) // a shorter file reads as zeros past its end
	copy(slotFile, content)
	return slotFile, etag, nil
}

// setSlotsUpdated raises the slot of every written record to the tick of the Updated it was written
// with (Delete stamps its key record too), one slot file per cycle and base pk. It must run after the
// records are written.
func (m *tableMeta) setSlotsUpdated(ptrs []unsafe.Pointer) error {
	if m.cacheByIDs == nil {
		return nil
	}
	if fileStore == nil {
		return fmt.Errorf("db: %s uses CacheByIDs and no file store is set (SetFileStore)", m.recordType.Name())
	}
	ticksByFile := map[string]map[uint8]uint32{}
	for _, ptr := range ptrs {
		updated := m.updated.acc.getI64(ptr)
		cycleYear := yearCycleOf(sUnixTimeToTime(updated))
		key := slotFileKey(cycleYear, m.pkValue(ptr))
		if ticksByFile[key] == nil {
			ticksByFile[key] = map[uint8]uint32{}
		}
		slot := slotOfRecordID(m.cacheByIDs.idColumn.acc.getI64(ptr))
		tick := writtenYearTick(updated, utils.SUnixTimeOf(cycleStartTime(cycleYear)))
		ticksByFile[key][slot] = max(ticksByFile[key][slot], tick)
	}
	for key, ticksBySlot := range ticksByFile {
		if err := raiseSlotTicks(key, ticksBySlot); err != nil {
			return fmt.Errorf("db: %s setting %d slot(s) of %s: %w", m.recordType.Name(), len(ticksBySlot), key, err)
		}
	}
	return nil
}

// raiseSlotTicks raises each slot to its tick when the file holds a lower one, retrying while a
// concurrent writer of the same file wins the conditional PUT. A file already that new isn't written.
func raiseSlotTicks(key string, ticksBySlot map[uint8]uint32) error {
	for attempt := 1; ; attempt++ {
		slotFile, etag, err := readSlotFile(key)
		if err != nil {
			return err
		}
		isRaised := false
		for slot, tick := range ticksBySlot {
			if tick > slotTickOf(slotFile, slot) {
				setSlotTick(slotFile, slot, tick)
				isRaised = true
			}
		}
		if !isRaised {
			return nil
		}
		_, err = fileStore.PutFileIfMatch(context.Background(), key, slotFile, etag)
		if !errors.Is(err, utils.ErrFilePreconditionFailed) || attempt == slotPutAttempts {
			return err
		}
		time.Sleep(time.Duration(rand.Int64N(int64(attempt) * int64(slotPutPauseStep))))
	}
}

// QueryCachedIDs resolves records by ID for a CacheByIDs entity, skipping every ID whose held tick
// still equals its slot's. IDs that don't exist are left out, like the unchanged ones. The records
// come grouped by partition, ascending, and carry toUpdated(slot tick) in Updated, or 0 while the
// tick is younger than slotTrustTicks.
func (r *Repo[T, E]) QueryCachedIDs(request CachedIDsRequest) ([]CachedPartition[E], error) {
	m := r.meta
	if m.cacheByIDs == nil {
		return nil, fmt.Errorf("db: %s QueryCachedIDs needs CacheByIDs in its schema", m.recordType.Name())
	}
	if fileStore == nil {
		return nil, fmt.Errorf("db: %s uses CacheByIDs and no file store is set (SetFileStore)", m.recordType.Name())
	}
	if len(request.IDs) == 0 {
		return nil, nil
	}
	idColumn := m.cacheByIDs.idColumn

	// A scratch record carries each partition and ID in turn, so the keys are built by the same code
	// as on write.
	var keyRecord E
	keyPtr := unsafe.Pointer(&keyRecord)
	basePKByPartition := map[int64]string{}
	for _, cachedID := range request.IDs {
		if cachedID.ID < 0 || uint64(cachedID.ID) > maxValueForBits(idColumn.bits) {
			return nil, fmt.Errorf("db: %s ID %d is outside %s's Size(%d)", m.recordType.Name(), cachedID.ID, idColumn.fieldName, idColumn.bits)
		}
		if _, isKnown := basePKByPartition[cachedID.Partition]; isKnown {
			continue
		}
		if len(m.partition) == 0 {
			if cachedID.Partition != 0 {
				return nil, fmt.Errorf("db: %s has no Partition, got partition %d", m.recordType.Name(), cachedID.Partition)
			}
		} else {
			partitionColumn := m.partition[0]
			if cachedID.Partition < 0 || uint64(cachedID.Partition) > maxValueForBits(partitionColumn.bits) {
				return nil, fmt.Errorf("db: %s partition %d is outside %s's Size(%d)", m.recordType.Name(), cachedID.Partition, partitionColumn.fieldName, partitionColumn.bits)
			}
			partitionColumn.acc.setI64(keyPtr, cachedID.Partition)
		}
		basePKByPartition[cachedID.Partition] = m.pkValue(keyPtr)
	}

	cycleYear := CurrentYearCycle()
	cycleStart := utils.SUnixTimeOf(cycleStartTime(cycleYear))
	slotFileByPartition, err := readSlotFiles(cycleYear, basePKByPartition)
	if err != nil {
		return nil, err
	}

	// Ticks at or above it may still be raised by a write stamped inside their window.
	trustedBelow := utils.SUnixTimeOf(Now()) - slotTrustTicks
	trustedSlotTick := func(partition int64, slot uint8) uint32 {
		tick := max(slotTickOf(slotFileByPartition[partition], slot), 1) // stored 0: no write this cycle
		if yearTickToUpdated(tick, cycleStart) <= trustedBelow {
			return tick
		}
		return 0
	}

	// A client on another cycle computed its ticks against another cycle start: none can be compared.
	isHeldTickComparable := request.CycleYear == cycleYear
	var keysToRead []map[string]types.AttributeValue
	isRequested := map[[2]int64]bool{} // BatchGetItem rejects duplicate keys
	for _, cachedID := range request.IDs {
		requestKey := [2]int64{cachedID.Partition, cachedID.ID}
		if isRequested[requestKey] {
			continue
		}
		isRequested[requestKey] = true
		slotTick := trustedSlotTick(cachedID.Partition, slotOfRecordID(cachedID.ID))
		if isHeldTickComparable && slotTick != 0 && slotTick == cachedID.YearTick {
			continue
		}
		if len(m.partition) > 0 {
			m.partition[0].acc.setI64(keyPtr, cachedID.Partition)
		}
		idColumn.acc.setI64(keyPtr, cachedID.ID)
		keysToRead = append(keysToRead, m.keyOnly(keyPtr))
	}
	if len(keysToRead) == 0 {
		return nil, nil
	}

	client, err := Client()
	if err != nil {
		return nil, err
	}
	// Consistent, so a record is at least as new as the slot tick stamped on it.
	items, _, err := batchGet(client, keysToRead, true)
	if err != nil {
		return nil, err
	}
	recordsByPartition := map[int64][]E{}
	for _, item := range items {
		var record E
		if err := m.unmarshalItem(item, &record); err != nil {
			return nil, err
		}
		recordPtr := unsafe.Pointer(&record)
		var partition int64
		if len(m.partition) > 0 {
			partition = m.partition[0].acc.getI64(recordPtr)
		}
		slotTick := trustedSlotTick(partition, slotOfRecordID(idColumn.acc.getI64(recordPtr)))
		m.updated.acc.setI64(recordPtr, yearTickToUpdated(slotTick, cycleStart))
		recordsByPartition[partition] = append(recordsByPartition[partition], record)
	}
	cachedPartitions := make([]CachedPartition[E], 0, len(recordsByPartition))
	for partition, records := range recordsByPartition {
		cachedPartitions = append(cachedPartitions, CachedPartition[E]{Partition: partition, Records: records})
	}
	slices.SortFunc(cachedPartitions, func(a, b CachedPartition[E]) int { return cmp.Compare(a.Partition, b.Partition) })
	return cachedPartitions, nil
}

// readSlotFiles GETs the current cycle's slot file of every partition, slotReadConcurrency at a time.
func readSlotFiles(cycleYear int, basePKByPartition map[int64]string) (map[int64][]byte, error) {
	slotFileByPartition := make(map[int64][]byte, len(basePKByPartition))
	var mutex sync.Mutex
	var firstErr error
	var waitGroup sync.WaitGroup
	readSlots := make(chan struct{}, slotReadConcurrency)
	for partition, basePK := range basePKByPartition {
		waitGroup.Add(1)
		readSlots <- struct{}{}
		go func() {
			defer func() { <-readSlots; waitGroup.Done() }()
			slotFile, _, err := readSlotFile(slotFileKey(cycleYear, basePK))
			mutex.Lock()
			defer mutex.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("db: reading the slot file of partition %d: %w", partition, err)
			}
			slotFileByPartition[partition] = slotFile
		}()
	}
	waitGroup.Wait()
	return slotFileByPartition, firstErr
}

// DecodeCachedIDsRequest decodes genix-ui's by-IDs request (the "cc" query parameter), utils.Base64 of:
//
//	[cycle year − 2000: u8]
//	[partition count: uvarint]
//	per partition, ascending:
//	  [partition delta: uvarint]                  the first one is absolute
//	  [slot count: uvarint]
//	  [slot: u8][year tick: u24 LE] × slot count   ascending by slot, ticks ≥ 1
//	  [ID count: uvarint]
//	  [ID delta: uvarint] × ID count              ascending IDs, the first one absolute
//
// An ID takes its slot's tick, 0 when the slot isn't listed. The client sends the lowest tick it holds
// among a slot's requested IDs (a record it doesn't hold counts as 0, which leaves the slot out).
func DecodeCachedIDsRequest(encodedRequest string) (CachedIDsRequest, error) {
	payload, err := utils.Base64.DecodeString(encodedRequest)
	if err != nil || len(payload) == 0 {
		return CachedIDsRequest{}, fmt.Errorf("the by-IDs request is not valid base64")
	}
	reader := cachedIDsReader{payload: payload, position: 1}
	request := CachedIDsRequest{CycleYear: 2000 + int(payload[0])}

	partitionCount := reader.count()
	var partition int64
	for partitionIndex := uint64(0); partitionIndex < partitionCount && reader.err == nil; partitionIndex++ {
		partitionDelta := reader.uvarint()
		if partitionIndex > 0 && partitionDelta == 0 {
			reader.fail("partitions must be ascending")
		}
		partition += int64(partitionDelta)

		var tickBySlot [256]uint32
		slotCount := reader.count()
		if slotCount > 256 {
			reader.fail("more than 256 slots")
		}
		previousSlot := -1
		for slotIndex := uint64(0); slotIndex < slotCount && reader.err == nil; slotIndex++ {
			slot, tick := reader.slotTick()
			if int(slot) <= previousSlot || tick == 0 {
				reader.fail("slots must be ascending with ticks above 0")
			}
			previousSlot = int(slot)
			tickBySlot[slot] = tick
		}

		idCount := reader.count()
		var id int64
		for idIndex := uint64(0); idIndex < idCount && reader.err == nil; idIndex++ {
			idDelta := reader.uvarint()
			if idIndex > 0 && idDelta == 0 {
				reader.fail("IDs must be ascending")
			}
			id += int64(idDelta)
			request.IDs = append(request.IDs, CachedID{Partition: partition, ID: id, YearTick: tickBySlot[slotOfRecordID(id)]})
		}
	}
	if reader.err == nil && reader.position != len(payload) {
		reader.fail("trailing bytes")
	}
	if reader.err != nil {
		return CachedIDsRequest{}, reader.err
	}
	return request, nil
}

// cachedIDsReader walks a by-IDs payload; the first malformed read sets err and the rest read zeros.
type cachedIDsReader struct {
	payload  []byte
	position int
	err      error
}

func (reader *cachedIDsReader) fail(reason string) {
	if reader.err == nil {
		reader.err = fmt.Errorf("the by-IDs request is malformed at byte %d: %s", reader.position, reason)
	}
}

func (reader *cachedIDsReader) uvarint() uint64 {
	if reader.err != nil {
		return 0
	}
	value, width := binary.Uvarint(reader.payload[reader.position:])
	if width <= 0 || value > 1<<62 {
		reader.fail("bad uvarint")
		return 0
	}
	reader.position += width
	return value
}

// count reads an element count, refusing one the rest of the payload can't hold (each element takes
// at least a byte), so a forged count can't allocate.
func (reader *cachedIDsReader) count() uint64 {
	count := reader.uvarint()
	if count > uint64(len(reader.payload)-reader.position) {
		reader.fail("count beyond the payload")
		return 0
	}
	return count
}

func (reader *cachedIDsReader) slotTick() (uint8, uint32) {
	if reader.err != nil {
		return 0, 0
	}
	if reader.position+4 > len(reader.payload) {
		reader.fail("truncated slot")
		return 0, 0
	}
	entry := reader.payload[reader.position:]
	reader.position += 4
	return entry[0], uint32(entry[1]) | uint32(entry[2])<<8 | uint32(entry[3])<<16
}
