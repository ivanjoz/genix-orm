package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// By-IDs cache (Schema.CacheByIDs). A client asks "give me records [12, 87, 412]
// of partition 5" and gets back only the ones that moved since the value it holds.
//
// It works on slots, not records: a record belongs to slot uint8(ID), and each
// partition (a driver's base pk) has, per year cycle, one 768-byte file in the
// file store holding per slot the year tick of its last write:
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
// Write path (RaiseWrittenSlots): after the records are written, each touched file
// is read with its ETag, its touched slots raised to max(stored, tick), and written
// back with If-Match (retried on a lost race). Setting it after the write keeps it
// race-free: a reader that sees the new tick then reads the records consistently.
//
// Read path (ReadCachedIDsPlan): one GET per requested partition, then the driver
// reads consistently the IDs whose held tick differs from their slot's, and
// overwrites their Updated with toUpdated(slot tick), the value the client sends
// back next time.
//
// A tick is a 2-second window, and a late write lands after a read that already
// returned its tick. So a slot tick is trusted only once its window ended
// SlotTrustTicks ago: a younger one is returned as 0, which forces a read on the
// client's next revalidation.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// SlotTrustTicks covers a write still in flight and the clock skew between servers: 8 seconds.
	SlotTrustTicks = 4
	slotFileSize   = 256 * 3
	// A slot file is raised by a GET + conditional PUT, one winner per round: 30 jittered attempts
	// serialize 20 concurrent writers of one partition.
	slotPutAttempts  = 30
	slotPutPauseStep = 50 * time.Millisecond
	// slotReadConcurrency caps the parallel slot file GETs of one request.
	slotReadConcurrency = 10
	slotFilesFolder     = "ids-cache/"
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
// A zero Partition may be omitted, so the client reads `p || 0`.
type CachedPartition[E any] struct {
	Partition int64 `json:"p"`
	Records   []E   `json:"r"`
}

// SlotOfRecordID buckets a record into one of 256 slots; every path must agree on it.
func SlotOfRecordID(recordID int64) uint8 { return uint8(recordID) }

// CycleStartTime is when the year cycle of a calendar year starts.
func CycleStartTime(cycleYear int) time.Time { return time.Date(cycleYear, 1, 1, 7, 0, 0, 0, time.UTC) }

// YearCycleOf is the year cycle holding moment: the calendar year, until its 07:00 UTC January 1.
func YearCycleOf(moment time.Time) int {
	year := moment.UTC().Year()
	if moment.Before(CycleStartTime(year)) {
		return year - 1
	}
	return year
}

// WrittenYearTick is the tick a write stamped updated sets on its slot, in the cycle starting at
// cycleStart (Updated units).
func WrittenYearTick(updated, cycleStart int64) uint32 {
	return uint32(updated-cycleStart) + 2
}

// YearTickToUpdated is the Updated value a client holds for a tick: the end of its window, the cycle
// start for tick 1, and 0 for an unknown tick.
func YearTickToUpdated(tick uint32, cycleStart int64) int64 {
	if tick == 0 {
		return 0
	}
	return cycleStart + int64(tick) - 1
}

// SUnixTimeToTime turns an SUnixTime back into the start of its 2-second tick.
func SUnixTimeToTime(sUnixTime int64) time.Time { return time.Unix(sUnixTime*2+1e9, 0) }

func SlotFileKey(cycleYear int, basePK string) string {
	return fmt.Sprintf("%s%d/%s", slotFilesFolder, cycleYear, basePK)
}

func SlotTickOf(slotFile []byte, slot uint8) uint32 {
	offset := int(slot) * 3
	return uint32(slotFile[offset]) | uint32(slotFile[offset+1])<<8 | uint32(slotFile[offset+2])<<16
}

func setSlotTick(slotFile []byte, slot uint8, tick uint32) {
	offset := int(slot) * 3
	slotFile[offset], slotFile[offset+1], slotFile[offset+2] = byte(tick), byte(tick>>8), byte(tick>>16)
}

// ReadSlotFile returns a partition's slot file and its ETag; a missing one is all zeros with no ETag.
func ReadSlotFile(key string) ([]byte, string, error) {
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

// SlotWrite is one written record of a CacheByIDs table: its partition's base pk, its ID and the
// Updated it was written with.
type SlotWrite struct {
	BasePK  string
	ID      int64
	Updated int64
}

// RaiseWrittenSlots raises the slot of every written record to the tick of the Updated it was written
// with, one slot file per cycle and base pk. It must run after the records are written.
func RaiseWrittenSlots(writes []SlotWrite) error {
	ticksByFile := map[string]map[uint8]uint32{}
	for _, write := range writes {
		cycleYear := YearCycleOf(SUnixTimeToTime(write.Updated))
		key := SlotFileKey(cycleYear, write.BasePK)
		if ticksByFile[key] == nil {
			ticksByFile[key] = map[uint8]uint32{}
		}
		slot := SlotOfRecordID(write.ID)
		tick := WrittenYearTick(write.Updated, utils.SUnixTimeOf(CycleStartTime(cycleYear)))
		ticksByFile[key][slot] = max(ticksByFile[key][slot], tick)
	}
	for key, ticksBySlot := range ticksByFile {
		if err := RaiseSlotTicks(key, ticksBySlot); err != nil {
			return fmt.Errorf("setting %d slot(s) of %s: %w", len(ticksBySlot), key, err)
		}
	}
	return nil
}

// RaiseSlotTicks raises each slot to its tick when the file holds a lower one, retrying while a
// concurrent writer of the same file wins the conditional PUT. A file already that new isn't written.
func RaiseSlotTicks(key string, ticksBySlot map[uint8]uint32) error {
	for attempt := 1; ; attempt++ {
		slotFile, etag, err := ReadSlotFile(key)
		if err != nil {
			return err
		}
		isRaised := false
		for slot, tick := range ticksBySlot {
			if tick > SlotTickOf(slotFile, slot) {
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

// CachedIDsPlan is a by-IDs request checked against the current cycle's slot files: which IDs the
// driver must read, and the Updated each record it returns carries.
type CachedIDsPlan struct {
	cycleStart          int64
	trustedBelow        int64
	slotFileByPartition map[int64][]byte
	// IDsToRead are the requested IDs whose held tick differs from their slot's, without duplicates.
	IDsToRead []CachedID
}

// ReadCachedIDsPlan reads the current cycle's slot file of every requested partition (basePKByPartition
// names each partition's file) and selects the IDs to read.
func ReadCachedIDsPlan(request CachedIDsRequest, basePKByPartition map[int64]string, now time.Time) (*CachedIDsPlan, error) {
	cycleYear := YearCycleOf(now)
	plan := &CachedIDsPlan{
		cycleStart: utils.SUnixTimeOf(CycleStartTime(cycleYear)),
		// Ticks at or above it may still be raised by a write stamped inside their window.
		trustedBelow: utils.SUnixTimeOf(now) - SlotTrustTicks,
	}
	var err error
	if plan.slotFileByPartition, err = readSlotFiles(cycleYear, basePKByPartition); err != nil {
		return nil, err
	}
	// A client on another cycle computed its ticks against another cycle start: none can be compared.
	isHeldTickComparable := request.CycleYear == cycleYear
	isRequested := map[[2]int64]bool{}
	for _, cachedID := range request.IDs {
		requestKey := [2]int64{cachedID.Partition, cachedID.ID}
		if isRequested[requestKey] {
			continue
		}
		isRequested[requestKey] = true
		slotTick := plan.trustedSlotTick(cachedID.Partition, SlotOfRecordID(cachedID.ID))
		if isHeldTickComparable && slotTick != 0 && slotTick == cachedID.YearTick {
			continue
		}
		plan.IDsToRead = append(plan.IDsToRead, cachedID)
	}
	return plan, nil
}

// trustedSlotTick is the slot's tick, 1 when the cycle has no write to it, and 0 while it is younger
// than SlotTrustTicks.
func (plan *CachedIDsPlan) trustedSlotTick(partition int64, slot uint8) uint32 {
	tick := max(SlotTickOf(plan.slotFileByPartition[partition], slot), 1) // stored 0: no write this cycle
	if YearTickToUpdated(tick, plan.cycleStart) <= plan.trustedBelow {
		return tick
	}
	return 0
}

// HeldUpdated is the Updated a returned record carries: its slot tick as the client holds it, 0 while
// the tick isn't trusted yet.
func (plan *CachedIDsPlan) HeldUpdated(partition, recordID int64) int64 {
	return YearTickToUpdated(plan.trustedSlotTick(partition, SlotOfRecordID(recordID)), plan.cycleStart)
}

// readSlotFiles GETs the slot file of every partition of a cycle, slotReadConcurrency at a time.
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
			slotFile, _, err := ReadSlotFile(SlotFileKey(cycleYear, basePK))
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
			request.IDs = append(request.IDs, CachedID{Partition: partition, ID: id, YearTick: tickBySlot[SlotOfRecordID(id)]})
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
