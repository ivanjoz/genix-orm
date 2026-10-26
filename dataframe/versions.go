package dataframe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// The versions file: when each frame file last changed, so a reader holding a copy
// learns whether to fetch it again without the file being read.
//
//	dtf/<table id>/<partition>.versions   one per table and partition, shared by the
//	                                      table's frames, 1,025 bytes:
//	  u8        format version
//	  uint32    counter: how many slots were ever handed out
//	  255 × uint32  the SUnixTime of the last run that rewrote a file on that slot (0 = never)
//
// Every frame file holds a slot (1..255) in its header for its whole life, handed out
// round-robin (counter % 255 + 1) when a run or a rebuild creates it. Files share
// slots: a slot that moves makes every file on it "changed", which only costs a
// refetch, never hides a change. Files created close together, which are the ones
// that keep changing (the current month of each frame), get distinct slots.
// A run raises the slots of the files it rewrites before writing them and before
// moving its snapshot, so a reader that sees the snapshot past a change also sees
// the change's slot past it.
// ─────────────────────────────────────────────────────────────────────────────

const (
	versionsVersion = 1
	slotCount       = 255
	versionsSize    = 1 + 4 + slotCount*4
	// versionsPutAttempts bounds the retries of a raise against concurrent runs of other frames.
	versionsPutAttempts = 30
	versionsPauseStep   = 20 * time.Millisecond
)

// slotCycle is how many slots the counter cycles through: slotCount, fewer in tests so every slot
// is shared.
var slotCycle = slotCount

// Versions is the decoded versions file: SlotTimes[slot-1] is the SUnixTime of the last run that
// rewrote a file on slot.
type Versions struct {
	Counter   uint32
	SlotTimes [slotCount]uint32
}

// SlotTime is the last change of the files on slot; slot 0 (no slot yet) never reads as unchanged.
func (versions *Versions) SlotTime(slot uint8) int64 {
	if slot == 0 {
		return 1<<62 - 1
	}
	return int64(versions.SlotTimes[slot-1])
}

func encodeVersions(versions *Versions) []byte {
	content := make([]byte, versionsSize)
	content[0] = versionsVersion
	binary.LittleEndian.PutUint32(content[1:], versions.Counter)
	for i, slotTime := range versions.SlotTimes {
		binary.LittleEndian.PutUint32(content[5+i*4:], slotTime)
	}
	return content
}

func decodeVersions(content []byte) (*Versions, error) {
	if len(content) != versionsSize || content[0] != versionsVersion {
		return nil, errors.New("dataframe: the versions file is corrupt")
	}
	versions := &Versions{Counter: binary.LittleEndian.Uint32(content[1:])}
	for i := range versions.SlotTimes {
		versions.SlotTimes[i] = binary.LittleEndian.Uint32(content[5+i*4:])
	}
	return versions, nil
}

// readVersions reads a versions file and its ETag; a missing one is every slot at 0.
func readVersions(store utils.FileStore, versionsKey string) (*Versions, string, error) {
	content, etag, err := store.GetFile(versionsKey)
	if errors.Is(err, utils.ErrFileNotFound) {
		return &Versions{}, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("dataframe: reading the versions file: %w", err)
	}
	versions, err := decodeVersions(content)
	return versions, etag, err
}

// allocateAndRaiseSlots hands newFileCount slots out round-robin and raises them, and the
// rewrittenSlots, to moment (never lower), in one conditional write retried while another run wins
// it. It returns the slots of the new files, in order.
func allocateAndRaiseSlots(ctx context.Context, store utils.FileStore, versionsKey string, newFileCount int, rewrittenSlots []uint8, moment int64) ([]uint8, error) {
	for attempt := 1; ; attempt++ {
		versions, etag, err := readVersions(store, versionsKey)
		if err != nil {
			return nil, err
		}
		newSlots := make([]uint8, newFileCount)
		for i := range newSlots {
			newSlots[i] = uint8(versions.Counter%uint32(slotCycle)) + 1
			versions.Counter++
		}
		isChanged := newFileCount > 0
		for _, slot := range append(newSlots, rewrittenSlots...) {
			if slot != 0 && versions.SlotTimes[slot-1] < uint32(moment) {
				versions.SlotTimes[slot-1] = uint32(moment)
				isChanged = true
			}
		}
		if !isChanged {
			return newSlots, nil
		}
		_, err = store.PutFileIfMatch(ctx, versionsKey, encodeVersions(versions), etag)
		if err == nil {
			return newSlots, nil
		}
		if !errors.Is(err, utils.ErrFilePreconditionFailed) || attempt == versionsPutAttempts {
			return nil, fmt.Errorf("dataframe: writing the versions file: %w", err)
		}
		time.Sleep(time.Duration(rand.Int64N(int64(attempt) * int64(versionsPauseStep))))
	}
}
