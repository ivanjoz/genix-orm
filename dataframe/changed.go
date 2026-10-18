package dataframe

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Changed files: a client (the browser) keeps raw frame files and asks for the ones
// that changed since its copies. It sends, per file it holds, the snapshot and the
// slot of its copy, and the generation they are of; ReadChanged answers the selected
// files it must replace:
//
//   - every file when the generation differs (a rebuild), or one it doesn't hold;
//   - a copy on slot 0 (made before any run created the file);
//   - a copy whose slot moved past it in the versions file (a run rewrote the file,
//     or another file on the same slot);
//   - a copy a record written since the last run touches: those files go out moved
//     to now, as a fresh Read does, so the client is as current as the records.
//
// The rest go out as the bytes stored. A copy at snapshot S holds every write up to
// S; a later one was taken by a run (which raised the file's slot to its target,
// past S, before moving the snapshot) or is still among the records written after
// it (whose Updated ≥ S). Nothing is missed.
//
// Wire (the HTTP body the client reads):
//
//	u8       wire version
//	uvarint  generation length, generation bytes
//	u8       Rows columns, u8 summed columns (Sums, then the count): what the files decode with
//	uvarint  w: the frame's snapshot (the runs' "as of")
//	uvarint  file count
//	file*    uvarint key count, uvarint × keys, uvarint snapshot, u8 slot,
//	         uvarint content length (0 = no rows), the file's bytes (codec.go)
//
// The held files travel in the query string, ParseHeldFiles:
//
//	<keys>.<snapshot>.<slot>,…    keys '_'-joined decimal, snapshot and slot decimal
// ─────────────────────────────────────────────────────────────────────────────

const wireVersion = 1

// HeldFile is a file a client holds: its Keys, and the snapshot and slot of its copy.
type HeldFile struct {
	Keys     []int64
	Snapshot int64
	Slot     uint8
}

// ChangedFile is a file the client must replace. A nil Content is a file without rows.
type ChangedFile struct {
	Keys     []int64
	Snapshot int64
	Slot     uint8
	Content  []byte
}

// ChangedFiles is a ReadChanged answer: the generation the files are of, the frame's columns, its
// snapshot w and the files to replace.
type ChangedFiles struct {
	Generation     string
	RowColumnCount uint8
	SumsCount      uint8
	Snapshot       int64
	Files          []ChangedFile
}

// ReadChanged answers the selected files the client must replace (above). heldGeneration and held
// describe the client's copies; now is the read's SUnixTime, the snapshot of the files it moves.
func ReadChanged(table *Table, frame *Frame, backend Backend, store utils.FileStore, selection Selection, heldGeneration string, held []HeldFile, now int64) (ChangedFiles, error) {
	readName := fmt.Sprintf("dataframe: %s frame %q", table.Name, frame.Name)
	if err := frame.checkSelection(readName, selection); err != nil {
		return ChangedFiles{}, err
	}
	for attempt := 1; ; attempt++ {
		state, err := backend.ReadState(frame)
		if err != nil {
			return ChangedFiles{}, err
		}
		if state.Generation == "" || state.Shape != frame.Shape {
			return ChangedFiles{}, fmt.Errorf("%s: %w", readName, ErrFrameNotBuilt)
		}
		versions, _, err := ReadVersions(store)
		if err != nil {
			return ChangedFiles{}, err
		}
		histories, err := backend.HistoriesWrittenAfter(state.Snapshot)
		if err != nil {
			return ChangedFiles{}, err
		}
		fileKeys, err := frame.selectedFileKeys(store, state.Generation, selection)
		if err != nil {
			return ChangedFiles{}, fmt.Errorf("%s: %w", readName, err)
		}
		// The last write since w each file was touched by; a file a write creates is not listed yet.
		lastWriteByFileKey := map[string]int64{}
		for _, history := range histories {
			for _, fileKey := range table.filesOfHistory(frame, state.Generation, history, state.Snapshot, math.MaxInt64) {
				lastWriteByFileKey[fileKey] = max(lastWriteByFileKey[fileKey], history.updated)
			}
		}
		for fileKey := range lastWriteByFileKey {
			keys, _ := frame.keysOfFile(state.Generation, fileKey)
			if selection.matches(keys) && !slices.Contains(fileKeys, fileKey) {
				fileKeys = append(fileKeys, fileKey)
			}
		}

		// The held copies count as selected too: a slot-0 copy of a file no run created (its records
		// left it before the next run) is in no listing and touched by no write any more, and must
		// still be answered as empty.
		heldByFileKey := map[string]HeldFile{}
		if heldGeneration == state.Generation {
			for _, heldFile := range held {
				if len(heldFile.Keys) != len(frame.Keys) || !selection.matches(heldFile.Keys) {
					continue
				}
				fileKey := frame.fileKeyOf(state.Generation, heldFile.Keys)
				heldByFileKey[fileKey] = heldFile
				if !slices.Contains(fileKeys, fileKey) {
					fileKeys = append(fileKeys, fileKey)
				}
			}
		}
		var sentKeys []string
		for _, fileKey := range fileKeys {
			heldFile, isHeld := heldByFileKey[fileKey]
			lastWrite, isTouched := lastWriteByFileKey[fileKey]
			if !isHeld || versions.SlotTime(heldFile.Slot) > heldFile.Snapshot || (isTouched && lastWrite >= heldFile.Snapshot) {
				sentKeys = append(sentKeys, fileKey)
			}
		}

		loaded, err := loadFiles(store, frame, sentKeys, state.Snapshot)
		if err != nil {
			return ChangedFiles{}, err
		}
		table.moveRecords(frame, state.Generation, histories, loaded, math.MaxInt64)
		// A run that moved w meanwhile wrote files this read took as of the old w.
		stateAfter, err := backend.ReadState(frame)
		if err != nil {
			return ChangedFiles{}, err
		}
		if stateAfter != state {
			if attempt == maxReadAttempts {
				return ChangedFiles{}, fmt.Errorf("%s: the frame kept moving while it was read: retry", readName)
			}
			continue
		}

		answer := ChangedFiles{Generation: state.Generation, RowColumnCount: uint8(len(frame.Rows)), SumsCount: uint8(frame.SumsCount),
			Snapshot: state.Snapshot, Files: make([]ChangedFile, len(sentKeys))}
		for i, fileKey := range sentKeys {
			keys, _ := frame.keysOfFile(state.Generation, fileKey)
			changed := ChangedFile{Keys: keys, Slot: loaded.slotByFileKey[fileKey]}
			switch _, isTouched := lastWriteByFileKey[fileKey]; {
			case isTouched:
				file := loaded.rowsByFileKey[fileKey].fileAt(now, len(frame.Rows), frame.SumsCount)
				file.Slot, changed.Snapshot = changed.Slot, now
				if file.rowCount() > 0 {
					changed.Content = EncodeFile(file)
				}
			case loaded.contentByFileKey[fileKey] == nil:
				changed.Snapshot = now // no file: no rows as of now, re-asked until a run creates it
			default:
				changed.Snapshot, changed.Content = loaded.snapshotByFileKey[fileKey], loaded.contentByFileKey[fileKey]
			}
			answer.Files[i] = changed
		}
		slices.SortFunc(answer.Files, func(a, b ChangedFile) int { return slices.Compare(a.Keys, b.Keys) })
		return answer, nil
	}
}

// AppendChangedFiles encodes an answer as the wire body (above).
func AppendChangedFiles(out []byte, answer ChangedFiles) []byte {
	out = append(out, wireVersion)
	out = binary.AppendUvarint(out, uint64(len(answer.Generation)))
	out = append(out, answer.Generation...)
	out = append(out, answer.RowColumnCount, answer.SumsCount)
	out = binary.AppendUvarint(out, uint64(answer.Snapshot))
	out = binary.AppendUvarint(out, uint64(len(answer.Files)))
	for _, file := range answer.Files {
		out = binary.AppendUvarint(out, uint64(len(file.Keys)))
		for _, key := range file.Keys {
			out = binary.AppendUvarint(out, uint64(key))
		}
		out = binary.AppendUvarint(out, uint64(file.Snapshot))
		out = append(out, file.Slot)
		out = binary.AppendUvarint(out, uint64(len(file.Content)))
		out = append(out, file.Content...)
	}
	return out
}

var errWireCorrupt = errors.New("dataframe: the changed-files body is truncated or corrupt")

// DecodeChangedFiles reads a wire body written by AppendChangedFiles (the Go twin of the browser's
// decoder, for tests and Go clients).
func DecodeChangedFiles(content []byte) (ChangedFiles, error) {
	decoder := &byteDecoder{data: content, end: len(content)}
	version, err := decoder.readByte()
	if err != nil || version != wireVersion {
		return ChangedFiles{}, errWireCorrupt
	}
	readBytes := func() ([]byte, error) {
		length, err := decoder.readUvarint()
		if err != nil || length > uint64(decoder.end-decoder.position) {
			return nil, errWireCorrupt
		}
		decoder.position += int(length)
		return content[decoder.position-int(length) : decoder.position], nil
	}
	generation, err := readBytes()
	if err != nil {
		return ChangedFiles{}, err
	}
	answer := ChangedFiles{Generation: string(generation)}
	if answer.RowColumnCount, err = decoder.readByte(); err != nil {
		return ChangedFiles{}, errWireCorrupt
	}
	if answer.SumsCount, err = decoder.readByte(); err != nil {
		return ChangedFiles{}, errWireCorrupt
	}
	snapshot, err := decoder.readUvarint()
	if err != nil {
		return ChangedFiles{}, errWireCorrupt
	}
	answer.Snapshot = int64(snapshot)
	fileCount, err := decoder.readUvarint()
	if err != nil || fileCount > uint64(len(content)) {
		return ChangedFiles{}, errWireCorrupt
	}
	for range fileCount {
		keyCount, err := decoder.readUvarint()
		if err != nil || keyCount > 3 {
			return ChangedFiles{}, errWireCorrupt
		}
		file := ChangedFile{Keys: make([]int64, keyCount)}
		for k := range file.Keys {
			key, err := decoder.readUvarint()
			if err != nil {
				return ChangedFiles{}, errWireCorrupt
			}
			file.Keys[k] = int64(key)
		}
		fileSnapshot, err := decoder.readUvarint()
		if err != nil {
			return ChangedFiles{}, errWireCorrupt
		}
		file.Snapshot = int64(fileSnapshot)
		if file.Slot, err = decoder.readByte(); err != nil {
			return ChangedFiles{}, errWireCorrupt
		}
		if file.Content, err = readBytes(); err != nil {
			return ChangedFiles{}, err
		}
		if len(file.Content) == 0 {
			file.Content = nil
		}
		answer.Files = append(answer.Files, file)
	}
	if decoder.position != decoder.end {
		return ChangedFiles{}, errWireCorrupt
	}
	return answer, nil
}

// ParseHeldFiles reads the held files of a query string ("<keys>.<snapshot>.<slot>,…", keys
// '_'-joined). "" is none.
func ParseHeldFiles(text string) ([]HeldFile, error) {
	if text == "" {
		return nil, nil
	}
	var held []HeldFile
	for _, part := range strings.Split(text, ",") {
		fields := strings.Split(part, ".")
		if len(fields) != 3 {
			return nil, fmt.Errorf("dataframe: held file %q is not <keys>.<snapshot>.<slot>", part)
		}
		heldFile := HeldFile{}
		for _, keyText := range strings.Split(fields[0], "_") {
			key, err := strconv.ParseInt(keyText, 10, 64)
			if err != nil || key < 0 {
				return nil, fmt.Errorf("dataframe: held file %q has a bad key", part)
			}
			heldFile.Keys = append(heldFile.Keys, key)
		}
		snapshot, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("dataframe: held file %q has a bad snapshot", part)
		}
		slot, err := strconv.ParseUint(fields[2], 10, 8)
		if err != nil {
			return nil, fmt.Errorf("dataframe: held file %q has a bad slot", part)
		}
		heldFile.Snapshot, heldFile.Slot = snapshot, uint8(slot)
		held = append(held, heldFile)
	}
	return held, nil
}

// FormatHeldFiles writes held files as ParseHeldFiles reads them (Go clients and tests).
func FormatHeldFiles(held []HeldFile) string {
	parts := make([]string, len(held))
	for i, heldFile := range held {
		keyTexts := make([]string, len(heldFile.Keys))
		for k, key := range heldFile.Keys {
			keyTexts[k] = strconv.FormatInt(key, 10)
		}
		parts[i] = strings.Join(keyTexts, "_") + "." + strconv.FormatInt(heldFile.Snapshot, 10) + "." + strconv.Itoa(int(heldFile.Slot))
	}
	return strings.Join(parts, ",")
}
