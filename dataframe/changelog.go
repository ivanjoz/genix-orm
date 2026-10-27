package dataframe

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
)

// ─────────────────────────────────────────────────────────────────────────────
// Changelog: what a record put in its frames before each write that changed it, newest
// first, over the last ~36 h. dynamo keeps it on the item (cgl); scylla logs one body
// per write in <table>__dtf_log. It names lines, never columns: a backend turns the
// stored and the written record into lines (Table.LinesOf) and the engine only compares
// those, so how the record holds its values (a list column, a status) is not its concern.
//
//	changelog := entry*                          newest first (dynamo)
//	entry     := u16 delta (big-endian), body    delta: Updated − the change's SUnixTime
//	body      := uvarint frame count, frame*     only the frames whose lines changed
//	frame     := u16 shape tag (big-endian)      Frame.shapeTag: a tag no frame has is
//	             uvarint payload length          skipped by its length (another shape:
//	             payload                         that frame is rebuilt anyway)
//	payload   := uvarint line count, then each line's values as zigzag varints, in
//	             Keys, Rows, Sums order (the lines the record held before the change)
//
// Every write of a stored record shifts the deltas by how much Updated moved, drops
// the entries past MaxDelta (≈ 36.4 h) and, when a frame's lines changed, puts first an
// entry with delta 0 and the stored lines of the changed frames. An insert writes none:
// created = Updated says the record did not exist before.
// ─────────────────────────────────────────────────────────────────────────────

// MaxDelta: an entry is kept at most this many SUnixTime ticks (2 s) behind Updated.
const MaxDelta = math.MaxUint16

var errChangelogCorrupt = errors.New("dataframe: changelog is truncated or corrupt")

// loggedLines are the lines one frame held before a change.
type loggedLines struct {
	frameIndex int
	lines      []Line
}

// changelogEntry is one change: delta ticks before Updated, the frames it changed with the lines they held before it.
type changelogEntry struct {
	delta  int64
	frames []loggedLines
}

// isSameLines says whether a frame's lines are unchanged: a deterministic LinesOf gives them in the
// same order, so an order-sensitive compare only costs a needless entry when it does not.
func isSameLines(a, b []Line) bool {
	return slices.EqualFunc(a, b, func(lineA, lineB Line) bool { return slices.Equal(lineA, lineB) })
}

// changedFrames lists, per frame whose lines differ between stored and written, the stored lines.
func (table *Table) changedFrames(storedLines, writtenLines RecordLines) []loggedLines {
	var changed []loggedLines
	for i := range table.Frames {
		if !isSameLines(storedLines[i], writtenLines[i]) {
			changed = append(changed, loggedLines{frameIndex: i, lines: storedLines[i]})
		}
	}
	return changed
}

// EncodeChangedLines is the body of one write over a stored record: the stored lines of every frame
// whose lines the write changes. nil when it changes none. scylla logs it as the write's change.
func (table *Table) EncodeChangedLines(storedLines, writtenLines RecordLines) []byte {
	changed := table.changedFrames(storedLines, writtenLines)
	if len(changed) == 0 {
		return nil
	}
	return table.appendBody(nil, changed)
}

// NextChangelog is the changelog of a record written over a stored one: the stored entries shifted by
// how much Updated moved and cut at MaxDelta, after a new entry with the stored lines of the frames
// that changed. nil when nothing is left: the backend then stores no changelog.
func (table *Table) NextChangelog(storedChangelog []byte, storedUpdated, writtenUpdated int64, storedLines, writtenLines RecordLines) ([]byte, error) {
	entries, err := table.decodeChangelog(storedChangelog)
	if err != nil {
		return nil, err
	}
	// A write stamped before the stored Updated (a lagging clock) shifts nothing.
	shift := max(writtenUpdated-storedUpdated, 0)
	var nextEntries []changelogEntry
	if changed := table.changedFrames(storedLines, writtenLines); len(changed) > 0 {
		nextEntries = append(nextEntries, changelogEntry{delta: 0, frames: changed})
	}
	for _, entry := range entries {
		if entry.delta += shift; entry.delta <= MaxDelta {
			nextEntries = append(nextEntries, entry)
		}
	}
	var out []byte
	for _, entry := range nextEntries {
		out = binary.BigEndian.AppendUint16(out, uint16(entry.delta))
		out = table.appendBody(out, entry.frames)
	}
	return out, nil
}

func (table *Table) appendBody(out []byte, frames []loggedLines) []byte {
	out = binary.AppendUvarint(out, uint64(len(frames)))
	for _, logged := range frames {
		var payload []byte
		payload = binary.AppendUvarint(payload, uint64(len(logged.lines)))
		for _, line := range logged.lines {
			for _, value := range line {
				payload = binary.AppendVarint(payload, value)
			}
		}
		out = binary.BigEndian.AppendUint16(out, table.Frames[logged.frameIndex].shapeTag)
		out = binary.AppendUvarint(out, uint64(len(payload)))
		out = append(out, payload...)
	}
	return out
}

// readBody reads one body at the start of content and returns its frames and its length. A frame of
// a tag no frame has is skipped.
func (table *Table) readBody(content []byte) ([]loggedLines, int, error) {
	frameCount, position := binary.Uvarint(content)
	if position <= 0 {
		return nil, 0, errChangelogCorrupt
	}
	var frames []loggedLines
	for range frameCount {
		if len(content)-position < 2 {
			return nil, 0, errChangelogCorrupt
		}
		shapeTag := binary.BigEndian.Uint16(content[position:])
		position += 2
		payloadLength, size := binary.Uvarint(content[position:])
		if size <= 0 || uint64(len(content)-position-size) < payloadLength {
			return nil, 0, errChangelogCorrupt
		}
		position += size
		payload := content[position : position+int(payloadLength)]
		position += int(payloadLength)
		frameIndex := slices.IndexFunc(table.Frames, func(frame Frame) bool { return frame.shapeTag == shapeTag })
		if frameIndex < 0 {
			continue
		}
		lines, err := decodeLines(payload, table.Frames[frameIndex].lineLength())
		if err != nil {
			return nil, 0, err
		}
		frames = append(frames, loggedLines{frameIndex: frameIndex, lines: lines})
	}
	return frames, position, nil
}

func decodeLines(payload []byte, lineLength int) ([]Line, error) {
	lineCount, position := binary.Uvarint(payload)
	// Every value takes at least one byte: a count past that is corrupt, not an allocation to attempt.
	if position <= 0 || lineCount*uint64(lineLength) > uint64(len(payload)) {
		return nil, errChangelogCorrupt
	}
	lines := make([]Line, lineCount)
	backing := make([]int64, int(lineCount)*lineLength)
	for i := range lines {
		lines[i] = backing[i*lineLength : (i+1)*lineLength : (i+1)*lineLength]
		for v := range lines[i] {
			value, size := binary.Varint(payload[position:])
			if size <= 0 {
				return nil, errChangelogCorrupt
			}
			lines[i][v], position = value, position+size
		}
	}
	if position != len(payload) {
		return nil, errChangelogCorrupt
	}
	return lines, nil
}

func (table *Table) decodeChangelog(content []byte) ([]changelogEntry, error) {
	var entries []changelogEntry
	for position := 0; position < len(content); {
		if len(content)-position < 2 {
			return nil, errChangelogCorrupt
		}
		entry := changelogEntry{delta: int64(binary.BigEndian.Uint16(content[position:]))}
		frames, bodyLength, err := table.readBody(content[position+2:])
		if err != nil {
			return nil, err
		}
		entry.frames = frames
		position += 2 + bodyLength
		entries = append(entries, entry)
	}
	return entries, nil
}

// History is what a stored record says of its past: its partition, the lines it puts in every frame
// now, when it was created and last written (SUnixTimes), and its changes. A backend builds one per
// record it reads for a run, a rebuild or a fresh read.
type History struct {
	// Partition is the record's partition value (non-negative): the folder of its files.
	Partition int64
	RecordKey string
	lines     RecordLines
	created   int64
	updated   int64
	entries   []changelogEntry
}

// NewHistory decodes a stored record's changelog into its History. lines are what it puts in every
// frame now (Table.LinesOf).
func (table *Table) NewHistory(partition int64, recordKey string, lines RecordLines, created, updated int64, changelog []byte) (History, error) {
	entries, err := table.decodeChangelog(changelog)
	if err != nil {
		return History{}, fmt.Errorf("%s: %w", recordKey, err)
	}
	return History{Partition: partition, RecordKey: recordKey, lines: lines, created: created, updated: updated, entries: entries}, nil
}

// HistoryChange is one logged write of a record, for a backend that logs writes beside the table
// instead of keeping a changelog on the record (scylla): its moment (a SUnixTime), whether it was the
// record's insert, and otherwise its body (EncodeChangedLines).
type HistoryChange struct {
	Moment   int64
	IsInsert bool
	Body     []byte
}

// NewHistoryOfChanges builds the History of a record from its lines now and its logged changes,
// newest first. It reaches back to the oldest change given: the backend passes every change after the
// earliest moment the History will be asked about.
func (table *Table) NewHistoryOfChanges(partition int64, recordKey string, lines RecordLines, changes []HistoryChange) (History, error) {
	history := History{Partition: partition, RecordKey: recordKey, lines: lines}
	if len(changes) > 0 {
		history.updated = changes[0].Moment
	}
	for _, change := range changes {
		if change.IsInsert {
			// Before its insert the record did not exist: nothing older applies.
			history.created = change.Moment
			break
		}
		frames, bodyLength, err := table.readBody(change.Body)
		if err != nil || bodyLength != len(change.Body) {
			return History{}, fmt.Errorf("%s: %w", recordKey, errChangelogCorrupt)
		}
		history.entries = append(history.entries, changelogEntry{delta: history.updated - change.Moment, frames: frames})
	}
	return history, nil
}

// linesAt is what the record put in a frame at the SUnixTime moment: false before it was created. A
// change at moment t counts from t on. Exact for moments after the record's Updated − MaxDelta.
func (history History) linesAt(frame *Frame, moment int64) ([]Line, bool) {
	if moment < history.created {
		return nil, false
	}
	lines := history.lines[frame.Index]
	for _, entry := range history.entries {
		if history.updated-entry.delta <= moment {
			break
		}
		for _, logged := range entry.frames {
			if logged.frameIndex == frame.Index {
				lines = logged.lines
			}
		}
	}
	return lines, true
}

// changeMoments lists the moments of the record's changes in (from, to], oldest first.
func (history History) changeMoments(from, to int64) []int64 {
	var moments []int64
	for _, entry := range history.entries {
		if moment := history.updated - entry.delta; moment > from && moment <= to {
			moments = append(moments, moment)
		}
	}
	slices.Reverse(moments)
	return moments
}
