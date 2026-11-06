// Package fdb is the FoundationDB driver for genix-orm. It never links libfdb_c: it talks to the
// fdbgate daemon (a sibling module that does) over a unix socket, using the frames defined here.
package fdb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ivanjoz/colbin"
)

// Op is the first byte of a request frame.
type Op uint8

const (
	OpGetMany       Op = 0x01
	OpGetRanges     Op = 0x02
	OpIndexRangeGet Op = 0x03
	OpCommit        Op = 0x04
	OpReserveRange  Op = 0x05
	OpSetCounter    Op = 0x06
	OpClearPrefix   Op = 0x07
	OpPing          Op = 0x08
)

// Status is the first byte of a reply frame.
type Status uint8

const (
	StatusOK              Status = 0
	StatusConditionFailed Status = 1
	StatusFDBError        Status = 2
	StatusBadRequest      Status = 3
	StatusTooLarge        Status = 4
)

// MaxFrameBytes bounds a payload on both sides; a commit can carry up to 1 MB of mutations.
const MaxFrameBytes = 16 << 20

const frameHeaderBytes = 9 // op/status:1 + correlation:u32 + length:u32

type ConditionKind uint8

const (
	ConditionKeyAbsent         ConditionKind = 1
	ConditionKeyPresent        ConditionKind = 2
	ConditionValueEquals       ConditionKind = 3
	ConditionValuePrefixEquals ConditionKind = 4
)

type MutationKind uint8

const (
	MutationSet                    MutationKind = 1
	MutationClear                  MutationKind = 2
	MutationClearRange             MutationKind = 3
	MutationAtomicAdd              MutationKind = 4
	MutationAtomicMax              MutationKind = 5
	MutationSetVersionstampedKey   MutationKind = 6
	MutationSetVersionstampedValue MutationKind = 7
)

type Range struct {
	Begin     []byte `cb:"1"`
	End       []byte `cb:"2"`
	Limit     uint32 `cb:"3"`
	Reverse   bool   `cb:"4"`
	ByteLimit uint32 `cb:"5"`
}

// RangeResult.NextBegin is the continuation: the next Begin, or the next End when the range was Reverse.
type RangeResult struct {
	Keys      ByteList `cb:"1"`
	Values    ByteList `cb:"2"`
	More      bool     `cb:"3"`
	NextBegin []byte   `cb:"4"`
}

type Condition struct {
	Kind  ConditionKind `cb:"1"`
	Key   []byte        `cb:"2"`
	Value []byte        `cb:"3"`
}

type Mutation struct {
	Kind   MutationKind `cb:"1"`
	Key    []byte       `cb:"2"`
	Value  []byte       `cb:"3"`
	EndKey []byte       `cb:"4"`
}

type GetManyRequest struct {
	Keys     ByteList `cb:"1"`
	Snapshot bool     `cb:"2"`
}

// Values.Absent(i) is true when Keys[i] does not exist, which differs from an empty value.
type GetManyReply struct {
	Values      ByteList `cb:"1"`
	ReadVersion int64    `cb:"2"`
}

type GetRangesRequest struct {
	Ranges []Range `cb:"1"`
}

type GetRangesReply struct {
	Results []RangeResult `cb:"1"`
}

// IndexRangeGetRequest: every index value is a record-key suffix, so the daemon fetches
// RecordPrefixes[i]+value for each index key of Ranges[i] inside the same transaction.
type IndexRangeGetRequest struct {
	Ranges         []Range  `cb:"1"`
	RecordPrefixes ByteList `cb:"2"`
}

type IndexRangeGetReply struct {
	Results []RangeResult `cb:"1"`
	// Records is flattened across Results, aligned with the index keys in order; absent = dangling entry.
	Records ByteList `cb:"2"`
}

type CommitRequest struct {
	Conditions []Condition `cb:"1"`
	Mutations  []Mutation  `cb:"2"`
}

type CommitReply struct {
	CommittedVersion int64 `cb:"1"`
	// FailedCondition is the index into CommitRequest.Conditions; meaningful with StatusConditionFailed.
	FailedCondition uint32 `cb:"2"`
}

type ReserveRangeRequest struct {
	CounterKey []byte `cb:"1"`
	Count      uint32 `cb:"2"`
}

type ReserveRangeReply struct {
	First int64 `cb:"1"`
}

type SetCounterRequest struct {
	CounterKey []byte `cb:"1"`
	Value      int64  `cb:"2"`
}

type SetCounterReply struct {
	Previous int64 `cb:"1"`
}

type ClearPrefixRequest struct {
	Prefix []byte `cb:"1"`
}

type ClearPrefixReply struct {
	Done bool `cb:"1"`
}

type PingReply struct {
	ReadVersion int64 `cb:"1"`
}

// ErrorBody accompanies every non-OK status.
type ErrorBody struct {
	Code    int32  `cb:"1"`
	Message string `cb:"2"`
}

// Error is what a non-OK reply becomes on the client.
type Error struct {
	Status          Status
	Code            int32
	Message         string
	FailedCondition uint32
}

func (frameError *Error) Error() string {
	return fmt.Sprintf("fdbgate status %d (code %d): %s", frameError.Status, frameError.Code, frameError.Message)
}

// WriteFrame writes one frame; lead is the op (request) or the status (reply).
func WriteFrame(writer io.Writer, lead uint8, correlation uint32, payload []byte) error {
	if len(payload) > MaxFrameBytes {
		return fmt.Errorf("fdb frame payload %d bytes exceeds max %d", len(payload), MaxFrameBytes)
	}
	frame := make([]byte, frameHeaderBytes+len(payload))
	frame[0] = lead
	binary.BigEndian.PutUint32(frame[1:5], correlation)
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(payload)))
	copy(frame[frameHeaderBytes:], payload)
	// One Write call so concurrent writers holding a mutex never interleave half frames.
	_, writeError := writer.Write(frame)
	return writeError
}

// ReadFrame reads one frame and enforces MaxFrameBytes before allocating the payload.
func ReadFrame(reader io.Reader) (lead uint8, correlation uint32, payload []byte, readError error) {
	var header [frameHeaderBytes]byte
	if _, readError = io.ReadFull(reader, header[:]); readError != nil {
		return
	}
	lead = header[0]
	correlation = binary.BigEndian.Uint32(header[1:5])
	payloadLength := binary.BigEndian.Uint32(header[5:9])
	if payloadLength > MaxFrameBytes {
		readError = fmt.Errorf("fdb frame payload %d bytes exceeds max %d", payloadLength, MaxFrameBytes)
		return
	}
	payload = make([]byte, payloadLength)
	_, readError = io.ReadFull(reader, payload)
	return
}

func Encode(message any) ([]byte, error) { return colbin.Marshal(message) }

func Decode(payload []byte, destination any) error { return colbin.Unmarshal(payload, destination) }

// ByteList is a list of byte strings in colbin-friendly form: colbin has no [][]byte, and one flat
// blob decodes with a single allocation. Lengths[i] == -1 marks an absent entry (not an empty one).
type ByteList struct {
	Lengths []int32 `cb:"1"`
	Data    []byte  `cb:"2"`
}

func NewByteList(entries [][]byte) ByteList {
	var list ByteList
	for _, entry := range entries {
		list.Append(entry)
	}
	return list
}

func (list *ByteList) Append(entry []byte) {
	list.Lengths = append(list.Lengths, int32(len(entry)))
	list.Data = append(list.Data, entry...)
}

func (list *ByteList) AppendAbsent() { list.Lengths = append(list.Lengths, -1) }

func (list *ByteList) Len() int { return len(list.Lengths) }

func (list *ByteList) Absent(index int) bool { return list.Lengths[index] < 0 }

// Slices expands the list to [][]byte as sub-slices of Data (no copy); absent entries are nil.
func (list *ByteList) Slices() [][]byte {
	slices := make([][]byte, len(list.Lengths))
	var offset int
	for index, length := range list.Lengths {
		if length < 0 {
			continue
		}
		slices[index] = list.Data[offset : offset+int(length) : offset+int(length)]
		offset += int(length)
	}
	return slices
}
