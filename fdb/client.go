package fdb

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

const DefaultPoolSize = 4

// Client multiplexes requests over a small pool of unix-socket connections to fdbgate.
// A broken connection fails its pending requests and is re-dialed on the next use.
type Client struct {
	socketPath  string
	connections []*clientConnection
	nextPick    atomic.Uint32
}

type clientConnection struct {
	client       *Client
	mutex        sync.Mutex // guards conn, pending and writes
	conn         net.Conn
	pending      map[uint32]chan frameReply
	nextCorrelID uint32
}

type frameReply struct {
	status  Status
	payload []byte
	err     error
}

func Dial(socketPath string, poolSize int) (*Client, error) {
	if poolSize <= 0 {
		poolSize = DefaultPoolSize
	}
	client := &Client{socketPath: socketPath}
	for range poolSize {
		client.connections = append(client.connections, &clientConnection{client: client})
	}
	// Fail fast on a wrong socket path instead of on the first query.
	if _, pingError := client.Ping(context.Background()); pingError != nil {
		return nil, fmt.Errorf("fdbgate at %s: %w", socketPath, pingError)
	}
	return client, nil
}

func (client *Client) Close() {
	for _, connection := range client.connections {
		connection.mutex.Lock()
		if connection.conn != nil {
			connection.conn.Close()
		}
		connection.mutex.Unlock()
	}
}

// Call sends one request and decodes the OK reply into reply (nil to ignore the body).
// A non-OK status is returned with its raw body so the caller decides how to interpret it.
func (client *Client) Call(ctx context.Context, op Op, request any, reply any) (Status, []byte, error) {
	var payload []byte
	if request != nil {
		var encodeError error
		if payload, encodeError = Encode(request); encodeError != nil {
			return 0, nil, encodeError
		}
	}
	connection := client.connections[client.nextPick.Add(1)%uint32(len(client.connections))]
	status, body, callError := connection.roundTrip(ctx, op, payload)
	if callError != nil {
		return 0, nil, callError
	}
	if status != StatusOK {
		return status, body, nil
	}
	if reply != nil {
		if decodeError := Decode(body, reply); decodeError != nil {
			return status, body, decodeError
		}
	}
	return status, body, nil
}

// callOK is Call for ops where any non-OK status is an error.
func (client *Client) callOK(ctx context.Context, op Op, request any, reply any) error {
	status, body, callError := client.Call(ctx, op, request, reply)
	if callError != nil {
		return callError
	}
	if status != StatusOK {
		return decodeStatusError(status, body)
	}
	return nil
}

func decodeStatusError(status Status, body []byte) error {
	frameError := &Error{Status: status}
	var errorBody ErrorBody
	if len(body) > 0 && Decode(body, &errorBody) == nil {
		frameError.Code, frameError.Message = errorBody.Code, errorBody.Message
	}
	return frameError
}

func (connection *clientConnection) roundTrip(ctx context.Context, op Op, payload []byte) (Status, []byte, error) {
	connection.mutex.Lock()
	if connection.conn == nil {
		conn, dialError := net.Dial("unix", connection.client.socketPath)
		if dialError != nil {
			connection.mutex.Unlock()
			return 0, nil, dialError
		}
		connection.conn = conn
		connection.pending = map[uint32]chan frameReply{}
		go connection.readLoop(conn)
	}
	connection.nextCorrelID++
	correlation := connection.nextCorrelID
	replyChannel := make(chan frameReply, 1)
	connection.pending[correlation] = replyChannel
	writeError := WriteFrame(connection.conn, uint8(op), correlation, payload)
	if writeError != nil {
		delete(connection.pending, correlation)
		connection.conn.Close() // readLoop sees the close and fails the other pending requests
	}
	connection.mutex.Unlock()
	if writeError != nil {
		return 0, nil, writeError
	}

	select {
	case reply := <-replyChannel:
		return reply.status, reply.payload, reply.err
	case <-ctx.Done():
		connection.mutex.Lock()
		delete(connection.pending, correlation)
		connection.mutex.Unlock()
		return 0, nil, ctx.Err()
	}
}

func (connection *clientConnection) readLoop(conn net.Conn) {
	for {
		lead, correlation, payload, readError := ReadFrame(conn)
		if readError != nil {
			connection.mutex.Lock()
			// Only tear down state if this loop still owns the connection.
			if connection.conn == conn {
				for _, waiting := range connection.pending {
					waiting <- frameReply{err: fmt.Errorf("fdbgate connection lost: %w", readError)}
				}
				connection.pending = nil
				connection.conn = nil
			}
			connection.mutex.Unlock()
			conn.Close()
			return
		}
		connection.mutex.Lock()
		waiting, found := connection.pending[correlation]
		delete(connection.pending, correlation)
		connection.mutex.Unlock()
		if found {
			waiting <- frameReply{status: Status(lead), payload: payload}
		}
	}
}

func (client *Client) Ping(ctx context.Context) (int64, error) {
	var reply PingReply
	return reply.ReadVersion, client.callOK(ctx, OpPing, nil, &reply)
}

// GetMany returns the values in key order; a missing key has present[i] == false.
func (client *Client) GetMany(ctx context.Context, keys [][]byte, snapshot bool) (values [][]byte, present []bool, err error) {
	var reply GetManyReply
	if err = client.callOK(ctx, OpGetMany, &GetManyRequest{Keys: NewByteList(keys), Snapshot: snapshot}, &reply); err != nil {
		return nil, nil, err
	}
	present = make([]bool, reply.Values.Len())
	for valueIndex := range present {
		present[valueIndex] = !reply.Values.Absent(valueIndex)
	}
	return reply.Values.Slices(), present, nil
}

func (client *Client) GetRanges(ctx context.Context, ranges []Range) ([]RangeResult, error) {
	var reply GetRangesReply
	if err := client.callOK(ctx, OpGetRanges, &GetRangesRequest{Ranges: ranges}, &reply); err != nil {
		return nil, err
	}
	return reply.Results, nil
}

// IndexRangeGet reads index ranges and, in the same transaction, the record each index entry points
// to: recordPrefixes[i] + entry value. The records are flattened across ranges in entry order; a nil
// record is a dangling entry.
func (client *Client) IndexRangeGet(ctx context.Context, ranges []Range, recordPrefixes [][]byte) ([]RangeResult, [][]byte, error) {
	var reply IndexRangeGetReply
	request := &IndexRangeGetRequest{Ranges: ranges, RecordPrefixes: NewByteList(recordPrefixes)}
	if err := client.callOK(ctx, OpIndexRangeGet, request, &reply); err != nil {
		return nil, nil, err
	}
	return reply.Results, reply.Records.Slices(), nil
}

// ReserveRange advances a counter by count and returns the first value of the reserved range.
func (client *Client) ReserveRange(ctx context.Context, counterKey []byte, count uint32) (int64, error) {
	var reply ReserveRangeReply
	return reply.First, client.callOK(ctx, OpReserveRange, &ReserveRangeRequest{CounterKey: counterKey, Count: count}, &reply)
}

// SetCounter moves a counter to value and returns what it held.
func (client *Client) SetCounter(ctx context.Context, counterKey []byte, value int64) (int64, error) {
	var reply SetCounterReply
	return reply.Previous, client.callOK(ctx, OpSetCounter, &SetCounterRequest{CounterKey: counterKey, Value: value}, &reply)
}

// ClearPrefix deletes every key under prefix, in as many transactions as it takes.
func (client *Client) ClearPrefix(ctx context.Context, prefix []byte) error {
	for {
		var reply ClearPrefixReply
		if err := client.callOK(ctx, OpClearPrefix, &ClearPrefixRequest{Prefix: prefix}, &reply); err != nil {
			return err
		}
		if reply.Done {
			return nil
		}
	}
}

// ErrConditionFailed is returned by Commit when a declared condition does not hold.
type ErrConditionFailed struct{ Index uint32 }

func (conditionError *ErrConditionFailed) Error() string {
	return fmt.Sprintf("commit condition %d failed", conditionError.Index)
}

func (client *Client) Commit(ctx context.Context, conditions []Condition, mutations []Mutation) (committedVersion int64, err error) {
	var reply CommitReply
	status, body, callError := client.Call(ctx, OpCommit, &CommitRequest{Conditions: conditions, Mutations: mutations}, &reply)
	if callError != nil {
		return 0, callError
	}
	switch status {
	case StatusOK:
		return reply.CommittedVersion, nil
	case StatusConditionFailed:
		var failed CommitReply
		if decodeError := Decode(body, &failed); decodeError != nil {
			return 0, decodeError
		}
		return 0, &ErrConditionFailed{Index: failed.FailedCondition}
	default:
		return 0, decodeStatusError(status, body)
	}
}
