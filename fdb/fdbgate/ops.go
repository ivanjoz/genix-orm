package main

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	fdbclient "github.com/ivanjoz/genix-orm/fdb"
)

// FDB error codes that map to a dedicated reply status (see fdb_c error codes).
const (
	fdbErrorTransactionTimedOut = 1031
	fdbErrorTransactionTooLarge = 2101
	fdbErrorKeyTooLarge         = 2102
	fdbErrorValueTooLarge       = 2103
)

const clearPrefixChunkKeys = 10_000

// conditionFailedError aborts the Transact retry loop: the binding only retries fdb.Error values.
type conditionFailedError struct{ index uint32 }

func (conditionError *conditionFailedError) Error() string {
	return fmt.Sprintf("condition %d failed", conditionError.index)
}

// handleOp decodes the request for op, runs it in one transaction and returns the encoded reply body.
func (gate *gate) handleOp(op fdbclient.Op, payload []byte) (fdbclient.Status, []byte) {
	reply, opError := gate.runOp(op, payload)
	if opError == nil {
		body, encodeError := fdbclient.Encode(reply)
		if encodeError != nil {
			return errorReply(fdbclient.StatusFDBError, 0, encodeError.Error())
		}
		return fdbclient.StatusOK, body
	}

	var conditionError *conditionFailedError
	if errors.As(opError, &conditionError) {
		body, _ := fdbclient.Encode(&fdbclient.CommitReply{FailedCondition: conditionError.index})
		return fdbclient.StatusConditionFailed, body
	}
	var badRequest badRequestError
	if errors.As(opError, &badRequest) {
		return errorReply(fdbclient.StatusBadRequest, 0, badRequest.Error())
	}
	var fdbError fdb.Error
	if errors.As(opError, &fdbError) {
		switch fdbError.Code {
		case fdbErrorTransactionTimedOut, fdbErrorTransactionTooLarge, fdbErrorKeyTooLarge, fdbErrorValueTooLarge:
			return errorReply(fdbclient.StatusTooLarge, int32(fdbError.Code), fdbError.Error())
		}
		return errorReply(fdbclient.StatusFDBError, int32(fdbError.Code), fdbError.Error())
	}
	return errorReply(fdbclient.StatusFDBError, 0, opError.Error())
}

type badRequestError string

func (badRequest badRequestError) Error() string { return string(badRequest) }

func errorReply(status fdbclient.Status, code int32, message string) (fdbclient.Status, []byte) {
	body, _ := fdbclient.Encode(&fdbclient.ErrorBody{Code: code, Message: message})
	return status, body
}

func (gate *gate) runOp(op fdbclient.Op, payload []byte) (any, error) {
	switch op {
	case fdbclient.OpPing:
		return gate.ping()
	case fdbclient.OpGetMany:
		var request fdbclient.GetManyRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.getMany(&request)
	case fdbclient.OpGetRanges:
		var request fdbclient.GetRangesRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.getRanges(&request)
	case fdbclient.OpIndexRangeGet:
		var request fdbclient.IndexRangeGetRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.indexRangeGet(&request)
	case fdbclient.OpCommit:
		var request fdbclient.CommitRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.commit(&request)
	case fdbclient.OpReserveRange:
		var request fdbclient.ReserveRangeRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.reserveRange(&request)
	case fdbclient.OpSetCounter:
		var request fdbclient.SetCounterRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.setCounter(&request)
	case fdbclient.OpClearPrefix:
		var request fdbclient.ClearPrefixRequest
		if decodeError := fdbclient.Decode(payload, &request); decodeError != nil {
			return nil, badRequestError(decodeError.Error())
		}
		return gate.clearPrefix(&request)
	}
	return nil, badRequestError(fmt.Sprintf("unknown op 0x%02x", uint8(op)))
}

func (gate *gate) ping() (*fdbclient.PingReply, error) {
	result, transactError := gate.database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
		return readTransaction.GetReadVersion().Get()
	})
	if transactError != nil {
		return nil, transactError
	}
	return &fdbclient.PingReply{ReadVersion: result.(int64)}, nil
}

func (gate *gate) getMany(request *fdbclient.GetManyRequest) (*fdbclient.GetManyReply, error) {
	result, transactError := gate.database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
		var source fdb.ReadTransaction = readTransaction
		if request.Snapshot {
			source = readTransaction.Snapshot()
		}
		reply := &fdbclient.GetManyReply{}
		// Issue every Get before awaiting any so the reads pipeline in one round trip.
		keys := request.Keys.Slices()
		futures := make([]fdb.FutureByteSlice, len(keys))
		for keyIndex, key := range keys {
			futures[keyIndex] = source.Get(fdb.Key(key))
		}
		for _, future := range futures {
			value, getError := future.Get()
			if getError != nil {
				return nil, getError
			}
			if value == nil {
				reply.Values.AppendAbsent()
			} else {
				reply.Values.Append(value)
			}
		}
		readVersion, versionError := readTransaction.GetReadVersion().Get()
		reply.ReadVersion = readVersion
		return reply, versionError
	})
	if transactError != nil {
		return nil, transactError
	}
	return result.(*fdbclient.GetManyReply), nil
}

// readRange reads one Range honoring Limit and ByteLimit; More/NextBegin let the client continue.
func readRange(readTransaction fdb.ReadTransaction, requested fdbclient.Range) (fdbclient.RangeResult, error) {
	keyRange := fdb.KeyRange{Begin: fdb.Key(requested.Begin), End: fdb.Key(requested.End)}
	// Ask for one row beyond Limit: the binding's own limit ends the iterator, so the extra row is
	// the only way to learn that the range continues.
	fetchLimit := int(requested.Limit)
	if fetchLimit > 0 {
		fetchLimit++
	}
	iterator := readTransaction.GetRange(keyRange, fdb.RangeOptions{
		Limit: fetchLimit, Reverse: requested.Reverse, Mode: fdb.StreamingModeWantAll,
	}).Iterator()

	var result fdbclient.RangeResult
	var collectedBytes uint32
	var lastKey []byte
	for iterator.Advance() {
		if requested.Limit > 0 && uint32(result.Keys.Len()) >= requested.Limit {
			result.More = true // the extra row
			break
		}
		pair, getError := iterator.Get()
		if getError != nil {
			return result, getError
		}
		result.Keys.Append(pair.Key)
		result.Values.Append(pair.Value)
		lastKey = pair.Key
		collectedBytes += uint32(len(pair.Key) + len(pair.Value))
		if requested.ByteLimit > 0 && collectedBytes >= requested.ByteLimit {
			result.More = iterator.Advance() // more only if the range really has a further key
			break
		}
	}
	if result.More {
		if requested.Reverse {
			result.NextBegin = append([]byte(nil), lastKey...) // becomes the next exclusive End
		} else {
			result.NextBegin = append(append([]byte(nil), lastKey...), 0x00) // first key after lastKey
		}
	}
	return result, nil
}

func (gate *gate) getRanges(request *fdbclient.GetRangesRequest) (*fdbclient.GetRangesReply, error) {
	result, transactError := gate.database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
		reply := &fdbclient.GetRangesReply{Results: make([]fdbclient.RangeResult, 0, len(request.Ranges))}
		for _, requested := range request.Ranges {
			rangeResult, readError := readRange(readTransaction, requested)
			if readError != nil {
				return nil, readError
			}
			reply.Results = append(reply.Results, rangeResult)
		}
		return reply, nil
	})
	if transactError != nil {
		return nil, transactError
	}
	return result.(*fdbclient.GetRangesReply), nil
}

// indexRangeGet reads index ranges, then the record each index entry points at, in one transaction.
// An index entry's value is the record-key suffix, so no tuple parsing happens here.
func (gate *gate) indexRangeGet(request *fdbclient.IndexRangeGetRequest) (*fdbclient.IndexRangeGetReply, error) {
	if request.RecordPrefixes.Len() != len(request.Ranges) {
		return nil, badRequestError("RecordPrefixes must have one entry per range")
	}
	recordPrefixes := request.RecordPrefixes.Slices()
	result, transactError := gate.database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
		reply := &fdbclient.IndexRangeGetReply{}
		var recordFutures []fdb.FutureByteSlice
		for rangeIndex, requested := range request.Ranges {
			rangeResult, readError := readRange(readTransaction, requested)
			if readError != nil {
				return nil, readError
			}
			for _, recordSuffix := range rangeResult.Values.Slices() {
				recordKey := append(append([]byte(nil), recordPrefixes[rangeIndex]...), recordSuffix...)
				recordFutures = append(recordFutures, readTransaction.Get(fdb.Key(recordKey)))
			}
			reply.Results = append(reply.Results, rangeResult)
		}
		for _, future := range recordFutures {
			recordValue, getError := future.Get()
			if getError != nil {
				return nil, getError
			}
			if recordValue == nil {
				reply.Records.AppendAbsent()
			} else {
				reply.Records.Append(recordValue)
			}
		}
		return reply, nil
	})
	if transactError != nil {
		return nil, transactError
	}
	return result.(*fdbclient.IndexRangeGetReply), nil
}

// commit checks conditions with reads inside the transaction (so they add read conflicts and are
// re-checked on every retry), then applies the mutations.
func (gate *gate) commit(request *fdbclient.CommitRequest) (*fdbclient.CommitReply, error) {
	var versionstampFuture fdb.FutureKey
	_, transactError := gate.database.Transact(func(transaction fdb.Transaction) (any, error) {
		for conditionIndex, condition := range request.Conditions {
			storedValue, getError := transaction.Get(fdb.Key(condition.Key)).Get()
			if getError != nil {
				return nil, getError
			}
			holds := false
			switch condition.Kind {
			case fdbclient.ConditionKeyAbsent:
				holds = storedValue == nil
			case fdbclient.ConditionKeyPresent:
				holds = storedValue != nil
			case fdbclient.ConditionValueEquals:
				holds = storedValue != nil && string(storedValue) == string(condition.Value)
			case fdbclient.ConditionValuePrefixEquals:
				holds = len(storedValue) >= len(condition.Value) && string(storedValue[:len(condition.Value)]) == string(condition.Value)
			default:
				return nil, badRequestError(fmt.Sprintf("unknown condition kind %d", condition.Kind))
			}
			if !holds {
				return nil, &conditionFailedError{index: uint32(conditionIndex)}
			}
		}
		for _, mutation := range request.Mutations {
			switch mutation.Kind {
			case fdbclient.MutationSet:
				transaction.Set(fdb.Key(mutation.Key), mutation.Value)
			case fdbclient.MutationClear:
				transaction.Clear(fdb.Key(mutation.Key))
			case fdbclient.MutationClearRange:
				transaction.ClearRange(fdb.KeyRange{Begin: fdb.Key(mutation.Key), End: fdb.Key(mutation.EndKey)})
			case fdbclient.MutationAtomicAdd:
				transaction.Add(fdb.Key(mutation.Key), mutation.Value)
			case fdbclient.MutationAtomicMax:
				transaction.Max(fdb.Key(mutation.Key), mutation.Value)
			case fdbclient.MutationSetVersionstampedKey:
				transaction.SetVersionstampedKey(fdb.Key(mutation.Key), mutation.Value)
			case fdbclient.MutationSetVersionstampedValue:
				transaction.SetVersionstampedValue(fdb.Key(mutation.Key), mutation.Value)
			default:
				return nil, badRequestError(fmt.Sprintf("unknown mutation kind %d", mutation.Kind))
			}
		}
		versionstampFuture = transaction.GetVersionstamp()
		return nil, nil
	})
	if transactError != nil {
		return nil, transactError
	}
	// The versionstamp is 8 bytes of commit version (big-endian) + 2 bytes of batch order.
	// A read-only commit has no versionstamp; its version stays 0.
	var committedVersion int64
	if versionstamp, versionError := versionstampFuture.Get(); versionError == nil && len(versionstamp) >= 8 {
		committedVersion = int64(binary.BigEndian.Uint64(versionstamp[:8]))
	}
	return &fdbclient.CommitReply{CommittedVersion: committedVersion}, nil
}

// reserveRange atomically takes count IDs: it returns the first one of the reserved block.
func (gate *gate) reserveRange(request *fdbclient.ReserveRangeRequest) (*fdbclient.ReserveRangeReply, error) {
	result, transactError := gate.database.Transact(func(transaction fdb.Transaction) (any, error) {
		storedValue, getError := transaction.Get(fdb.Key(request.CounterKey)).Get()
		if getError != nil {
			return nil, getError
		}
		currentValue := decodeCounter(storedValue)
		transaction.Set(fdb.Key(request.CounterKey), encodeCounter(currentValue+int64(request.Count)))
		return currentValue + 1, nil
	})
	if transactError != nil {
		return nil, transactError
	}
	return &fdbclient.ReserveRangeReply{First: result.(int64)}, nil
}

func (gate *gate) setCounter(request *fdbclient.SetCounterRequest) (*fdbclient.SetCounterReply, error) {
	result, transactError := gate.database.Transact(func(transaction fdb.Transaction) (any, error) {
		storedValue, getError := transaction.Get(fdb.Key(request.CounterKey)).Get()
		if getError != nil {
			return nil, getError
		}
		transaction.Set(fdb.Key(request.CounterKey), encodeCounter(request.Value))
		return decodeCounter(storedValue), nil
	})
	if transactError != nil {
		return nil, transactError
	}
	return &fdbclient.SetCounterReply{Previous: result.(int64)}, nil
}

// clearPrefix clears at most clearPrefixChunkKeys keys per call so one transaction never grows
// past the size limit; the client repeats until Done.
func (gate *gate) clearPrefix(request *fdbclient.ClearPrefixRequest) (*fdbclient.ClearPrefixReply, error) {
	result, transactError := gate.database.Transact(func(transaction fdb.Transaction) (any, error) {
		prefixRange, rangeError := fdb.PrefixRange(request.Prefix)
		if rangeError != nil {
			return nil, badRequestError(rangeError.Error())
		}
		beginKey, endKey := prefixRange.FDBRangeKeys()
		keys, getError := transaction.GetRange(prefixRange, fdb.RangeOptions{Limit: clearPrefixChunkKeys + 1}).GetSliceWithError()
		if getError != nil {
			return nil, getError
		}
		if len(keys) <= clearPrefixChunkKeys {
			transaction.ClearRange(fdb.KeyRange{Begin: beginKey, End: endKey})
			return true, nil
		}
		boundary := append(append([]byte(nil), keys[clearPrefixChunkKeys-1].Key...), 0x00)
		transaction.ClearRange(fdb.KeyRange{Begin: beginKey, End: fdb.Key(boundary)})
		return false, nil
	})
	if transactError != nil {
		return nil, transactError
	}
	return &fdbclient.ClearPrefixReply{Done: result.(bool)}, nil
}

// Counters are int64 little-endian so the same key also works with AtomicAdd mutations.
func decodeCounter(stored []byte) int64 {
	var counterValue int64
	for byteIndex := min(len(stored), 8) - 1; byteIndex >= 0; byteIndex-- {
		counterValue = counterValue<<8 | int64(stored[byteIndex])
	}
	return counterValue
}

func encodeCounter(counterValue int64) []byte {
	encoded := make([]byte, 8)
	for byteIndex := range encoded {
		encoded[byteIndex] = byte(counterValue >> (8 * byteIndex))
	}
	return encoded
}
