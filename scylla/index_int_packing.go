package scylla

import ()

// storeVirtualPacked turns an unsigned layout into the value bound to the CQL int/bigint column.
// Scylla compares those as signed, so the top bit is flipped: that maps unsigned order onto signed
// order, and range scans over the stored value follow the layout's order. Every write and every
// query bound on a virtual packed column goes through here.
func storeVirtualPacked(packed uint64, isInt32 bool) any {
	if isInt32 {
		return int32(uint32(packed) ^ (1 << 31))
	}
	return int64(packed ^ (1 << 63))
}

// loadVirtualPacked is the inverse of storeVirtualPacked for a value scanned back from Scylla.
func loadVirtualPacked(stored int64, isInt32 bool) uint64 {
	if isInt32 {
		return uint64(uint32(int32(stored)) ^ (1 << 31))
	}
	return uint64(stored) ^ (1 << 63)
}
