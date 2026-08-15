package db

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// HistoricalUnixEnvVar freezes the process clock at a given unix second so seed and sample
// generators can write records dated in the past through the normal write path.
//
// This package is the single source of truth for "now" across the whole backend: it is the
// lowest layer both the application (which reads it through its own db + core wrappers) and
// the storage drivers can import, so the ORM's managed created/updated columns and the
// application's business dates never disagree about which day it is.
//
// Unset or zero means the real clock. Any other value is taken literally — including one in
// the future, which is what makes it usable for expiry testing as well.
const HistoricalUnixEnvVar = "GENIX_HISTORICAL_UNIX"

// Atomic because generators advance the simulated clock between writes while the ORM reads it
// from whatever goroutine happens to be flushing a batch.
var historicalUnixSeconds atomic.Int64

func init() {
	rawValue := os.Getenv(HistoricalUnixEnvVar)
	if rawValue == "" {
		return
	}
	parsedUnixSeconds, parseError := strconv.ParseInt(rawValue, 10, 64)
	if parseError != nil || parsedUnixSeconds < 0 {
		// No logger exists this early and this package must not depend on one; an unusable
		// value is ignored so a stray env var can never stop the process from booting.
		return
	}
	historicalUnixSeconds.Store(parsedUnixSeconds)
}

// SetHistoricalUnix overrides the process clock at runtime. Zero restores the real clock.
// Generators call this once per simulated instant, which is why it is a setter and not a
// boot-time-only constant.
func SetHistoricalUnix(unixSeconds int64) {
	historicalUnixSeconds.Store(unixSeconds)
}

// HistoricalUnix reports the active override, or 0 when the real clock is in use.
func HistoricalUnix() int64 {
	return historicalUnixSeconds.Load()
}

// Now is the effective wall clock. Every persisted date in the project derives from it.
// Code measuring elapsed time or setting a network deadline must keep calling time.Now()
// directly: those need the monotonic clock, which an override would break.
func Now() time.Time {
	if overriddenUnixSeconds := historicalUnixSeconds.Load(); overriddenUnixSeconds > 0 {
		return time.Unix(overriddenUnixSeconds, 0).In(time.Now().Location())
	}
	return time.Now()
}
