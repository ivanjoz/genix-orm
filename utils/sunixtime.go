package utils

import "time"

// SUnixTimeOf is the SUnixTime of moment: genix-orm's datetime unit, (unix − 1e9) / 2, 2-second
// ticks that fit an int32 until 2153. The managed Updated, the record's created moment, the
// changelog deltas and the DataFrame snapshots are all SUnixTimes.
func SUnixTimeOf(moment time.Time) int64 { return (moment.Unix() - 1e9) / 2 }
