//go:build (amd64 || arm64) && !purego

package dataframe

import "unsafe"

// prefetch asks the CPU to bring the cache line at address in ahead of its load: Go has no way to
// say it. A hint, never a fault, whatever address holds.
//
//go:noescape
func prefetch(address unsafe.Pointer)
