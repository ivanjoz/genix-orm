//go:build !(amd64 || arm64) || purego

package dataframe

import "unsafe"

func prefetch(address unsafe.Pointer) {}
