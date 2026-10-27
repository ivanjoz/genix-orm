//go:build !purego

#include "textflag.h"

// func prefetch(address unsafe.Pointer)
TEXT ·prefetch(SB), NOSPLIT, $0-8
	MOVQ address+0(FP), AX
	PREFETCHT0 (AX)
	RET
