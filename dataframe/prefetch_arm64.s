//go:build !purego

#include "textflag.h"

// func prefetch(address unsafe.Pointer)
TEXT ·prefetch(SB), NOSPLIT, $0-8
	MOVD address+0(FP), R0
	PRFM (R0), PLDL1KEEP
	RET
