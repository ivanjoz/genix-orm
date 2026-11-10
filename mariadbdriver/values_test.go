package mariadbdriver

import (
	"reflect"
	"testing"

	"github.com/ivanjoz/colbin"
)

type storeID int32

// Every fixed-width number slice round-trips through colbin.EncodeSlice, named types included.
func TestNumberSliceRoundTrip(t *testing.T) {
	cases := []any{
		[]int8{-3, 0, 7},
		[]int16{-5, 300, 32000},
		[]int32{500_000, 500_003, 500_006},
		[]int64{1 << 40, -1, 0},
		[]uint16{0, 65535},
		[]uint32{0, 4_000_000_000},
		[]float32{12.5, 9.99, 0.1},
		[]float64{1.25, 1.0 / 3, -0.5},
		[]storeID{3, 4, 5},
	}
	// The bytes are colbin.EncodeSlice's, not a colbin message.
	if encoded, _ := encodeNumberSlice(reflect.ValueOf([]storeID{3, 4, 5})); string(encoded) != string(colbin.EncodeSlice([]int32{3, 4, 5})) {
		t.Fatal("[]storeID: bytes differ from colbin.EncodeSlice")
	}
	for _, original := range cases {
		encoded, isNumberSlice := encodeNumberSlice(reflect.ValueOf(original))
		if !isNumberSlice {
			t.Fatalf("%T: not encoded as a number slice", original)
		}
		decoded := reflect.New(reflect.TypeOf(original)).Elem()
		if isNumberSlice, err := decodeNumberSlice(encoded, decoded); !isNumberSlice || err != nil {
			t.Fatalf("%T: decode handled=%v err=%v", original, isNumberSlice, err)
		}
		if !reflect.DeepEqual(decoded.Interface(), original) {
			t.Fatalf("%T: got %v, want %v", original, decoded.Interface(), original)
		}
	}
}

// Slices EncodeSlice does not take stay colbin messages.
func TestNumberSliceLeavesOtherSlices(t *testing.T) {
	for _, other := range []any{[]string{"a"}, []int{1}, []uint64{1}, []struct{ A int32 }{{1}}} {
		if _, isNumberSlice := encodeNumberSlice(reflect.ValueOf(other)); isNumberSlice {
			t.Fatalf("%T: encoded as a number slice", other)
		}
		if isNumberSlice, _ := decodeNumberSlice(nil, reflect.New(reflect.TypeOf(other)).Elem()); isNumberSlice {
			t.Fatalf("%T: decoded as a number slice", other)
		}
	}
}
