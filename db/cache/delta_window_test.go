package cache

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/ivanjoz/genix-orm/utils"
)

// deltaWindowVectors is testdata/delta_window_vectors.json, which genix-ui's delta window test reads
// too: both sides must hash a window and encode its Metadata the same way. The file was computed by an
// independent implementation of the formulas, not by this package.
type deltaWindowVectors struct {
	Records []struct {
		ID      int64  `json:"id"`
		Version int16  `json:"version"`
		Hash    uint32 `json:"hash"`
	} `json:"records"`
	Windows []struct {
		Records [][2]int64 `json:"records"`
		Hash    uint32     `json:"hash"`
	} `json:"windows"`
	Metadata []struct {
		IsReplace    bool    `json:"isReplace"`
		NextCursor   int32   `json:"nextCursor"`
		ReadTime     int32   `json:"readTime"`
		WindowFrom   int32   `json:"windowFrom"`
		WindowTo     int32   `json:"windowTo"`
		WindowHash   uint32  `json:"windowHash"`
		FilterField  string  `json:"filterField"`
		FilterValues []int64 `json:"filterValues"`
		RemovedIDs   []int64 `json:"removedIDs"`
		Hex          string  `json:"hex"`
	} `json:"metadata"`
}

func readDeltaWindowVectors(t *testing.T) deltaWindowVectors {
	t.Helper()
	content, err := os.ReadFile("testdata/delta_window_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors deltaWindowVectors
	if err := json.Unmarshal(content, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestDeltaRecordHashMatchesTheSharedVectors(t *testing.T) {
	for _, vector := range readDeltaWindowVectors(t).Records {
		if hash := DeltaRecordHash(vector.ID, vector.Version); hash != vector.Hash {
			t.Errorf("DeltaRecordHash(%d, %d) = %d, want %d", vector.ID, vector.Version, hash, vector.Hash)
		}
	}
}

// The window hash is a sum mod 2³², so the vectors also pin that record order doesn't matter.
func TestDeltaWindowHashMatchesTheSharedVectors(t *testing.T) {
	for _, vector := range readDeltaWindowVectors(t).Windows {
		windowHash := uint32(0)
		for _, record := range vector.Records {
			windowHash += DeltaRecordHash(record[0], int16(record[1]))
		}
		if windowHash != vector.Hash {
			t.Errorf("window %v hashes to %d, want %d", vector.Records, windowHash, vector.Hash)
		}
	}
}

func TestDeltaWindowMetadataMatchesTheSharedVectors(t *testing.T) {
	for _, vector := range readDeltaWindowVectors(t).Metadata {
		encoded := EncodeDeltaWindowMetadata(DeltaWindowMetadata{
			IsReplace: vector.IsReplace, NextCursor: vector.NextCursor, ReadTime: vector.ReadTime,
			WindowFrom: vector.WindowFrom, WindowTo: vector.WindowTo, WindowHash: vector.WindowHash,
			FilterField: vector.FilterField, FilterValues: vector.FilterValues, RemovedIDs: vector.RemovedIDs,
		})
		content, err := utils.DecodeSafe85(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(content) != vector.Hex {
			t.Errorf("Metadata %+v encodes to %x, want %s", vector, content, vector.Hex)
		}
	}
}

func TestParseDeltaWindowCursor(t *testing.T) {
	cases := map[string]DeltaWindowCursor{
		"":                    {},
		"400000000.400000005": {Updated: 400000000, ReadTime: 400000005},
		"12.40.r":             {Updated: 12, ReadTime: 40, ResendUnsettled: true},
		// A bare C, a third part other than "r", a non-positive or overflowing value: a first sync.
		"400000000":      {},
		"12.40.x":        {},
		"12.40.r.1":      {},
		"-5.40":          {},
		"12.0":           {},
		"abc.40":         {},
		"99999999999.40": {},
	}
	for encodedCursor, want := range cases {
		if got := ParseDeltaWindowCursor(encodedCursor); got != want {
			t.Errorf("ParseDeltaWindowCursor(%q) = %+v, want %+v", encodedCursor, got, want)
		}
	}
}

func TestPlanDeltaSync(t *testing.T) {
	cases := []struct {
		name   string
		cursor DeltaWindowCursor
		want   DeltaSyncPlan
	}{
		{
			// A reload 2 s after a bulk write stamped at C: the write is hashed, not resent.
			name:   "unsettled slice is hashed",
			cursor: DeltaWindowCursor{Updated: 1000, ReadTime: 1001},
			want:   DeltaSyncPlan{ChangedFrom: 1001, MovesFrom: 993, WindowFrom: 993, WindowTo: 1001},
		},
		{
			name:   "mismatched slice is resent",
			cursor: DeltaWindowCursor{Updated: 1000, ReadTime: 1001, ResendUnsettled: true},
			want:   DeltaSyncPlan{ChangedFrom: 993, MovesFrom: 993},
		},
		{
			// V − lag = C: the write at C is still unsettled.
			name:   "slice edge",
			cursor: DeltaWindowCursor{Updated: 1000, ReadTime: 1008},
			want:   DeltaSyncPlan{ChangedFrom: 1001, MovesFrom: 1000, WindowFrom: 1000, WindowTo: 1001},
		},
		{
			name:   "settled cursor reads past C only",
			cursor: DeltaWindowCursor{Updated: 1000, ReadTime: 1009},
			want:   DeltaSyncPlan{ChangedFrom: 1001, MovesFrom: 1001},
		},
		{
			// Settled, the resend flag has no slice left to resend.
			name:   "settled resend",
			cursor: DeltaWindowCursor{Updated: 1000, ReadTime: 5000, ResendUnsettled: true},
			want:   DeltaSyncPlan{ChangedFrom: 1001, MovesFrom: 1001},
		},
	}
	for _, testCase := range cases {
		if got := PlanDeltaSync(testCase.cursor); got != testCase.want {
			t.Errorf("%s: PlanDeltaSync(%+v) = %+v, want %+v", testCase.name, testCase.cursor, got, testCase.want)
		}
	}
}
