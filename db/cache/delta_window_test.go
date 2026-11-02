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
			IsReplace: vector.IsReplace, NextCursor: vector.NextCursor,
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
	cases := map[string]int32{"": 0, "400000000": 400000000, "-5": 0, "12.40": 0, "abc": 0, "99999999999": 0}
	for encodedCursor, want := range cases {
		if got := ParseDeltaWindowCursor(encodedCursor); got != want {
			t.Errorf("ParseDeltaWindowCursor(%q) = %d, want %d", encodedCursor, got, want)
		}
	}
}
