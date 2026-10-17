package cache

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/utils"
)

// deltaLogVectors is testdata/delta_log_vectors.json, which genix-ui's delta-log wire test reads too:
// the two implementations must produce and read the same bytes.
type deltaLogVectors struct {
	Entries []struct {
		Name         string `json:"name"`
		Is64BitScale bool   `json:"is64BitScale"`
		Entries      []struct {
			LogID     uint64 `json:"logID"`
			Version   uint16 `json:"version"`
			IsDeleted bool   `json:"isDeleted"`
		} `json:"entries"`
		Hex string `json:"hex"`
	} `json:"entries"`
	WireVersions [][2]int `json:"wireVersions"`
	LogIDs       []struct {
		Values     []uint64 `json:"values"`
		ColumnBits []int8   `json:"columnBits"`
		LogID      uint64   `json:"logID"`
	} `json:"logIDs"`
	Metadata []struct {
		Flags      byte   `json:"flags"`
		Bucket     int64  `json:"bucket"`
		ColumnBits []int8 `json:"columnBits"`
		FileHex    string `json:"fileHex"`
		Safe85     string `json:"safe85"`
	} `json:"metadata"`
}

func readDeltaLogVectors(t *testing.T) deltaLogVectors {
	t.Helper()
	content, err := os.ReadFile("testdata/delta_log_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors deltaLogVectors
	if err := json.Unmarshal(content, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestDeltaLogEntriesMatchTheSharedVectors(t *testing.T) {
	for _, vector := range readDeltaLogVectors(t).Entries {
		entries := make([]DeltaLogEntry, len(vector.Entries))
		for i, entry := range vector.Entries {
			entries[i] = DeltaLogEntry{LogID: entry.LogID, Version: entry.Version, IsDeleted: entry.IsDeleted}
		}
		encoded := hex.EncodeToString(EncodeDeltaLogEntries(entries, vector.Is64BitScale))
		if encoded != vector.Hex {
			t.Errorf("%s: encodes to %s, want %s", vector.Name, encoded, vector.Hex)
			continue
		}
		content, _ := hex.DecodeString(vector.Hex)
		decoded, err := DecodeDeltaLogEntries(content, vector.Is64BitScale)
		if err != nil {
			t.Errorf("%s: %v", vector.Name, err)
			continue
		}
		// A group comes back sorted by log ID: compare as sets.
		sortEntries := func(entries []DeltaLogEntry) {
			slices.SortFunc(entries, func(a, b DeltaLogEntry) int { return int(a.LogID) - int(b.LogID) })
		}
		sortEntries(entries)
		sortEntries(decoded)
		if !slices.Equal(decoded, entries) {
			t.Errorf("%s: decodes to %+v, want %+v", vector.Name, decoded, entries)
		}
	}
}

func TestDeltaLogWireVersionsLogIDsAndMetadataMatchTheSharedVectors(t *testing.T) {
	vectors := readDeltaLogVectors(t)
	for _, versionPair := range vectors.WireVersions {
		if got := WireVersion(int16(versionPair[0])); int(got) != versionPair[1] {
			t.Errorf("WireVersion(%d) = %d, want %d", versionPair[0], got, versionPair[1])
		}
	}
	for _, vector := range vectors.LogIDs {
		if got := PackDeltaLogID(vector.Values, vector.ColumnBits); got != vector.LogID {
			t.Errorf("PackDeltaLogID(%v, %v) = %d, want %d", vector.Values, vector.ColumnBits, got, vector.LogID)
		}
		if got := UnpackDeltaLogID(vector.LogID, vector.ColumnBits); !slices.Equal(got, vector.Values) {
			t.Errorf("UnpackDeltaLogID(%d, %v) = %v, want %v", vector.LogID, vector.ColumnBits, got, vector.Values)
		}
	}
	for _, vector := range vectors.Metadata {
		fileContent, _ := hex.DecodeString(vector.FileHex)
		if got := utils.EncodeSafe85(DeltaMetadataBytes(vector.Flags, vector.Bucket, vector.ColumnBits, fileContent)); got != vector.Safe85 {
			t.Errorf("metadata %+v encodes to %q, want %q", vector, got, vector.Safe85)
		}
	}
}

func TestDecodeDeltaLogEntriesRejectsDamage(t *testing.T) {
	for _, damagedHex := range []string{
		"009a40",   // a 4-byte entry cut at 3
		"009a4000", // version 0
		"c00100",   // a group of 0
		"c0010305", // a group of 3 holding 1 ID
	} {
		content, _ := hex.DecodeString(damagedHex)
		if entries, err := DecodeDeltaLogEntries(content, false); err == nil {
			t.Errorf("%s decoded to %+v", damagedHex, entries)
		}
	}
}
