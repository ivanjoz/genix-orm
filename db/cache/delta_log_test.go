package cache

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// deltaNow is a moment in bucket 5000 (SUnixTime 1_500_010).
var deltaNow = SUnixTimeToTime(1_500_010)

func withMemoryFileStore(t *testing.T) *utils.MemoryFileStore {
	t.Helper()
	store := utils.NewMemoryFileStore()
	SetFileStore(store)
	t.Cleanup(func() { SetFileStore(nil) })
	return store
}

func TestDeltaBucketsAndSealing(t *testing.T) {
	if bucket := DeltaBucketOf(SUnixTimeToTime(1_500_299)); bucket != 5000 {
		t.Fatalf("bucket = %d, want 5000", bucket)
	}
	bucketEnd := SUnixTimeToTime(1_500_300)
	if IsDeltaBucketSealed(5000, bucketEnd.Add(DeltaBucketSealDelay-time.Second)) {
		t.Error("a bucket still takes appends until DeltaBucketSealDelay after its end")
	}
	if !IsDeltaBucketSealed(5000, bucketEnd.Add(DeltaBucketSealDelay)) {
		t.Error("a bucket is sealed DeltaBucketSealDelay after its end")
	}
}

func TestDeltaLogFilesAreListedAndReadByBucket(t *testing.T) {
	store := withMemoryFileStore(t)
	folder := DeltaLogFolder + "1/003/"
	for _, fileKey := range []string{folder + "4998", folder + "5000", folder + "4990", DeltaLogFolder + "1/002-003/1/4999"} {
		store.PutFileIfMatch(context.Background(), fileKey, []byte{byte(len(fileKey))}, "")
	}
	buckets, err := ListDeltaLogBuckets(folder, 4995)
	if slices.Sort(buckets); err != nil || !slices.Equal(buckets, []int64{4998, 5000}) {
		t.Fatalf("buckets = %v, err %v; want [4998 5000]", buckets, err)
	}
	contents, err := ReadDeltaLogFiles(folder, []int64{4998, 4999})
	if err != nil || len(contents[0]) != 1 || contents[1] != nil {
		t.Fatalf("contents = %v, err %v; want the 4998 file and an empty 4999", contents, err)
	}
}

func TestDeltaLogAppendsKeepTheLastEntryPerLogID(t *testing.T) {
	store := withMemoryFileStore(t)
	appends := NewDeltaLogAppends(deltaNow)
	appends.Add("delta-log/p1/003/", false, DeltaLogEntry{LogID: 7, Version: 4})
	appends.Add("delta-log/p1/003/", false, DeltaLogEntry{LogID: 8, Version: 1})
	appends.Add("delta-log/p1/003/", false, DeltaLogEntry{LogID: 7, Version: 5, IsDeleted: true})
	if err := appends.Write(); err != nil {
		t.Fatal(err)
	}
	content, _, _ := store.GetFile("delta-log/p1/003/5000")
	entries, err := DecodeDeltaLogEntries(content, false)
	if want := []DeltaLogEntry{{LogID: 7, Version: 5, IsDeleted: true}, {LogID: 8, Version: 1}}; err != nil || !slices.Equal(entries, want) {
		t.Fatalf("entries = %+v, err %v; want %+v", entries, err, want)
	}
}

func TestReadDeltaLogFollowsTheCursor(t *testing.T) {
	store := withMemoryFileStore(t)
	folder := "delta-log/p1/003/"
	write := func(bucket int64, entries ...DeltaLogEntry) {
		appends := NewDeltaLogAppends(SUnixTimeToTime(bucket*DeltaBucketTicks + 1))
		for _, entry := range entries {
			appends.Add(folder, false, entry)
		}
		if err := appends.Write(); err != nil {
			t.Fatal(err)
		}
	}
	write(4999, DeltaLogEntry{LogID: 1, Version: 1})
	write(5000, DeltaLogEntry{LogID: 2, Version: 1})
	previousFile, _, _ := store.GetFile(folder + "4999")
	firstFile, _, _ := store.GetFile(folder + "5000")

	// No cursor: a replace, reading the current file and the previous one while it isn't sealed.
	read, err := ReadDeltaLog(folder, DeltaCursor{}, deltaNow)
	if err != nil || !read.IsReplace || !slices.Equal(read.Buckets, []int64{4999, 5000}) {
		t.Fatalf("first sync: %+v, err %v", read, err)
	}
	metadataFiles := read.MetadataFiles(false, nil)
	if len(metadataFiles) != 2 || metadataFiles[0].Bucket != 4999 {
		t.Fatalf("first sync metadata = %+v", metadataFiles)
	}
	decoded, _ := utils.DecodeSafe85(metadataFiles[1].Text)
	if want := DeltaMetadataBytes(DeltaMetadataFlagReplace, 5000, nil, firstFile); !bytes.Equal(decoded, want) {
		t.Fatalf("metadata = %v, want %v", decoded, want)
	}

	// The client applied both files; a new entry lands in 5000. Only it comes back, and 4999 (held
	// whole, not sealed) sends no Metadata.
	write(5000, DeltaLogEntry{LogID: 1, Version: 2})
	read, err = ReadDeltaLog(folder, DeltaCursor{4999: len(previousFile), 5000: len(firstFile)}, deltaNow)
	if err != nil || read.IsReplace {
		t.Fatalf("later sync: %+v, err %v", read, err)
	}
	entries, err := read.LoggedEntries(false)
	if want := []DeltaLogEntry{{LogID: 1, Version: 2}}; err != nil || !slices.Equal(entries, want) {
		t.Fatalf("logged entries = %+v, err %v; want %+v", entries, err, want)
	}
	if metadataFiles = read.MetadataFiles(false, nil); len(metadataFiles) != 1 || metadataFiles[0].Bucket != 5000 {
		t.Fatalf("later sync metadata = %+v", metadataFiles)
	}

	// A cursor older than the replace window is answered with the whole view.
	if read, _ = ReadDeltaLog(folder, DeltaCursor{5000 - deltaLogReplaceAfterBuckets - 1: 0}, deltaNow); !read.IsReplace {
		t.Fatal("a cursor older than 7 days must be a replace")
	}
}

func TestParseDeltaCursor(t *testing.T) {
	if cursor := ParseDeltaCursor("5887211.40.5887212.8"); len(cursor) != 2 || cursor[5887211] != 40 || cursor[5887212] != 8 {
		t.Fatalf("cursor = %v", cursor)
	}
	if cursor := ParseDeltaCursor("5887211.40.x.8"); len(cursor) != 1 {
		t.Fatalf("a malformed pair must end the cursor: %v", cursor)
	}
	if cursor := ParseDeltaCursor(""); len(cursor) != 0 {
		t.Fatalf("empty cursor = %v", cursor)
	}
}

func TestDeltaReplaceMetadata(t *testing.T) {
	replace, err := utils.DecodeSafe85(DeltaReplaceMetadata())
	if err != nil || !bytes.Equal(replace, DeltaMetadataBytes(DeltaMetadataFlagReplace|DeltaMetadataFlagSealed, 0, nil, nil)) {
		t.Fatalf("replace metadata = %v, err %v", replace, err)
	}
}
