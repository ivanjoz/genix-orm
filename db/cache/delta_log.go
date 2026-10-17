package cache

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta logs: the change-log files of the TypeDelta indexes, in the
// utils.FileStore, and the rules a Delta() read follows over them. A driver names
// the folders (its base pk, the index tag, the pinned values), computes the log
// IDs and reads the records; the files and their buckets are this file's.
//
//	key = delta-log/<base pk>/<index tag>/<pinned value>/…/<bucket>
//	bucket = floor(SUnixTime / 300): the 10 minutes the append landed in
//
// Write (DeltaLogAppends): once the records landed, every write appends one entry
// per index to the file of each pinned folder the record is in (delta_log_codec.go),
// and a deleted entry to each folder it left: a hard delete, a soft delete
// (Status 0), or a pinned value that changed. The appends run in parallel, under
// appendDeltaLogTimeout, and an append that fails fails the write: the caller
// retries it.
//
// Read (ReadDeltaLog): a client's cursor holds, per file still open, how many of
// its bytes it applied. Append order is landing order, so reading every file past
// those offsets misses nothing, whatever clock stamped the writes. A bucket is
// sealed once its last append can no longer land: DeltaBucketSealDelay after its
// end (the append timeout plus clock skew). Then the client drops it from its
// cursor.
//
//   - First sync (no cursor), or a replace: read the current bucket's file (and the
//     previous one while it isn't sealed), then the driver lists the view. The files
//     are read first, so a write landing in between comes again on the next sync,
//     where the client skips it by version.
//   - Later sync: read the held files, the current one, and, when the cursor is
//     more than one bucket behind, the files a listing finds in between. Per log ID
//     the last entry past the cursor wins; the driver reads the IDs not deleted
//     consistently. A cursor older than deltaLogReplaceAfterBuckets, or more than
//     deltaLogMaxFilesRead files behind, gets a replace: a first sync whose Metadata
//     records tell the client to drop what the response doesn't bring.
//
// The response ends with one Metadata record per file that moved for the client:
// its log ID's last column is −bucket, and Metadata is the Safe85 of the file's
// whole content behind a header (DeltaMetadataBytes): flags, bucket, log ID widths.
//
// Retention: a lifecycle rule on the bucket deletes the files after 8 days, the 7
// days a cursor is answered from the log plus a margin.
// ─────────────────────────────────────────────────────────────────────────────

const (
	DeltaLogFolder = "delta-log/"
	// DeltaBucketTicks is a bucket's length in SUnixTime ticks: 10 minutes.
	DeltaBucketTicks = 300
	// appendDeltaLogTimeout bounds the appends of one write.
	appendDeltaLogTimeout = 20 * time.Second
	// DeltaBucketSealDelay is how long after its end a bucket may still take an append.
	DeltaBucketSealDelay = 30 * time.Second
	// deltaLogReplaceAfterBuckets: a cursor older than 7 days is answered with the whole view.
	deltaLogReplaceAfterBuckets = 7 * 144
	// deltaLogMaxFilesRead: past one day of files, listing the view is cheaper than reading them.
	deltaLogMaxFilesRead = 144
	// MaxVersion is the highest record Version: the write after it stores 1 again.
	MaxVersion = 32767
)

// NextVersion is the Version of the write after the one stored at version (0: nothing stored).
func NextVersion(version int64) int64 {
	if version >= MaxVersion {
		return 1
	}
	return version + 1
}

// DeltaBucketOf is the bucket a moment falls in.
func DeltaBucketOf(moment time.Time) int64 { return utils.SUnixTimeOf(moment) / DeltaBucketTicks }

// IsDeltaBucketSealed reports whether bucket can no longer take an append at now.
func IsDeltaBucketSealed(bucket int64, now time.Time) bool {
	bucketEnd := SUnixTimeToTime((bucket + 1) * DeltaBucketTicks)
	return !now.Before(bucketEnd.Add(DeltaBucketSealDelay))
}

// DeltaCursor is what a client holds of a delta index's log: per log file still open (its 10-minute
// bucket), how many of its bytes it applied. Empty on a first sync.
type DeltaCursor map[int64]int

// ParseDeltaCursor reads a cursor as genix-ui sends it: "<bucket>.<bytes>" pairs joined by ".". A
// malformed pair ends the cursor where it is.
func ParseDeltaCursor(encodedCursor string) DeltaCursor {
	cursorParts := strings.Split(encodedCursor, ".")
	cursor := DeltaCursor{}
	for i := 0; i+1 < len(cursorParts); i += 2 {
		bucket, bucketErr := strconv.ParseInt(cursorParts[i], 10, 64)
		heldBytes, bytesErr := strconv.Atoi(cursorParts[i+1])
		if bucketErr != nil || bytesErr != nil || heldBytes < 0 {
			break
		}
		cursor[bucket] = heldBytes
	}
	return cursor
}

// DeltaLogAppends gathers the entries of one write call, per file, the last entry of a log ID in the
// call winning.
type DeltaLogAppends struct {
	bucketName    string
	appendsByFile map[string]*deltaFileAppend
	fileKeys      []string
}

type deltaFileAppend struct {
	is64BitScale bool
	logIDs       []uint64
	entryByLogID map[uint64]DeltaLogEntry
}

// NewDeltaLogAppends starts the appends of a write landing at now.
func NewDeltaLogAppends(now time.Time) *DeltaLogAppends {
	return &DeltaLogAppends{
		bucketName:    strconv.FormatInt(DeltaBucketOf(now), 10),
		appendsByFile: map[string]*deltaFileAppend{},
	}
}

// Add queues entry for the current bucket's file of folder (which ends in "/").
func (appends *DeltaLogAppends) Add(folder string, is64BitScale bool, entry DeltaLogEntry) {
	fileKey := folder + appends.bucketName
	pending := appends.appendsByFile[fileKey]
	if pending == nil {
		pending = &deltaFileAppend{is64BitScale: is64BitScale, entryByLogID: map[uint64]DeltaLogEntry{}}
		appends.appendsByFile[fileKey] = pending
		appends.fileKeys = append(appends.fileKeys, fileKey)
	}
	if _, isListed := pending.entryByLogID[entry.LogID]; !isListed {
		pending.logIDs = append(pending.logIDs, entry.LogID)
	}
	pending.entryByLogID[entry.LogID] = entry
}

// Write appends every queued file in parallel. A failed append is the write's error.
func (appends *DeltaLogAppends) Write() error {
	if len(appends.fileKeys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendDeltaLogTimeout)
	defer cancel()
	return utils.RunParallel(len(appends.fileKeys), func(fileIndex int) error {
		fileKey := appends.fileKeys[fileIndex]
		pending := appends.appendsByFile[fileKey]
		entries := make([]DeltaLogEntry, len(pending.logIDs))
		for i, logID := range pending.logIDs {
			entries[i] = pending.entryByLogID[logID]
		}
		if err := fileStore.AppendBytesToFile(ctx, fileKey, EncodeDeltaLogEntries(entries, pending.is64BitScale)); err != nil {
			return fmt.Errorf("appending to the delta log %s: %w", fileKey, err)
		}
		return nil
	})
}

// DeltaLogRead is the log side of one Delta() read: the files it read and whether the driver must
// list the whole view (a replace) instead of reading the logged records.
type DeltaLogRead struct {
	IsReplace bool
	// Buckets are the files read, ascending, with their Contents (nil for a missing file).
	Buckets  []int64
	Contents [][]byte
	cursor   DeltaCursor
	now      time.Time
}

// ReadDeltaLog picks the files a cursor needs in folder (the index's folder with its pinned values,
// ending in "/") and reads them. The files go first: a write landing after them comes again on the
// next sync.
func ReadDeltaLog(folder string, cursor DeltaCursor, now time.Time) (*DeltaLogRead, error) {
	currentBuckets := []int64{DeltaBucketOf(now)}
	if !IsDeltaBucketSealed(currentBuckets[0]-1, now) {
		currentBuckets = append(currentBuckets, currentBuckets[0]-1)
	}
	buckets, isReplace := currentBuckets, len(cursor) == 0
	if !isReplace {
		oldestHeldBucket := currentBuckets[0]
		for heldBucket := range cursor {
			buckets = append(buckets, heldBucket)
			oldestHeldBucket = min(oldestHeldBucket, heldBucket)
		}
		isReplace = oldestHeldBucket < currentBuckets[0]-deltaLogReplaceAfterBuckets
		if !isReplace && currentBuckets[0]-oldestHeldBucket > 1 {
			listedBuckets, err := ListDeltaLogBuckets(folder, oldestHeldBucket)
			if err != nil {
				return nil, err
			}
			buckets = append(buckets, listedBuckets...)
		}
		slices.Sort(buckets)
		buckets = slices.Compact(buckets)
		if isReplace || len(buckets) > deltaLogMaxFilesRead {
			buckets, isReplace = currentBuckets, true
		}
	}
	slices.Sort(buckets)
	contents, err := ReadDeltaLogFiles(folder, buckets)
	if err != nil {
		return nil, err
	}
	return &DeltaLogRead{IsReplace: isReplace, Buckets: buckets, Contents: contents, cursor: cursor, now: now}, nil
}

// LoggedEntries decodes the entries past the cursor, the last one per log ID winning, in first-seen
// order of their log IDs.
func (read *DeltaLogRead) LoggedEntries(is64BitScale bool) ([]DeltaLogEntry, error) {
	var logIDs []uint64
	lastEntryByLogID := map[uint64]DeltaLogEntry{}
	for i, bucket := range read.Buckets {
		unreadContent := read.Contents[i]
		// A file shorter than what the client holds is not the one it read: read it whole.
		if heldBytes := read.cursor[bucket]; heldBytes <= len(unreadContent) {
			unreadContent = unreadContent[heldBytes:]
		}
		entries, err := DecodeDeltaLogEntries(unreadContent, is64BitScale)
		if err != nil {
			return nil, fmt.Errorf("delta log bucket %d: %w", bucket, err)
		}
		for _, entry := range entries {
			if _, isListed := lastEntryByLogID[entry.LogID]; !isListed {
				logIDs = append(logIDs, entry.LogID)
			}
			lastEntryByLogID[entry.LogID] = entry
		}
	}
	lastEntries := make([]DeltaLogEntry, len(logIDs))
	for i, logID := range logIDs {
		lastEntries[i] = lastEntryByLogID[logID]
	}
	return lastEntries, nil
}

// DeltaMetadataFile is the Metadata of one log file the client must apply: the driver returns it as a
// record whose log ID's last column is −Bucket and whose Metadata is Text.
type DeltaMetadataFile struct {
	Bucket int64
	Text   string
}

// MetadataFiles lists the Metadata of every file that moved for the client. logIDColumnBits are the
// packed log ID's column Sizes, empty for a one-column log ID.
func (read *DeltaLogRead) MetadataFiles(is64BitScale bool, logIDColumnBits []int8) []DeltaMetadataFile {
	flags := byte(0)
	if is64BitScale {
		flags |= DeltaMetadataFlag64BitScale
	}
	if read.IsReplace {
		flags |= DeltaMetadataFlagReplace
	}
	var metadataFiles []DeltaMetadataFile
	for i, bucket := range read.Buckets {
		heldBytes, isHeld := read.cursor[bucket]
		isSealed := IsDeltaBucketSealed(bucket, read.now)
		if !read.IsReplace && isHeld && heldBytes == len(read.Contents[i]) && !isSealed {
			continue // nothing new, and the client keeps it
		}
		bucketFlags := flags
		if isSealed {
			bucketFlags |= DeltaMetadataFlagSealed
		}
		metadataText := utils.EncodeSafe85(DeltaMetadataBytes(bucketFlags, bucket, logIDColumnBits, read.Contents[i]))
		metadataFiles = append(metadataFiles, DeltaMetadataFile{Bucket: bucket, Text: metadataText})
	}
	return metadataFiles
}

// ListDeltaLogBuckets lists the buckets of the files in folder from fromBucket on.
func ListDeltaLogBuckets(folder string, fromBucket int64) ([]int64, error) {
	fileKeys, err := fileStore.ListFiles(folder)
	if err != nil {
		return nil, fmt.Errorf("db: listing the delta log %s: %w", folder, err)
	}
	var buckets []int64
	for _, fileKey := range fileKeys {
		bucket, err := strconv.ParseInt(strings.TrimPrefix(fileKey, folder), 10, 64)
		if err == nil && bucket >= fromBucket {
			buckets = append(buckets, bucket)
		}
	}
	return buckets, nil
}

// ReadDeltaLogFiles reads the files of buckets, in order; a missing one is empty.
func ReadDeltaLogFiles(folder string, buckets []int64) ([][]byte, error) {
	contents := make([][]byte, len(buckets))
	err := utils.RunParallel(len(buckets), func(bucketIndex int) error {
		fileKey := folder + strconv.FormatInt(buckets[bucketIndex], 10)
		content, _, err := fileStore.GetFile(fileKey)
		if errors.Is(err, utils.ErrFileNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("db: reading the delta log %s: %w", fileKey, err)
		}
		contents[bucketIndex] = content
		return nil
	})
	return contents, err
}

// DeltaReplaceMetadata is the Metadata of a route not backed by a delta log that answers its whole
// view every time: one record carrying it (any negative ID) tells the client to drop every held
// record the response doesn't bring. It is sealed, so the client keeps no cursor and sends none.
func DeltaReplaceMetadata() string {
	return utils.EncodeSafe85(DeltaMetadataBytes(DeltaMetadataFlagReplace|DeltaMetadataFlagSealed, 0, nil, nil))
}

// PinnedValueText is one pinned value as a log folder name.
func PinnedValueText(value any) string {
	reflected := reflect.ValueOf(value)
	switch {
	case reflected.CanInt():
		return strconv.FormatInt(reflected.Int(), 10)
	case reflected.CanUint():
		return strconv.FormatUint(reflected.Uint(), 10)
	default:
		return url.PathEscape(fmt.Sprint(value))
	}
}
