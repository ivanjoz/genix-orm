package dataframe

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// The write path (../DATA_FRAMES.md, section 6). Inserts reach the frames from the
// table: a run reads the records written since its snapshot. Updates and deletes
// also need the values the record held before, which only the write sees, so the
// driver's write appends them to each frame's log (AppendLogEntries) before its base
// write, when the frame's values change. Every write lands within its deadline
// (WriteWindow), which is what lets a run know when a version has settled.
// ─────────────────────────────────────────────────────────────────────────────

// ErrWriteDeadline is what a write to a table with DataFrames returns when it could not land within
// its deadline (Configure) of reading the stored records: nothing it had not sent yet was written.
// Retry it.
var ErrWriteDeadline = errors.New("db: the write passed its DataFrame write deadline: retry it")

const (
	// CancelGrace is how long after its deadline a write may still append the cancel markers of the
	// records it logged and did not land.
	CancelGrace = 3 * time.Second
	// settleMargin covers a request already sent when its deadline hits, and the clock skew between
	// Lambdas.
	settleMargin = 2 * time.Second
)

var (
	configuredStore Store
	writeDeadline   = 10 * time.Second
)

// Configure sets the store of every table's DataFrame files and the write deadline (10 s by default):
// a write to a table with frames lands within it of reading the stored records, or fails with
// ErrWriteDeadline. Compactions reach a version twice the deadline plus 5 s after it was stamped
// (settle). Call it once at boot, before the first write.
func Configure(store Store, deadline time.Duration) {
	configuredStore, writeDeadline = store, deadline
}

// WriteDeadline is the configured write deadline.
func WriteDeadline() time.Duration { return writeDeadline }

// settle is how long after a checkpoint is read every write with a version up to it has landed or
// given up, its cancel markers appended. Such a write stamped its version before the checkpoint, so
// it lands by the deadline after it. A write that loses to it read the record before it landed, so it
// is done one deadline plus the grace later.
func settle() time.Duration { return 2*writeDeadline + CancelGrace + settleMargin }

// settleSeconds is settle rounded up: checkpoint times are whole seconds.
func settleSeconds() int64 { return int64((settle() + time.Second - 1) / time.Second) }

func configured() (Store, error) {
	if configuredStore == nil {
		return nil, errors.New("db: no frame store is set: call dataframe.Configure at boot")
	}
	return configuredStore, nil
}

// WriteWindow bounds a write to a table with frames. It opens at the stored read the write diffs
// against, or at the version stamp when it reads nothing: the log entries and the base items are
// sent by landBy, and the cancel markers of the records logged and not landed within CancelGrace
// after. The zero WriteWindow is a table without frames: no bound.
type WriteWindow struct{ landBy time.Time }

// OpenWriteWindow is the window of a write that starts at start.
func OpenWriteWindow(start time.Time) WriteWindow {
	return WriteWindow{landBy: start.Add(writeDeadline)}
}

// LandingContext bounds the calls that land the write; now is the driver's clock.
func (window WriteWindow) LandingContext(now time.Time) (context.Context, context.CancelFunc) {
	return window.contextUntil(window.landBy, now)
}

// CancelContext bounds the append of the cancel markers.
func (window WriteWindow) CancelContext(now time.Time) (context.Context, context.CancelFunc) {
	return window.contextUntil(window.landBy.Add(CancelGrace), now)
}

func (window WriteWindow) contextUntil(at, now time.Time) (context.Context, context.CancelFunc) {
	if window.landBy.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), at.Sub(now))
}

// LandingError marks the error of a write that ran out of its window with ErrWriteDeadline.
func LandingError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrWriteDeadline, err)
	}
	return err
}

// CheckValues fails a write holding a value its frames can't store: a negative Keys or Rows value
// (they name files and are delta-coded), or a negative Sums value on a frame without AllowNegativeSums.
func CheckValues(frames []Frame, records []unsafe.Pointer) error {
	for _, record := range records {
		for i := range frames {
			frame := &frames[i]
			checkedColumns := append(slices.Clip(frame.Keys), frame.Rows)
			if !frame.AllowsNegativeSums {
				checkedColumns = append(checkedColumns, frame.Sums...)
			}
			for _, column := range checkedColumns {
				if value := column.Get(record); value < 0 {
					return fmt.Errorf("db: %s %s is %d: DataFrame %q takes no negative value there", frame.RecordName, column.FieldName, value, frame.Name)
				}
			}
		}
	}
	return nil
}

// LoggedWrite is one record a write call replaces, as the frames' logs need it. Stored and Written are
// the record before and after (Written nil for a delete), each nil when it counts in no frame
// (soft-deleted).
type LoggedWrite struct {
	SK string
	// CreatedVersion is the stored record's: the incarnation the entry belongs to.
	CreatedVersion int64
	// NewVersion is the write's Updated.
	NewVersion      int64
	Stored, Written unsafe.Pointer
}

// AppendCancelMarkers appends the cancel markers of logged writes that did not land: they lost their
// condition, or were never sent. Left in the log, the entry of a write that never landed could read as
// the record's values after another write that did. They go within CancelGrace after the window.
func AppendCancelMarkers(window WriteWindow, now time.Time, frames []Frame, writes []LoggedWrite) error {
	ctx, cancel := window.CancelContext(now)
	defer cancel()
	return AppendLogEntries(ctx, frames, writes, true)
}

// AppendLogEntries appends to each frame's log one entry per write that changes the frame's values:
// the stored values, keyed by the write's version. The driver calls it before the base write, so a
// change a run sees in the table always has its entry. isCancel appends instead the markers that void
// the entries of writes that did not land (AppendCancelMarkers). One append per frame, frames in
// parallel, bounded by ctx.
func AppendLogEntries(ctx context.Context, frames []Frame, writes []LoggedWrite, isCancel bool) error {
	entriesByFrame := make([][]LogEntry, len(frames))
	for _, write := range writes {
		for i := range frames {
			oldValues := frames[i].ValuesOf(write.Stored)
			if EqualValues(oldValues, frames[i].ValuesOf(write.Written)) {
				continue
			}
			entriesByFrame[i] = append(entriesByFrame[i], LogEntry{
				NewVersion: write.NewVersion, CreatedVersion: write.CreatedVersion, SK: write.SK, IsCancel: isCancel, OldValues: oldValues,
			})
		}
	}
	var appendedFrames []int
	for i, entries := range entriesByFrame {
		if len(entries) > 0 {
			appendedFrames = append(appendedFrames, i)
		}
	}
	if len(appendedFrames) == 0 {
		return nil
	}
	store, err := configured()
	if err != nil {
		return fmt.Errorf("db: %s has DataFrames: %w", frames[0].RecordName, err)
	}
	return parallel.Run(len(appendedFrames), func(position int) error {
		frameIndex := appendedFrames[position]
		return AppendLog(ctx, store, &frames[frameIndex], entriesByFrame[frameIndex])
	})
}
