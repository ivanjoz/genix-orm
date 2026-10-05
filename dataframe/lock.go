package dataframe

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// The frame lock (../DATA_FRAMES.md, section 8). One compaction at a time
// writes a frame's files, its indexes and its w: the scheduled run, a rebuild or
// the express compaction of a fresh read, and every write of it goes through its
// Lock. The lock is the frame folder's _lock object: its owner and its expiry.
// It is taken and renewed with conditional PUTs (If-None-Match to create it,
// If-Match on the ETag read to take over an expired one or to renew one's own):
// S3 answers 412 to every writer but one, so nothing is read back.
//
// A holder sends no write later than lockMargin before its expiry, and the lock is
// only taken over once expired: the margin covers a request already sent when its
// deadline hits and the clock skew between Lambdas, as a write's deadline does (D5).
// A holder that needs longer renews the lock as it writes.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// LockDuration is how long a lock lasts after it was taken or last renewed.
	LockDuration = 20 * time.Second
	// lockRenewBelow is the time left under which a write renews the lock first.
	lockRenewBelow = 10 * time.Second
	lockMargin     = 2 * time.Second
)

// ErrLockLost is a holder's write once its lock expired and another compaction took it over: the
// holder writes nothing more, and what it wrote is ahead of w, as a crash leaves it.
var ErrLockLost = errors.New("db: the frame lock expired and was taken over before the compaction ended")

// Lock is a frame lock held, and the writes of its holder. Safe for concurrent use: a compaction writes
// its files in parallel.
type Lock struct {
	store Store
	key   string
	owner string
	now   func() time.Time
	mutex sync.Mutex
	// etag and expiry are those of the holder's last write of the lock.
	etag   string
	expiry time.Time
}

// TakeLock takes the frame's lock, or returns nil while another holder's lock is live or another taker
// won the race for an expired one. now is the caller's clock.
func TakeLock(store Store, frame *Frame, now func() time.Time) (*Lock, error) {
	lock := &Lock{store: store, key: frame.lockKey(), owner: strconv.FormatUint(rand.Uint64(), 36), now: now}
	content, etag, err := store.Get(lock.key)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, err
	}
	// A lock that doesn't decode is taken over like an expired one.
	if expiryMillis, bytesRead := binary.Varint(content); bytesRead > 0 && now().Before(time.UnixMilli(expiryMillis)) {
		return nil, nil
	}
	err = lock.write(etag, now().Add(LockDuration))
	if errors.Is(err, ErrPreconditionFailed) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// write writes the lock with expiry over the version whose ETag is etag ("": none). The owner in the
// content makes every holder's ETag its own.
func (lock *Lock) write(etag string, expiry time.Time) error {
	content := append(binary.AppendVarint(nil, expiry.UnixMilli()), lock.owner...)
	newETag, err := lock.store.PutIfMatch(context.Background(), lock.key, content, etag)
	if err != nil {
		return err
	}
	lock.etag, lock.expiry = newETag, expiry
	return nil
}

// WriteContext bounds one write of the holder: it ends lockMargin before the lock expires. With less
// than lockRenewBelow left, the lock is renewed first, on the ETag of the holder's last write of it: a
// lock taken over since fails with ErrLockLost. One that expired but nobody took over is renewed: no
// other compaction wrote meanwhile.
func (lock *Lock) WriteContext() (context.Context, context.CancelFunc, error) {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if lock.expiry.Sub(lock.now()) < lockRenewBelow {
		err := lock.write(lock.etag, lock.now().Add(LockDuration))
		if errors.Is(err, ErrPreconditionFailed) {
			return nil, nil, ErrLockLost
		}
		if err != nil {
			return nil, nil, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), lock.expiry.Add(-lockMargin).Sub(lock.now()))
	return ctx, cancel, nil
}

// Release lets the next compaction take the lock at once: it writes it expired, unless it was taken
// over since. A holder that crashes never releases: its lock expires LockDuration after its last renewal.
func (lock *Lock) Release() error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if err := lock.write(lock.etag, time.UnixMilli(0)); err != nil && !errors.Is(err, ErrPreconditionFailed) {
		return err
	}
	return nil
}

func (lock *Lock) put(key string, content []byte) error {
	ctx, cancel, err := lock.WriteContext()
	if err != nil {
		return err
	}
	defer cancel()
	return lock.store.Put(ctx, key, content)
}

func (lock *Lock) append(key string, content []byte) error {
	ctx, cancel, err := lock.WriteContext()
	if err != nil {
		return err
	}
	defer cancel()
	return lock.store.Append(ctx, key, content)
}

// Delete removes objects of the frame as the holder.
func (lock *Lock) Delete(keys ...string) error {
	ctx, cancel, err := lock.WriteContext()
	if err != nil {
		return err
	}
	defer cancel()
	return lock.store.Delete(ctx, keys...)
}
