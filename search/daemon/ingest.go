package daemon

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// commandBudget returns the number of bytes a command line may use,
// leaving room for the trailing LF the server reads as the line
// separator.
func commandBudget(c *searchConn) int {
	bs := c.bufferSize
	if bs <= 0 {
		bs = 20000
	}
	return bs - 1
}

// popIPipelineChunkSize bounds the pipelined POPI window so the server's
// pending-command and pending-reply queues don't overflow on large batches
// (e.g. a 10k-record write would otherwise push ~400KB onto the wire
// before reading a single reply, which Sonic-style servers reset).
const popIPipelineChunkSize = 500

// popIPipeline writes POPI commands for ids in chunks of popIPipelineChunkSize,
// flushing and reading replies between chunks. Each chunk is one network
// round trip; the chunk size keeps the in-flight command/reply window within
// the server's per-connection limits.
//
// Each reply must be RESULT 1 (existed) or RESULT 0 (missing); both
// are valid. A ProtocolError reply from the server is non-fatal for
// the connection — we surface it but keep the pool entry usable.
// Any framing failure marks the conn broken.
func popIPipeline(ctx context.Context, c *searchConn, collection, bucket string, ids []int32) error {
	if len(ids) == 0 {
		return nil
	}
	if c.broken {
		return fmt.Errorf("%w: connection already broken", ErrProtocol)
	}

	var firstProtoErr error
	for chunkStart := 0; chunkStart < len(ids); chunkStart += popIPipelineChunkSize {
		chunkEnd := chunkStart + popIPipelineChunkSize
		if chunkEnd > len(ids) {
			chunkEnd = len(ids)
		}
		chunkIDs := ids[chunkStart:chunkEnd]

		// Reset the deadline per chunk so a slow remote link doesn't accumulate
		// the full 10k-record budget against the single 10s commandTimeout.
		if deadline, ok := ctx.Deadline(); ok {
			_ = c.netConn.SetDeadline(deadline)
		} else {
			_ = c.netConn.SetDeadline(time.Now().Add(commandTimeout))
		}

		for _, id := range chunkIDs {
			line := "POPI " + collection + " " + bucket + " " + strconv.FormatInt(int64(id), 10)
			if _, err := c.writer.WriteString(line); err != nil {
				c.broken = true
				return err
			}
			if err := c.writer.WriteByte('\n'); err != nil {
				c.broken = true
				return err
			}
		}
		if err := c.writer.Flush(); err != nil {
			c.broken = true
			return err
		}
		for range chunkIDs {
			line, err := readLine(c.reader)
			if err != nil {
				c.broken = true
				return err
			}
			res, perr := parseResult(line)
			if perr != nil {
				if _, ok := perr.(*ProtocolError); ok {
					if firstProtoErr == nil {
						firstProtoErr = perr
					}
					continue
				}
				c.broken = true
				return perr
			}
			if res.kind != kindResult {
				c.broken = true
				return fmt.Errorf("%w: expected RESULT for POPI, got %v", ErrProtocol, res.kind)
			}
		}
	}
	return firstProtoErr
}

// pushI sends one or more PUSHI <col> <bucket> <id> "<text>" ... lines
// batched to fit conn.bufferSize. PUSHI replaces the entry keyed by
// (collection, bucket, id), so we don't need a prior POPI on the same
// bucket.
//
// NormalizeSearchText guarantees the payload contains no characters
// that quote() escapes, so the quoted form is exactly len(text)+2
// bytes — we can size each entry without rendering it.
//
// If a single record's text would exceed the budget on a fresh line,
// the text is trimmed at the last space below the limit (or hard-cut
// for a pathological single huge token).
func pushI(ctx context.Context, c *searchConn, collection, bucket string, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	base := "PUSHI " + collection + " " + bucket
	budget := commandBudget(c)

	var sb strings.Builder
	sb.Grow(budget)
	sb.WriteString(base)
	for _, r := range records {
		idStr := strconv.FormatInt(int64(r.ID), 10)
		text := r.SearchText
		entry := 1 + len(idStr) + 1 + len(text) + 2 // " " id " " "text"
		if sb.Len()+entry > budget && sb.Len() > len(base) {
			if _, err := c.exec(ctx, sb.String()); err != nil {
				return err
			}
			sb.Reset()
			sb.WriteString(base)
		}
		// Even on a fresh line the record may not fit — trim text.
		maxText := budget - len(base) - 1 - len(idStr) - 1 - 2
		if maxText <= 0 {
			continue
		}
		if len(text) > maxText {
			if cut := strings.LastIndexByte(text[:maxText], ' '); cut > 0 {
				text = text[:cut]
			} else {
				text = text[:maxText]
			}
			if text == "" {
				continue
			}
		}
		sb.WriteByte(' ')
		sb.WriteString(idStr)
		sb.WriteByte(' ')
		sb.WriteString(quote(text))
	}
	if sb.Len() > len(base) {
		if _, err := c.exec(ctx, sb.String()); err != nil {
			return err
		}
	}
	return nil
}
