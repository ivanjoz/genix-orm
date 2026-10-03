package daemon

import (
	"fmt"
	"strconv"
	"strings"
)

// Match is one result from a QUERY: a record key plus the relevance score
// GenixSearch assigned it. Weight is 0 when the daemon omits the "|<score>"
// half of a token.
type Match struct {
	ID     int32
	Weight float32
}

// buildQueryLine composes the wire command for a QUERY. Pulled out so
// tests can pin the framing without dialing a live daemon.
func buildQueryLine(table, partition, query string, limit, offset int) string {
	line := fmt.Sprintf("QUERY %s %s %s", table, partition, quote(query))
	if limit > 0 {
		line += fmt.Sprintf(" LIMIT(%d)", limit)
	}
	if offset > 0 {
		line += fmt.Sprintf(" OFFSET(%d)", offset)
	}
	return line
}

// decodeMatches parses payload tokens from an EVENT QUERY frame into Match
// values. GenixSearch emits each token as "<key>|<score>"; a token without
// a '|' separator is accepted as a bare key with zero weight for
// forward-compat.
func decodeMatches(payload []string) ([]Match, error) {
	matches := make([]Match, 0, len(payload))
	for _, tok := range payload {
		key := tok
		var weight float32
		if idx := strings.IndexByte(tok, '|'); idx >= 0 {
			key = tok[:idx]
			if scoreStr := tok[idx+1:]; scoreStr != "" {
				score, err := strconv.ParseFloat(scoreStr, 32)
				if err != nil {
					return nil, fmt.Errorf("daemon: bad score %q in EVENT QUERY payload: %w", tok, err)
				}
				weight = float32(score)
			}
		}
		n, err := strconv.ParseInt(key, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("daemon: bad key %q in EVENT QUERY payload: %w", tok, err)
		}
		matches = append(matches, Match{ID: int32(n), Weight: weight})
	}
	return matches, nil
}
