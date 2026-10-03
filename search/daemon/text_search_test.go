package daemon

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// --- Unit tests for the pure helpers ----------------------------------

func TestNormalizeSearchText(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"Hello World", "hello world"},
		// Symbols are dropped (not spaced), so they merge the surrounding
		// letters instead of splitting the word.
		{"  Foo--Bar  baz!!  ", "foobar baz"},
		{"123-abc.DEF", "123abcdef"},
		{"   ", ""},
		{"only_letters", "onlyletters"},
		{"Marca Za&Co", "marca zaco"},
		// Spanish accents fold to their base letter; "Jamón" and "Jamon"
		// must tokenize identically.
		{"Jamón", "jamon"},
		{"Jamon", "jamon"},
		{"CAÑA 100LTS", "cana 100lts"},
		{"Pingüino Ñandú", "pinguino nandu"},
	}
	for _, tc := range cases {
		if got := NormalizeSearchText(tc.in); got != tc.want {
			t.Errorf("NormalizeSearchText(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidateIdentifier(t *testing.T) {
	good := []string{"productos", "p1", "n3", "P57", "_meta"}
	for _, s := range good {
		if err := validateIdentifier(s); err != nil {
			t.Errorf("validateIdentifier(%q) = %v, want nil", s, err)
		}
	}
	bad := []string{"", "1abc", "with space", "weird!", "p-1"}
	for _, s := range bad {
		if err := validateIdentifier(s); err == nil {
			t.Errorf("validateIdentifier(%q) returned nil, want error", s)
		}
	}
}

func TestQuoteHandlesQuotesAndBackslashes(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", `""`},
		{"hello", `"hello"`},
		{`a"b`, `"a\"b"`},
		{`a\b`, `"a\\b"`},
		{"a\nb", `"a\nb"`},
		{"a\tb", `"a\tb"`},
	}
	for _, tc := range cases {
		if got := quote(tc.in); got != tc.want {
			t.Errorf("quote(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseResultRecognizesAllFrameTypes(t *testing.T) {
	res, err := parseResult("OK")
	if err != nil || res.kind != kindOK {
		t.Fatalf("OK frame: %v %+v", err, res)
	}
	res, err = parseResult("RESULT 5")
	if err != nil || res.kind != kindResult || res.count != 5 {
		t.Fatalf("RESULT frame: %v %+v", err, res)
	}
	res, err = parseResult("PENDING abc123")
	if err != nil || res.kind != kindPending || res.marker != "abc123" {
		t.Fatalf("PENDING frame: %v %+v", err, res)
	}
	res, err = parseResult("EVENT QUERY abc123 7|50 99|30 1234|10")
	if err != nil || res.kind != kindEvent || res.eventKind != "QUERY" ||
		res.marker != "abc123" || len(res.payload) != 3 {
		t.Fatalf("EVENT frame: %v %+v", err, res)
	}
	res, err = parseResult("PONG")
	if err != nil || res.kind != kindPong {
		t.Fatalf("PONG frame: %v %+v", err, res)
	}
	res, err = parseResult("ENDED quit")
	if err != nil || res.kind != kindEnded {
		t.Fatalf("ENDED frame: %v %+v", err, res)
	}
	res, err = parseResult("CONNECTED <genixsearch v0.1.0>")
	if err != nil || res.kind != kindConnected {
		t.Fatalf("CONNECTED frame: %v %+v", err, res)
	}
	res, err = parseResult("STARTED ingest protocol(1) buffer(20000)")
	if err != nil || res.kind != kindStarted || res.bufferSize != 20000 {
		t.Fatalf("STARTED frame: %v %+v", err, res)
	}
	res, err = parseResult("ERR unknown_command")
	var pErr *ProtocolError
	if !errors.As(err, &pErr) || res.kind != kindErr || pErr.Reason != "unknown_command" {
		t.Fatalf("ERR frame: %v %+v", err, res)
	}
}

func TestDecodeMatchesParsesKeyScoreTokens(t *testing.T) {
	matches, err := decodeMatches([]string{"1|50", "42|30.5", "9999|10"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Match{{1, 50}, {42, 30.5}, {9999, 10}}
	if len(matches) != len(want) {
		t.Fatalf("len=%d, want %d", len(matches), len(want))
	}
	for i, v := range want {
		if matches[i] != v {
			t.Errorf("matches[%d] = %+v, want %+v", i, matches[i], v)
		}
	}
	// Bare-key token (no '|') still accepted for forward-compat.
	if matches, err := decodeMatches([]string{"7"}); err != nil || len(matches) != 1 || matches[0] != (Match{ID: 7}) {
		t.Errorf("bare key decode: matches=%v err=%v", matches, err)
	}
	if _, err := decodeMatches([]string{"abc|10"}); err == nil {
		t.Errorf("expected error for non-numeric key")
	}
}

func TestBuildQueryLine(t *testing.T) {
	got := buildQueryLine("productos", "p7", "fresa", 20, 10)
	want := `QUERY productos p7 "fresa" LIMIT(20) OFFSET(10)`
	if got != want {
		t.Errorf("buildQueryLine = %q, want %q", got, want)
	}
	// No LIMIT / OFFSET when zero.
	got = buildQueryLine("productos", "p7", "fresa", 0, 0)
	want = `QUERY productos p7 "fresa"`
	if got != want {
		t.Errorf("buildQueryLine (no limit/offset) = %q, want %q", got, want)
	}
}

// --- Integration tests with a net.Pipe-based fake daemon -------------

// fakeServer is a deterministic in-process replacement for the search
// daemon, reachable over a single net.Pipe. The test installs the
// server side as a dial target by swapping out the global pool. We
// don't go through dialAndHandshake (which would expect a real TCP
// listener); instead the test wires a conn directly into a private
// pool and exercises the backend.

type fakeServer struct {
	t       *testing.T
	r       *bufio.Reader
	w       *bufio.Writer
	conn    net.Conn
	mu      sync.Mutex
	scripts []string // sequence of canned replies in order
}

func newFakeServer(t *testing.T) (*fakeServer, net.Conn) {
	client, server := net.Pipe()
	f := &fakeServer{
		t:    t,
		r:    bufio.NewReader(server),
		w:    bufio.NewWriter(server),
		conn: server,
	}
	return f, client
}

// queueReply appends a canned line that the fake will send after the
// next inbound command is consumed.
func (f *fakeServer) queueReply(line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripts = append(f.scripts, line)
}

// run loops reading commands and replying with canned scripts. Returns
// the list of commands it saw (without trailing CR/LF) when the client
// closes the connection or the test signals done.
func (f *fakeServer) run(stop <-chan struct{}) []string {
	var got []string
	for {
		line, err := f.r.ReadString('\n')
		if err != nil {
			return got
		}
		got = append(got, strings.TrimRight(line, "\r\n"))
		f.mu.Lock()
		var reply string
		if len(f.scripts) > 0 {
			reply = f.scripts[0]
			f.scripts = f.scripts[1:]
		}
		f.mu.Unlock()
		if reply != "" {
			// Real server writes \r\n; mirror that here so the client's
			// readLine exercises its CR-stripping path.
			_, _ = f.w.WriteString(reply + "\r\n")
			_ = f.w.Flush()
		}
		select {
		case <-stop:
			return got
		default:
		}
	}
}

// makePooledConn returns a searchConn backed by a client net.Conn,
// bypassing dialAndHandshake. We pre-set bufferSize as if STARTED
// already happened.
func makePooledConn(client net.Conn, bufSize int) *searchConn {
	return &searchConn{
		mode:       "ingest",
		netConn:    client,
		reader:     bufio.NewReader(client),
		writer:     bufio.NewWriter(client),
		bufferSize: bufSize,
		usedAt:     time.Now(),
	}
}

// installFakeIngestPool replaces the global ingest pool with one whose
// only connection is the supplied searchConn. initPools() preserves
// non-nil pools, so subsequent Apply calls reuse this one.
func installFakeIngestPool(c *searchConn) (restore func()) {
	prev := ingestMgr
	p := &connPool{
		mode:      "ingest",
		min:       1,
		max:       1,
		idle:      []*searchConn{c},
		open:      1,
		pendingCh: make(chan struct{}, 1),
	}
	ingestMgr = p
	return func() {
		ingestMgr = prev
	}
}

// applyAgainstFake runs one Backend.Apply against a fake daemon that
// answers with replies, and returns the frames it received.
func applyAgainstFake(t *testing.T, bufSize int, replies []string, puts []db.TextDoc, dels []int32) ([]string, error) {
	fake, client := newFakeServer(t)
	defer client.Close()
	defer fake.conn.Close()

	for _, reply := range replies {
		fake.queueReply(reply)
	}

	stop := make(chan struct{})
	defer close(stop)
	var got []string
	done := make(chan struct{})
	go func() {
		got = fake.run(stop)
		close(done)
	}()

	restore := installFakeIngestPool(makePooledConn(client, bufSize))
	defer restore()

	err := NewBackend().Apply(context.Background(), "t", "p1", puts, dels)
	_ = client.Close()
	<-done
	return got, err
}

func TestPushISplitsWhenLineWouldOverflow(t *testing.T) {
	// Buffer sized so two records cannot fit on one PUSHI line.
	// "PUSHI t p1" header = 10 chars; entry ` 1 "ab"` = 7 chars.
	// bufferSize=18 -> budget=17. 10+7=17 fits; +7 more = 24 doesn't.
	puts := []db.TextDoc{{ID: 1, Text: "ab"}, {ID: 2, Text: "cd"}}
	got, err := applyAgainstFake(t, 18, []string{"PONG", "RESULT 1", "RESULT 1"}, puts, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	want := []string{
		"PING",
		`PUSHI t p1 1 "ab"`,
		`PUSHI t p1 2 "cd"`,
	}
	if len(got) < len(want) {
		t.Fatalf("got %d frames, want %d: %q", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("frame %d = %q, want %q", i, got[i], w)
		}
	}
}

func TestPushITruncatesOversizeText(t *testing.T) {
	// bufferSize 31 -> budget 30. Header `PUSHI t p1` = 10.
	// Entry prefix ` 1 "` = 4, trailing `"` = 1, so per-entry overhead = 5.
	// Remaining text budget = 30 - 10 - 5 = 15.
	text := "one two three four five six"
	got, err := applyAgainstFake(t, 31, []string{"PONG", "RESULT 1"}, []db.TextDoc{{ID: 1, Text: text}}, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(got) < 2 {
		t.Fatalf("expected >=2 frames, got %d: %q", len(got), got)
	}
	pushLine := got[1]
	if !strings.HasPrefix(pushLine, `PUSHI t p1 1 "`) || !strings.HasSuffix(pushLine, `"`) {
		t.Fatalf("unexpected PUSHI frame: %q", pushLine)
	}
	if len(pushLine) > 30 {
		t.Errorf("PUSHI frame exceeds budget: len=%d frame=%q", len(pushLine), pushLine)
	}
	// The trimmed text must be a word-boundary prefix of the original.
	quoted := strings.TrimSuffix(strings.TrimPrefix(pushLine, `PUSHI t p1 1 "`), `"`)
	if !strings.HasPrefix(text, quoted) || strings.HasSuffix(quoted, " ") {
		t.Errorf("trimmed text not a clean prefix: %q (orig %q)", quoted, text)
	}
}

func TestApplyRecoversFromProtocolError(t *testing.T) {
	_, err := applyAgainstFake(t, 20000, []string{"PONG", "ERR something_broken"}, []db.TextDoc{{ID: 1, Text: "foo"}}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pErr *ProtocolError
	if !errors.As(err, &pErr) {
		t.Fatalf("expected ProtocolError, got %T: %v", err, err)
	}
	if pErr.Reason != "something_broken" {
		t.Errorf("unexpected reason: %q", pErr.Reason)
	}
}

func TestPoolDiscardsBrokenConnection(t *testing.T) {
	// A connection that fails the PING at acquire-time is discarded.
	p := newConnPool("ingest", 0, 1)
	broken := &searchConn{
		mode:    "ingest",
		netConn: brokenConn{},
		reader:  bufio.NewReader(brokenConn{}),
		writer:  bufio.NewWriter(brokenConn{}),
		usedAt:  time.Now(),
	}
	p.idle = append(p.idle, broken)
	p.open = 1
	// acquire will try PING, fail, drop the conn, then attempt to dial
	// — which will fail because Configure was not called in this test.
	// We expect the dial error, NOT a "pool closed" / panic.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := p.acquire(ctx)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
}

type brokenConn struct{}

func (brokenConn) Read(_ []byte) (int, error)         { return 0, io.ErrClosedPipe }
func (brokenConn) Write(_ []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (brokenConn) Close() error                       { return nil }
func (brokenConn) LocalAddr() net.Addr                { return nil }
func (brokenConn) RemoteAddr() net.Addr               { return nil }
func (brokenConn) SetDeadline(_ time.Time) error      { return nil }
func (brokenConn) SetReadDeadline(_ time.Time) error  { return nil }
func (brokenConn) SetWriteDeadline(_ time.Time) error { return nil }
