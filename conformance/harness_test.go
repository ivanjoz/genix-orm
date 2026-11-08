// Package conformance runs one set of tests against whichever driver the build tag selects (none:
// MariaDB, -tags postgres, -tags fdb, -tags ydb) and compares what each test observes with golden
// files recorded on MariaDB. The drivers cannot share a binary (each installs process-wide codecs and
// column metadata in init), so the comparison goes through testdata/ instead of running both side by
// side.
//
//	GENIX_CONFORMANCE=1 go test .                                   # MariaDB, GENIX_MARIADB_PORT (3307)
//	GENIX_CONFORMANCE=1 GENIX_CONFORMANCE_UPDATE=1 go test .        # re-record on MariaDB
//	GENIX_CONFORMANCE=1 go test -tags postgres .                    # GENIX_POSTGRES_PORT (5433)
//	GENIX_CONFORMANCE=1 FDBGATE_SOCKET=... go test -tags fdb .
//	GENIX_CONFORMANCE=1 go test -tags ydb .
//
// A driver that answers "not implemented" fails the test with a message starting with
// "not implemented:", so the gaps of a driver in progress are one rg away.
package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

const conformanceKeyspace = "genix_orm_conformance"

// clockStart is the managed time (SUnixTime) every test starts at: 2025-10-09, fixed so the stamps
// the golden files hold do not move between runs.
const clockStart int32 = 380_000_000

// asyncIndexTimeout bounds how long a read waits for an async index (YDB) to catch up with a write.
const asyncIndexTimeout = 20 * time.Second

// goldenStep is one observation of a test: what it read or got back at one point.
type goldenStep struct {
	Step  string          `json:"step"`
	Value json.RawMessage `json:"value"`
}

type conformanceRun struct {
	t             *testing.T
	goldenPath    string
	recording     bool
	expectedSteps []goldenStep
	observed      []goldenStep
}

// TestMain connects before any test compiles a table: a compiled table keeps the keyspace it saw.
func TestMain(m *testing.M) {
	if os.Getenv("GENIX_CONFORMANCE") == "1" {
		if err := connectDriver(); err != nil {
			fmt.Fprintln(os.Stderr, "conformance:", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// runConformance resets the keyspace, deploys the controllers, puts the managed clock at
// clockStart, and runs the body. Panics that say "not implemented" (controller methods panic) become
// not-implemented failures instead of crashing the binary.
func runConformance(t *testing.T, controllers []db.Controller, body func(run *conformanceRun)) {
	if os.Getenv("GENIX_CONFORMANCE") != "1" {
		t.Skip("set GENIX_CONFORMANCE=1 to run the conformance suite")
	}
	run := &conformanceRun{
		t:          t,
		goldenPath: filepath.Join("testdata", t.Name()+".golden.json"),
		recording:  driverName == "mariadb" && os.Getenv("GENIX_CONFORMANCE_UPDATE") == "1",
	}
	if !run.recording {
		content, err := os.ReadFile(run.goldenPath)
		if err != nil {
			t.Fatalf("no golden file (record it on MariaDB with GENIX_CONFORMANCE_UPDATE=1): %v", err)
		}
		if err := json.Unmarshal(content, &run.expectedSteps); err != nil {
			t.Fatalf("%s: %v", run.goldenPath, err)
		}
	}

	run.setClock(0)
	t.Cleanup(func() { db.SetHistoricalUnix(0) })
	resetAndDeploy(t, controllers...)

	defer func() {
		if recovered := recover(); recovered != nil {
			if strings.Contains(fmt.Sprint(recovered), "not implemented") {
				t.Fatalf("not implemented: %v", recovered)
			}
			panic(recovered)
		}
	}()
	body(run)

	if run.recording && !t.Failed() {
		content, err := json.MarshalIndent(run.observed, "", "  ")
		if err == nil {
			err = os.WriteFile(run.goldenPath, append(content, '\n'), 0o644)
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if len(run.observed) != len(run.expectedSteps) {
		t.Fatalf("observed %d steps, the golden file holds %d", len(run.observed), len(run.expectedSteps))
	}
}

// groupSettleTicks is the driver's cache-group settle window: a SQL driver stamps its marks right
// before the COMMIT, so it settles sooner than the non-transactional drivers.
func groupSettleTicks() int32 {
	if driverName == "postgres" || driverName == "mariadb" {
		return db.SQLIndexGroupSettleTicks
	}
	return db.IndexGroupSettleTicks
}

// setClock puts the managed clock (the stamps writes get, and the read time of cache reads) at
// clockStart + offset ticks. One tick is 2 seconds.
func (run *conformanceRun) setClock(offset int32) {
	db.SetHistoricalUnix(1e9 + 2*int64(clockStart+offset))
}

// must fails the test on a write error, tagging the ones a driver has not implemented yet.
func (run *conformanceRun) must(operation string, err error) {
	run.t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "not implemented") {
		run.t.Fatalf("not implemented: %s: %v", operation, err)
	}
	run.t.Fatalf("%s: %v", operation, err)
}

// observe compares a value with the golden step at the same position (or records it).
func (run *conformanceRun) observe(step string, value any) {
	run.t.Helper()
	encoded := run.encode(step, value)
	if !run.recording {
		run.compare(step, encoded, true)
	}
	run.observed = append(run.observed, goldenStep{Step: step, Value: encoded})
}

// read observes what a read returns, re-running it until it matches the golden step or
// asyncIndexTimeout passes: a read served by an async index may lag the write before it.
func (run *conformanceRun) read(step string, readValue func() (any, error)) {
	run.t.Helper()
	deadline := time.Now().Add(asyncIndexTimeout)
	for {
		value, err := readValue()
		run.must(step, err)
		encoded := run.encode(step, value)
		if run.recording || run.compare(step, encoded, !time.Now().Before(deadline)) {
			run.observed = append(run.observed, goldenStep{Step: step, Value: encoded})
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (run *conformanceRun) encode(step string, value any) json.RawMessage {
	run.t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		run.t.Fatalf("%s: %v", step, err)
	}
	return encoded
}

// compare reports whether the observation matches the golden step at the same position; on a
// mismatch it fails the test when failOnMismatch is set.
func (run *conformanceRun) compare(step string, encoded json.RawMessage, failOnMismatch bool) bool {
	run.t.Helper()
	position := len(run.observed)
	if position >= len(run.expectedSteps) {
		run.t.Fatalf("step %d %q is not in the golden file", position, step)
	}
	expected := run.expectedSteps[position]
	if expected.Step != step {
		run.t.Fatalf("step %d is %q, the golden file has %q", position, step, expected.Step)
	}
	var expectedValue, observedValue any
	_ = json.Unmarshal(expected.Value, &expectedValue)
	_ = json.Unmarshal(encoded, &observedValue)
	if reflect.DeepEqual(expectedValue, observedValue) {
		return true
	}
	if failOnMismatch {
		run.t.Fatalf("step %d %q on %s:\n got: %s\nwant: %s", position, step, driverName, encoded, expected.Value)
	}
	return false
}
