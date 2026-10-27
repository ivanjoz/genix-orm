package benchmarks

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Golden: the bytes of every file a build at t0 and a compaction at t1 write, hashed
// per case and pinned in testdata/golden.txt. An optimization that moves one hash is a
// format change and needs its own decision. The slot byte (byte 1) is zeroed before
// hashing: a rebuild hands slots out in map order. go test -run Golden -update rewrites
// the file.
// ─────────────────────────────────────────────────────────────────────────────

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden.txt")

const goldenPath = "testdata/golden.txt"

// digestOf hashes the frame files of a store: each key without its generation folder, then its bytes
// with the slot zeroed.
func digestOf(t *testing.T, store *utils.MemoryFileStore, generation string) (string, int) {
	t.Helper()
	hash := sha256.New()
	keys := frameFiles(store)
	for _, key := range keys {
		content, _, _ := store.GetFile(key)
		if len(content) > 1 {
			content[1] = 0
		}
		fmt.Fprintf(hash, "%s\n%d\n", strings.Replace(key, "/"+generation+"/", "/G/", 1), len(content))
		hash.Write(content)
	}
	return hex.EncodeToString(hash.Sum(nil)), len(keys)
}

func readGolden(t *testing.T) map[string]string {
	golden := map[string]string{}
	file, err := os.Open(goldenPath)
	if os.IsNotExist(err) {
		return golden
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if name, digest, isPair := strings.Cut(scanner.Text(), " "); isPair {
			golden[name] = digest
		}
	}
	return golden
}

func TestGolden(t *testing.T) {
	golden, got := readGolden(t), map[string]string{}
	var names []string
	for _, c := range Cases() {
		backend, store, err := c.build(true)
		if err != nil {
			t.Fatal(err)
		}
		builtGeneration := backend.state.Generation
		digest, fileCount := digestOf(t, store, builtGeneration)
		got[c.Name+"/build"], names = digest, append(names, c.Name+"/build")
		t.Logf("%s: %d records, %d lines, %d files", c.Name, len(c.Records), c.LineCount(), fileCount)

		if err := dataframe.Compact(c.Table, backend, store, partition, t1); err != nil {
			t.Fatal(err)
		}
		got[c.Name+"/compact"], _ = digestOf(t, store, builtGeneration)
		names = append(names, c.Name+"/compact")

		// The compaction at t1 holds what a rebuild at t1 sums.
		rebuiltBackend, rebuiltStore := newMemoryBackend(c, true), utils.NewMemoryFileStore()
		if err := dataframe.Rebuild(c.Table, rebuiltBackend, rebuiltStore, partition, "", t1); err != nil {
			t.Fatal(err)
		}
		if compacted, rebuilt := decodedFiles(t, c, store, builtGeneration), decodedFiles(t, c, rebuiltStore, rebuiltBackend.state.Generation); !reflect.DeepEqual(compacted, rebuilt) {
			t.Fatalf("%s: the compaction at t1 holds %d files, a rebuild at t1 %d, or their rows differ", c.Name, len(compacted), len(rebuilt))
		}
	}
	if *updateGolden {
		var out strings.Builder
		for _, name := range names {
			fmt.Fprintf(&out, "%s %s\n", name, got[name])
		}
		if err := os.WriteFile(goldenPath, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, name := range names {
		if golden[name] != got[name] {
			t.Errorf("%s: digest %s, golden %s", name, got[name], golden[name])
		}
	}
}

// decodedFiles are the rows and sums of every file with rows, by key without the generation.
func decodedFiles(t *testing.T, c *Case, store *utils.MemoryFileStore, generation string) map[string][2][][]int64 {
	t.Helper()
	frame := &c.Table.Frames[0]
	files := map[string][2][][]int64{}
	for _, key := range frameFiles(store) {
		content, _, _ := store.GetFile(key)
		file, err := dataframe.DecodeFile(content, len(frame.Rows), frame.SumsCount)
		if err != nil {
			t.Fatal(key, err)
		}
		if len(file.Rows[0]) > 0 {
			files[strings.Replace(key, "/"+generation+"/", "/G/", 1)] = [2][][]int64{file.Rows, file.Sums}
		}
	}
	return files
}
