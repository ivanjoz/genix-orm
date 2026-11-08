// Package thirdparty holds the forked gocql this module cannot use unmodified, and the test that
// keeps the fork honest. See README.md for why the fork exists and why it lives here rather than
// in the consuming app.
package thirdparty

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGocqlForkStillDropsRecreate fails if a bump restored gocql's recreate.go. That file is the
// only thing pulling text/template into every binary built on this ORM, and losing the deletion
// costs 262,144 bytes with nothing else failing: it still compiles and still passes every
// functional test.
func TestGocqlForkStillDropsRecreate(t *testing.T) {
	if _, err := os.Stat("gocql/go.mod"); err != nil {
		t.Fatalf("the gocql fork is missing or incomplete: %v", err)
	}
	if _, err := os.Stat("gocql/recreate.go"); err == nil {
		t.Error("gocql/recreate.go is back; it links text/template. Re-run thirdparty/regenerate.sh")
	}

	// The deletion is only a proxy for the thing that actually matters. If upstream ever moves
	// those templates into another file, removing recreate.go silently stops working.
	var filesImportingTemplate []string
	_ = filepath.WalkDir("gocql", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil //nolint:nilerr // a partially readable fork is reported by the go.mod check above
		}
		contents, readErr := os.ReadFile(path)
		if readErr == nil && strings.Contains(string(contents), `"text/template"`) {
			filesImportingTemplate = append(filesImportingTemplate, path)
		}
		return nil
	})
	if len(filesImportingTemplate) > 0 {
		t.Errorf("text/template is reachable again via %v -- upstream moved the templates, so deleting recreate.go is no longer enough",
			filesImportingTemplate)
	}
}

// TestConsumingAppReplacesTheSameFork guards the duplicated replace directive. A replace in a
// non-main module is ignored by Go, so this module's go.mod covers only its own build and tests;
// the app has to name this directory itself. If the app's line is dropped, these tests keep
// passing against the fork while the app silently builds the unpatched driver.
func TestConsumingAppReplacesTheSameFork(t *testing.T) {
	appGoMod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Skipf("no consuming app alongside this module: %v", err)
	}
	if !strings.Contains(string(appGoMod), "./genix-orm/thirdparty/gocql") {
		t.Error("the app's go.mod no longer replaces gocql with ./genix-orm/thirdparty/gocql; " +
			"this module's replace does not apply to the app's build, so it is linking upstream gocql")
	}
}
