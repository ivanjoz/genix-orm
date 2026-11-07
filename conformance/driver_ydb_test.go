//go:build ydb && !fdb && !oceanbase

package conformance

import (
	"os"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/ydbdriver"
)

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = ydbdriver.TableStruct[TableT, RecordT]

const driverName = "ydb"

// connectDriver points the driver at GENIX_YDB_ENDPOINT (default: the docs/YDB_PLAN.md §5 container).
func connectDriver() error {
	endpoint := os.Getenv("GENIX_YDB_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:2136"
	}
	ydbdriver.SetConnection(ydbdriver.ConnParams{Endpoint: endpoint, Database: "/local", Keyspace: conformanceKeyspace})
	return ydbdriver.Ping()
}

// resetAndDeploy drops every table of the keyspace directory, then deploys the controllers.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := ydbdriver.WipeKeyspace(); err != nil {
		t.Fatal(err)
	}
	if err := ydbdriver.Deploy(controllers...); err != nil {
		t.Fatal(err)
	}
}

func makeController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	return &ydbdriver.YdbController[RecordT, TableT]{Schema: db.MakeSchema[RecordT]()}
}

// setFrameLock: this driver keeps no DataFrames yet.
func setFrameLock(acquire func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)) {
}
