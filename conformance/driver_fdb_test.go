//go:build fdb && !ydb

package conformance

import (
	"errors"
	"os"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/fdbdriver"
)

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = fdbdriver.TableStruct[TableT, RecordT]

const driverName = "fdb"

// connectDriver points the driver at FDBGATE_SOCKET (a running fdbgate, fdb/container).
func connectDriver() error {
	socketPath := os.Getenv("FDBGATE_SOCKET")
	if socketPath == "" {
		return errors.New("set FDBGATE_SOCKET to run the conformance suite on FoundationDB")
	}
	fdbdriver.SetConnection(fdbdriver.ConnParams{SocketPath: socketPath, Keyspace: conformanceKeyspace})
	return fdbdriver.Ping()
}

// resetAndDeploy clears the keyspace (records, indexes, counters). There is no schema to create:
// compiling each table is the deploy.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := fdbdriver.WipeKeyspace(); err != nil {
		t.Fatal(err)
	}
	for _, controller := range controllers {
		controller.GetTable()
	}
}

func makeController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	return &fdbdriver.FdbController[RecordT, TableT]{Schema: db.MakeSchema[RecordT]()}
}
