//go:build !fdb && !ydb

package conformance

import (
	"errors"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/scylla"
)

// The Scylla side of the suite, and the only driver that records golden files.

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = scylla.TableStruct[TableT, RecordT]

const driverName = "scylla"

// connectDriver dials GENIX_ORM_SCYLLA_TEST (host:port of a throwaway node).
func connectDriver() error {
	address := os.Getenv("GENIX_ORM_SCYLLA_TEST")
	if address == "" {
		return errors.New("set GENIX_ORM_SCYLLA_TEST=host:port to run the conformance suite on Scylla")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return err
	}
	scylla.MakeScyllaConnection(scylla.ConnParams{Host: host, Port: port, Keyspace: conformanceKeyspace})
	return nil
}

// resetAndDeploy drops the keyspace, so sequences and side tables start empty, then deploys.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := scylla.QueryExec("DROP KEYSPACE IF EXISTS " + conformanceKeyspace); err != nil {
		t.Fatal(err)
	}
	if err := scylla.Init(); err != nil {
		t.Fatal(err)
	}
	scylla.DeployScylla(0, controllers...)
}

func makeController[RecordT scylla.RecordOf[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	schema := db.MakeSchema[RecordT]()
	return &scylla.ScyllaController[RecordT, TableT]{
		TableName: conformanceKeyspace + "." + schema.Name,
		Table:     scylla.MakeScyllaTable[RecordT](),
		Schema:    schema,
	}
}
