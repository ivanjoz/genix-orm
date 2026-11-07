//go:build mariadb && !fdb && !ydb && !oceanbase && !postgres

package conformance

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/mariadbdriver"
)

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = mariadbdriver.TableStruct[TableT, RecordT]

const driverName = "mariadb"

// connectDriver points the driver at GENIX_MARIADB_HOST / _PORT / _USER / _PASSWORD (default: the
// load-test container, 127.0.0.1:3307, root / genix). The conformance keyspace is a MariaDB database.
func connectDriver() error {
	host := os.Getenv("GENIX_MARIADB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("GENIX_MARIADB_PORT"))
	if port == 0 {
		port = 3307
	}
	user := os.Getenv("GENIX_MARIADB_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("GENIX_MARIADB_PASSWORD")
	if _, isSet := os.LookupEnv("GENIX_MARIADB_PASSWORD"); !isSet {
		password = "genix"
	}
	mariadbdriver.SetConnection(mariadbdriver.ConnParams{Host: host, Port: port, User: user, Password: password, Database: conformanceKeyspace})
	return mariadbdriver.Ping()
}

// resetAndDeploy drops and recreates the conformance database, then deploys the controllers.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := mariadbdriver.WipeDatabase(); err != nil {
		t.Fatal(err)
	}
	if err := mariadbdriver.Deploy(controllers...); err != nil {
		t.Fatal(err)
	}
}

func makeController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	return &mariadbdriver.MariaController[RecordT, TableT]{Schema: db.MakeSchema[RecordT]()}
}

// setFrameLock: this driver's aggregate tables take no compaction lock.
func setFrameLock(acquire func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)) {
}
