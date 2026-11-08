//go:build oceanbase

package conformance

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/obdriver"
)

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = obdriver.TableStruct[TableT, RecordT]

const driverName = "oceanbase"

// connectDriver points the driver at GENIX_OCEANBASE_HOST / _PORT / _USER / _PASSWORD (default: the
// homelab server, docs/OCEANBASE_PLAN.md §5). The conformance keyspace is the OceanBase database.
func connectDriver() error {
	host := os.Getenv("GENIX_OCEANBASE_HOST")
	if host == "" {
		host = "100.64.0.4"
	}
	port, _ := strconv.Atoi(os.Getenv("GENIX_OCEANBASE_PORT"))
	if port == 0 {
		port = 2881
	}
	user := os.Getenv("GENIX_OCEANBASE_USER")
	if user == "" {
		user = "root@test"
	}
	obdriver.SetConnection(obdriver.ConnParams{
		Host: host, Port: port, User: user, Password: os.Getenv("GENIX_OCEANBASE_PASSWORD"),
		Database: conformanceKeyspace, Partitions: 1,
	})
	return obdriver.Ping()
}

// resetAndDeploy drops and recreates the conformance database, then deploys the controllers.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := obdriver.WipeDatabase(); err != nil {
		t.Fatal(err)
	}
	if err := obdriver.Deploy(controllers...); err != nil {
		t.Fatal(err)
	}
}

func makeController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	return &obdriver.ObController[RecordT, TableT]{Schema: db.MakeSchema[RecordT]()}
}

// setFrameLock installs the lock a DataFrame compaction takes.
func setFrameLock(acquire func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)) {
	obdriver.AcquireFrameLock = acquire
}
