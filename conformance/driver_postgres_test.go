//go:build postgres && !oceanbase

package conformance

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/pgdriver"
)

type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = pgdriver.TableStruct[TableT, RecordT]

const driverName = "postgres"

// connectDriver points the driver at GENIX_POSTGRES_HOST / _PORT / _USER / _PASSWORD (default: the
// load-test container, 127.0.0.1:5433; GENIX_POSTGRES_DATABASE picks the database, for YugabyteDB
// YSQL). The conformance keyspace is a PostgreSQL schema.
func connectDriver() error {
	host := os.Getenv("GENIX_POSTGRES_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("GENIX_POSTGRES_PORT"))
	if port == 0 {
		port = 5433
	}
	user := os.Getenv("GENIX_POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("GENIX_POSTGRES_PASSWORD")
	if _, isSet := os.LookupEnv("GENIX_POSTGRES_PASSWORD"); !isSet {
		password = "genix"
	}
	pgdriver.SetConnection(pgdriver.ConnParams{Host: host, Port: port, User: user, Password: password,
		DatabaseName: os.Getenv("GENIX_POSTGRES_DATABASE"), Database: conformanceKeyspace})
	return pgdriver.Ping()
}

// resetAndDeploy drops and recreates the conformance schema, then deploys the controllers.
func resetAndDeploy(t *testing.T, controllers ...db.Controller) {
	t.Helper()
	if err := pgdriver.WipeDatabase(); err != nil {
		t.Fatal(err)
	}
	if err := pgdriver.Deploy(controllers...); err != nil {
		t.Fatal(err)
	}
}

func makeController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]]() db.Controller {
	return &pgdriver.PgController[RecordT, TableT]{Schema: db.MakeSchema[RecordT]()}
}

// setFrameLock: this driver's aggregate tables take no compaction lock.
func setFrameLock(acquire func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)) {
}
