// Command fdbgate is the daemon that links libfdb_c and serves the fdb driver's frames over a unix
// socket (protocol: ../protocol.go). Configuration is by environment, see readConfig.
package main

import (
	"log"
	"os"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	socketPath := envOrDefault("FDBGATE_SOCKET", "/run/genix-fdb/fdbgate.sock")
	clusterFile := envOrDefault("FDB_CLUSTER_FILE", "/var/fdb/fdb.cluster")
	// Below FDB's 5 s hard limit, so a slow request answers status 4 instead of dying silently.
	transactionTimeoutMillis, _ := strconv.ParseInt(envOrDefault("FDBGATE_TX_TIMEOUT_MS", "4500"), 10, 64)

	fdb.MustAPIVersion(800)
	database, openError := fdb.OpenDatabase(clusterFile)
	if openError != nil {
		log.Fatalf("fdbgate: open database %s: %v", clusterFile, openError)
	}
	if optionError := database.Options().SetTransactionTimeout(transactionTimeoutMillis); optionError != nil {
		log.Fatalf("fdbgate: set transaction timeout: %v", optionError)
	}

	log.Printf("fdbgate: serving %s (cluster %s, tx timeout %d ms)", socketPath, clusterFile, transactionTimeoutMillis)
	gateInstance := &gate{database: database}
	log.Fatal(gateInstance.serve(socketPath))
}
