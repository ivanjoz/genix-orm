package main

import (
	"log"
	"net"
	"os"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	fdbclient "github.com/ivanjoz/genix-orm/fdb"
)

type gate struct {
	database fdb.Database
}

// serve accepts connections on the unix socket. The socket file is the only access control
// (0660); there is deliberately no TCP listener.
func (gate *gate) serve(socketPath string) error {
	_ = os.Remove(socketPath) // stale socket from a previous run
	listener, listenError := net.Listen("unix", socketPath)
	if listenError != nil {
		return listenError
	}
	if chmodError := os.Chmod(socketPath, 0o660); chmodError != nil {
		return chmodError
	}
	for {
		conn, acceptError := listener.Accept()
		if acceptError != nil {
			return acceptError
		}
		go gate.serveConnection(conn)
	}
}

// serveConnection reads frames and answers each in its own goroutine, so replies may arrive out of
// order; the client matches them by correlation.
func (gate *gate) serveConnection(conn net.Conn) {
	defer conn.Close()
	var writeMutex sync.Mutex
	for {
		lead, correlation, payload, readError := fdbclient.ReadFrame(conn)
		if readError != nil {
			return
		}
		go func() {
			status, body := gate.handleOp(fdbclient.Op(lead), payload)
			writeMutex.Lock()
			writeError := fdbclient.WriteFrame(conn, uint8(status), correlation, body)
			writeMutex.Unlock()
			if writeError != nil {
				log.Printf("fdbgate: write reply: %v", writeError)
				conn.Close()
			}
		}()
	}
}
