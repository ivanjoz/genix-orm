package scylla

import (
	"strings"
	"testing"
	"time"
)

// Verifies the failed-dial path: every attempt is spent, the panic reports the
// attempt count, the session is left nil so the next caller retries from
// scratch, and nothing busy waits (the old code spun on a nil session forever).
func TestGetScyllaConnectionRetriesThenPanics(t *testing.T) {
	connParams = ConnParams{Host: "127.0.0.1", Port: 1, ConnTimeout: 1, QueryTimeout: 1, WriteTimeout: 1}
	scyllaSession = nil
	defer func() {
		connParams = ConnParams{}
		scyllaSession = nil
	}()

	// Sum of the backoff sleeps between attempts: 500ms + 1s + 2s.
	minElapsed := connectFirstRetryDelay + 2*connectFirstRetryDelay + connectMaxRetryDelay

	panicValue, elapsed := recoverGetScyllaConnection(t)

	message, isString := panicValue.(string)
	if !isString || !strings.Contains(message, "after 4 attempts") {
		t.Fatalf("panic esperado con el conteo de intentos, se obtuvo: %v", panicValue)
	}
	if elapsed < minElapsed {
		t.Fatalf("los reintentos no esperaron el backoff: %v < %v", elapsed, minElapsed)
	}
	if scyllaSession != nil {
		t.Fatal("scyllaSession debe quedar nil para que el próximo request reintente")
	}
}

func recoverGetScyllaConnection(t *testing.T) (panicValue any, elapsed time.Duration) {
	t.Helper()
	startTime := time.Now()
	defer func() {
		panicValue = recover()
		elapsed = time.Since(startTime)
		if panicValue == nil {
			t.Error("se esperaba un panic al no poder conectar")
		}
	}()
	getScyllaConnection()
	return nil, 0
}
