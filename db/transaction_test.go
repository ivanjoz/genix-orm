package db

import (
	"errors"
	"testing"
)

// A body that wraps a driver error in one of its own (core.Err keeps only the text) must still hand
// the runner the driver's error: it is what the runner retries a deadlock on.
func TestRunTransactionHandsDriverErrorToRunner(t *testing.T) {
	deadlock := errors.New("deadlock")
	var errorSeenByRunner error
	previousRunner := transactionRunner
	defer func() { transactionRunner = previousRunner }()
	transactionRunner = func(body func(driverTransaction any) error) error {
		errorSeenByRunner = body(struct{}{})
		return errorSeenByRunner
	}

	RunTransaction(func(transaction *Transaction) error {
		transaction.noteDriverError(deadlock)
		return errors.New("Error al obtener las cajas: deadlock")
	})
	if !errors.Is(errorSeenByRunner, deadlock) {
		t.Fatalf("the runner saw %v, want the driver's error", errorSeenByRunner)
	}

	// A body that fails on its own, with no driver error, hands its error through.
	bodyFailure := errors.New("validation failed")
	RunTransaction(func(transaction *Transaction) error { return bodyFailure })
	if !errors.Is(errorSeenByRunner, bodyFailure) {
		t.Fatalf("the runner saw %v, want the body's error", errorSeenByRunner)
	}
}
