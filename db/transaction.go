package db

import (
	"fmt"
	"unsafe"
)

// Transactions across tables. A SQL driver (pgdriver, mariadbdriver) runs the body of RunTransaction
// in one database transaction: LockByKeys reads records under a row lock, InsertIn, UpdateIn, UpsertIn
// and InsertUpdateIncludeIn queue their writes, and everything commits at once. A write of rows
// LockByKeys locked reuses what it read instead of reading them again. A deadlock reruns the whole body, so the body must
// derive what it writes from what it reads inside it.
//
// A duplicate key reruns the body too. A record that does not exist yet cannot be locked, so two
// bodies can both find it missing and both insert it: insert it with InsertIn (a plain INSERT, never
// an upsert), and the loser's rerun finds the winner's record (by LockByKeys, or by a read through a
// TypeUniqueIndex) and works on it.
//
// A driver without transactions (Scylla and the others) runs the body as it is: each call is its own
// write, and LockByKeys is a plain read by key. The body is the same on every driver.

// Transaction is the transaction a RunTransaction body writes through.
type Transaction struct {
	// driverTransaction is the driver's open transaction; nil when the driver has none.
	driverTransaction any
	// afterCommit runs once the transaction committed (a text-search sync).
	afterCommit []func() error
	// driverError is the first error a driver call of the body returned. The body may wrap it in an
	// error of its own (core.Err keeps only the text), so the runner is handed this one instead: it is
	// what tells a deadlock, which reruns the body, from any other failure.
	driverError error
}

// noteDriverError keeps the first driver error of the body and returns err.
func (transaction *Transaction) noteDriverError(err error) error {
	if err != nil && transaction.driverError == nil {
		transaction.driverError = err
	}
	return err
}

// transactionRunner runs body in one database transaction, retrying it on a deadlock: installed by
// the SQL driver the process connects (SetTransactionRunner).
var transactionRunner func(body func(driverTransaction any) error) error

// SetTransactionRunner installs the driver's transactions: called by a SQL driver's SetConnection.
func SetTransactionRunner(runner func(body func(driverTransaction any) error) error) {
	transactionRunner = runner
}

// transactionalExecutor is a driver that reads and writes inside a driver transaction.
type transactionalExecutor[RecordT any] interface {
	LockByKeys(driverTransaction any, records *[]RecordT) error
	// InsertIn and UpdateIn return what runs after the commit.
	InsertIn(driverTransaction any, records *[]RecordT, columnsToExclude ...Coln) (func() error, error)
	UpdateIn(driverTransaction any, records *[]RecordT, columnsToInclude ...Coln) (func() error, error)
	UpsertIn(driverTransaction any, records *[]RecordT, columnsToExclude ...Coln) (func() error, error)
	InsertUpdateIncludeIn(driverTransaction any, records *[]RecordT, isInsert func(record *RecordT) bool,
		columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln) (func() error, error)
}

// RunTransaction runs body in one transaction of the connected SQL driver, or call by call on a
// driver without transactions.
func RunTransaction(body func(transaction *Transaction) error) error {
	if transactionRunner == nil {
		transaction := &Transaction{}
		if err := body(transaction); err != nil {
			return err
		}
		return transaction.runAfterCommit()
	}
	var committed *Transaction
	err := transactionRunner(func(driverTransaction any) error {
		// Each attempt starts clean: a retried body queues its writes again.
		committed = &Transaction{driverTransaction: driverTransaction}
		bodyError := body(committed)
		if bodyError != nil && committed.driverError != nil {
			return committed.driverError
		}
		return bodyError
	})
	if err != nil {
		return err
	}
	return committed.runAfterCommit()
}

func (transaction *Transaction) runAfterCommit() error {
	for _, afterCommit := range transaction.afterCommit {
		if err := afterCommit(); err != nil {
			return err
		}
	}
	return nil
}

// transactionalExecutorOf is the executor of a table inside an open driver transaction.
func transactionalExecutorOf[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
) (transactionalExecutor[RecordT], error) {
	executor := executorFor[RecordT, TableT, D]()
	transactional, isTransactional := executor.(transactionalExecutor[RecordT])
	if !isTransactional {
		return nil, fmt.Errorf("db: driver %q cannot write inside a transaction", executor.Name())
	}
	return transactional, nil
}

// LockByKeys replaces records with their stored versions, read by primary key (partition and keys)
// and locked until the transaction ends. A record with nothing stored is dropped from records.
func LockByKeys[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, records *[]RecordT,
) error {
	if transaction.driverTransaction == nil {
		return readByKeys[RecordT, TableT, D](records)
	}
	transactional, err := transactionalExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	return transaction.noteDriverError(transactional.LockByKeys(transaction.driverTransaction, records))
}

// InsertIn writes new records (as Insert) inside the transaction.
func InsertIn[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, records *[]RecordT, columnsToExclude ...Coln,
) error {
	if transaction.driverTransaction == nil {
		return Insert[RecordT, TableT, D](records, columnsToExclude...)
	}
	transactional, err := transactionalExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	afterCommit, err := transactional.InsertIn(transaction.driverTransaction, records, columnsToExclude...)
	if err != nil {
		return transaction.noteDriverError(err)
	}
	transaction.afterCommit = append(transaction.afterCommit, afterCommit)
	return nil
}

// UpdateIn writes the given columns of records (as Update) inside the transaction.
func UpdateIn[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, records *[]RecordT, columnsToInclude ...Coln,
) error {
	if transaction.driverTransaction == nil {
		return Update[RecordT, TableT, D](records, columnsToInclude...)
	}
	transactional, err := transactionalExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	afterCommit, err := transactional.UpdateIn(transaction.driverTransaction, records, columnsToInclude...)
	if err != nil {
		return transaction.noteDriverError(err)
	}
	transaction.afterCommit = append(transaction.afterCommit, afterCommit)
	return nil
}

// UpsertIn writes records whether or not they are stored (as Upsert) inside the transaction.
func UpsertIn[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, records *[]RecordT, columnsToExclude ...Coln,
) error {
	if transaction.driverTransaction == nil {
		return Upsert[RecordT, TableT, D](records, columnsToExclude...)
	}
	transactional, err := transactionalExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	afterCommit, err := transactional.UpsertIn(transaction.driverTransaction, records, columnsToExclude...)
	if err != nil {
		return transaction.noteDriverError(err)
	}
	transaction.afterCommit = append(transaction.afterCommit, afterCommit)
	return nil
}

// InsertUpdateIncludeIn splits records with isInsert (as InsertUpdateInclude) inside the transaction.
func InsertUpdateIncludeIn[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	transaction *Transaction, records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	if transaction.driverTransaction == nil {
		return InsertUpdateInclude[RecordT, TableT, D](records, isInsert, columnsToIncludeUpdate, columnsToExcludeInsert...)
	}
	transactional, err := transactionalExecutorOf[RecordT, TableT, D]()
	if err != nil {
		return err
	}
	afterCommit, err := transactional.InsertUpdateIncludeIn(transaction.driverTransaction, records, isInsert, columnsToIncludeUpdate, columnsToExcludeInsert...)
	if err != nil {
		return transaction.noteDriverError(err)
	}
	transaction.afterCommit = append(transaction.afterCommit, afterCommit)
	return nil
}

// readByKeys is LockByKeys on a driver without transactions: a plain read of each record by its
// partition and keys, one read per record.
func readByKeys[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT,
) error {
	executor := executorFor[RecordT, TableT, D]()
	table := MakeTable[RecordT, TableT, D]()
	schema := InitStructTable[TableT, RecordT](new(TableT))
	stored := make([]RecordT, 0, len(*records))
	for recordIndex := range *records {
		recordPointer := unsafe.Pointer(&(*records)[recordIndex])
		statements := []ColumnStatement{}
		if partitionColumn := table.GetPartKey(); partitionColumn != nil && !partitionColumn.IsNil() {
			statements = append(statements, ColumnStatement{Col: partitionColumn.GetName(), Operator: "=", Value: partitionColumn.GetRawValue(recordPointer)})
		}
		for keyIndex, keyValue := range table.GetKeyValues(recordPointer) {
			statements = append(statements, ColumnStatement{Col: table.GetKeys()[keyIndex].GetName(), Operator: "=", Value: keyValue})
		}
		if err := executor.Select(schema, &TableInfo{Statements: statements}, &stored, nil); err != nil {
			return err
		}
	}
	*records = stored
	return nil
}
