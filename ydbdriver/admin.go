package ydbdriver

// GetAutoincrementID reserves recordsSize IDs from the _sequences table (plan §12, Step 4).
func GetAutoincrementID(key string, recordsSize int) (int64, error) {
	return 0, errNotImplemented("GetAutoincrementID")
}

// SetCounter moves a sequence to an absolute value and returns what it held (Step 4).
func SetCounter(counterName string, value int64) (int64, error) {
	return 0, errNotImplemented("SetCounter")
}

// ListCounters returns the names of every sequence (Step 4).
func ListCounters() ([]string, error) { return nil, errNotImplemented("ListCounters") }

// WipePartition deletes one partition of a table and of its side tables (Step 7).
func WipePartition(namespace string, tableID int16, partition int64) error {
	return errNotImplemented("WipePartition")
}
