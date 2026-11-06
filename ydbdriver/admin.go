package ydbdriver

// WipePartition deletes one partition of a table and of its side tables (Step 7).
func WipePartition(namespace string, tableID int16, partition int64) error {
	return errNotImplemented("WipePartition")
}
