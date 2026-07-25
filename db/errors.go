package db

import (
	"errors"
	"fmt"
)

// Err builds an error from a message and any trailing values, so ORM call sites
// can report context without each one importing fmt.
func Err(message string, values ...any) error {
	if len(values) == 0 {
		return errors.New(message)
	}
	return fmt.Errorf("%s%v", message, fmt.Sprint(values...))
}
