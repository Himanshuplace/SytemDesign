// Package kvstore provides a thread-safe, persistent key-value store.
package kvstore

import (
	"errors"
	"fmt"
)

// Sentinel errors for common conditions.
var (
	// ErrKeyNotFound is returned when a key does not exist in the store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyExpired is returned when a key has expired.
	ErrKeyExpired = errors.New("key expired")

	// ErrStoreClosed is returned when operations are attempted on a closed store.
	ErrStoreClosed = errors.New("store is closed")

	// ErrEmptyKey is returned when an empty key is provided.
	ErrEmptyKey = errors.New("key cannot be empty")

	// ErrNilValue is returned when a nil value is provided.
	ErrNilValue = errors.New("value cannot be nil")

	// ErrTransactionConflict is returned when a transaction cannot be committed due to conflicts.
	ErrTransactionConflict = errors.New("transaction conflict")

	// ErrTransactionClosed is returned when operations are attempted on a closed transaction.
	ErrTransactionClosed = errors.New("transaction is closed")

	// ErrReadOnlyTransaction is returned when a write operation is attempted on a read-only transaction.
	ErrReadOnlyTransaction = errors.New("cannot write in read-only transaction")

	// ErrCorruptedData is returned when data integrity check fails.
	ErrCorruptedData = errors.New("corrupted data detected")

	// ErrSnapshotInProgress is returned when a snapshot operation is already running.
	ErrSnapshotInProgress = errors.New("snapshot already in progress")

	// ErrInvalidTTL is returned when an invalid TTL duration is provided.
	ErrInvalidTTL = errors.New("invalid TTL duration")
)

// KeyError represents an error associated with a specific key.
type KeyError struct {
	Key string
	Err error
}

// Error implements the error interface.
func (e *KeyError) Error() string {
	return fmt.Sprintf("key %q: %v", e.Key, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *KeyError) Unwrap() error {
	return e.Err
}

// NewKeyError creates a new KeyError.
func NewKeyError(key string, err error) *KeyError {
	return &KeyError{Key: key, Err: err}
}

// BatchError represents errors that occurred during a batch operation.
type BatchError struct {
	Errors []error
}

// Error implements the error interface.
func (e *BatchError) Error() string {
	if len(e.Errors) == 0 {
		return "batch operation failed"
	}
	if len(e.Errors) == 1 {
		return fmt.Sprintf("batch operation failed: %v", e.Errors[0])
	}
	return fmt.Sprintf("batch operation failed with %d errors; first: %v", len(e.Errors), e.Errors[0])
}

// Unwrap returns the first error for errors.Is/As support.
func (e *BatchError) Unwrap() error {
	if len(e.Errors) > 0 {
		return e.Errors[0]
	}
	return nil
}

// Add appends an error to the batch error list.
func (e *BatchError) Add(err error) {
	if err != nil {
		e.Errors = append(e.Errors, err)
	}
}

// HasErrors returns true if there are any errors.
func (e *BatchError) HasErrors() bool {
	return len(e.Errors) > 0
}

// WALError represents an error that occurred during WAL operations.
type WALError struct {
	Op  string // Operation that failed (write, read, sync, etc.)
	Err error
}

// Error implements the error interface.
func (e *WALError) Error() string {
	return fmt.Sprintf("wal %s: %v", e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *WALError) Unwrap() error {
	return e.Err
}

// NewWALError creates a new WALError.
func NewWALError(op string, err error) *WALError {
	return &WALError{Op: op, Err: err}
}

// ValidationError represents a validation error.
type ValidationError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on %s: %s", e.Field, e.Message)
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}

// IsNotFound checks if the error is a key not found error.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsExpired checks if the error is a key expired error.
func IsExpired(err error) bool {
	return errors.Is(err, ErrKeyExpired)
}

// IsClosed checks if the error is a store closed error.
func IsClosed(err error) bool {
	return errors.Is(err, ErrStoreClosed)
}
