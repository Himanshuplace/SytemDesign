package kvstore

import (
	"sync"
	"time"
)

// OperationType defines the type of batch operation.
type OperationType int

const (
	// OpPut represents a put operation.
	OpPut OperationType = iota

	// OpDelete represents a delete operation.
	OpDelete

	// OpPutWithTTL represents a put operation with TTL.
	OpPutWithTTL
)

// String returns the string representation of the operation type.
func (op OperationType) String() string {
	switch op {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpPutWithTTL:
		return "put_with_ttl"
	default:
		return "unknown"
	}
}

// Operation represents a single operation in a batch.
type Operation struct {
	Type  OperationType
	Key   string
	Value []byte
	TTL   time.Duration
}

// Batch represents a collection of operations to be applied atomically.
type Batch struct {
	operations []Operation
	mu         sync.Mutex
	committed  bool
	size       int // Total size in bytes of all operations
}

// NewBatch creates a new empty batch.
func NewBatch() *Batch {
	return &Batch{
		operations: make([]Operation, 0, 16), // Pre-allocate for common case
	}
}

// NewBatchWithCapacity creates a new batch with pre-allocated capacity.
func NewBatchWithCapacity(capacity int) *Batch {
	if capacity <= 0 {
		capacity = 16
	}
	return &Batch{
		operations: make([]Operation, 0, capacity),
	}
}

// Put adds a put operation to the batch.
func (b *Batch) Put(key string, value []byte) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return b
	}

	// Make a copy of the value to avoid external mutations
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.operations = append(b.operations, Operation{
		Type:  OpPut,
		Key:   key,
		Value: valueCopy,
	})

	b.size += len(key) + len(value)
	return b
}

// PutWithTTL adds a put operation with TTL to the batch.
func (b *Batch) PutWithTTL(key string, value []byte, ttl time.Duration) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return b
	}

	// Make a copy of the value to avoid external mutations
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.operations = append(b.operations, Operation{
		Type:  OpPutWithTTL,
		Key:   key,
		Value: valueCopy,
		TTL:   ttl,
	})

	b.size += len(key) + len(value)
	return b
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key string) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return b
	}

	b.operations = append(b.operations, Operation{
		Type: OpDelete,
		Key:  key,
	})

	b.size += len(key)
	return b
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.operations)
}

// Size returns the total size of all operations in bytes.
func (b *Batch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// IsEmpty returns true if the batch has no operations.
func (b *Batch) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.operations) == 0
}

// IsCommitted returns true if the batch has been committed.
func (b *Batch) IsCommitted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committed
}

// Operations returns a copy of all operations in the batch.
func (b *Batch) Operations() []Operation {
	b.mu.Lock()
	defer b.mu.Unlock()

	ops := make([]Operation, len(b.operations))
	copy(ops, b.operations)
	return ops
}

// Reset clears all operations from the batch, allowing it to be reused.
func (b *Batch) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.operations = b.operations[:0]
	b.committed = false
	b.size = 0
}

// markCommitted marks the batch as committed.
func (b *Batch) markCommitted() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.committed = true
}

// BatchResult contains the results of a batch operation.
type BatchResult struct {
	Successful int           // Number of successful operations
	Failed     int           // Number of failed operations
	Errors     []error       // Errors that occurred (if any)
	Duration   time.Duration // Time taken to apply the batch
}

// HasErrors returns true if any errors occurred.
func (r *BatchResult) HasErrors() bool {
	return r.Failed > 0 || len(r.Errors) > 0
}

// BatchBuilder provides a fluent interface for building batches.
type BatchBuilder struct {
	batch *Batch
	err   error
}

// NewBatchBuilder creates a new batch builder.
func NewBatchBuilder() *BatchBuilder {
	return &BatchBuilder{
		batch: NewBatch(),
	}
}

// Put adds a put operation.
func (bb *BatchBuilder) Put(key string, value []byte) *BatchBuilder {
	if bb.err != nil {
		return bb
	}

	if key == "" {
		bb.err = ErrEmptyKey
		return bb
	}

	bb.batch.Put(key, value)
	return bb
}

// PutString adds a put operation with a string value.
func (bb *BatchBuilder) PutString(key, value string) *BatchBuilder {
	return bb.Put(key, []byte(value))
}

// PutWithTTL adds a put operation with TTL.
func (bb *BatchBuilder) PutWithTTL(key string, value []byte, ttl time.Duration) *BatchBuilder {
	if bb.err != nil {
		return bb
	}

	if key == "" {
		bb.err = ErrEmptyKey
		return bb
	}

	if ttl < 0 {
		bb.err = ErrInvalidTTL
		return bb
	}

	bb.batch.PutWithTTL(key, value, ttl)
	return bb
}

// Delete adds a delete operation.
func (bb *BatchBuilder) Delete(key string) *BatchBuilder {
	if bb.err != nil {
		return bb
	}

	if key == "" {
		bb.err = ErrEmptyKey
		return bb
	}

	bb.batch.Delete(key)
	return bb
}

// Build returns the constructed batch and any error that occurred.
func (bb *BatchBuilder) Build() (*Batch, error) {
	if bb.err != nil {
		return nil, bb.err
	}
	return bb.batch, nil
}

// MustBuild returns the constructed batch and panics if an error occurred.
func (bb *BatchBuilder) MustBuild() *Batch {
	batch, err := bb.Build()
	if err != nil {
		panic(err)
	}
	return batch
}

// BatchApplier defines the interface for applying batches.
type BatchApplier interface {
	// ApplyBatch applies all operations in the batch atomically.
	ApplyBatch(batch *Batch) (*BatchResult, error)
}

// BatchValidator validates a batch before it's applied.
type BatchValidator struct {
	maxKeySize   int
	maxValueSize int
	maxBatchSize int
	maxBatchOps  int
}

// NewBatchValidator creates a new batch validator with the given limits.
func NewBatchValidator(maxKeySize, maxValueSize, maxBatchSize, maxBatchOps int) *BatchValidator {
	return &BatchValidator{
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
		maxBatchSize: maxBatchSize,
		maxBatchOps:  maxBatchOps,
	}
}

// Validate validates a batch against the configured limits.
func (v *BatchValidator) Validate(batch *Batch) error {
	if batch == nil {
		return NewValidationError("batch", "batch cannot be nil")
	}

	batch.mu.Lock()
	defer batch.mu.Unlock()

	// Check operation count
	if v.maxBatchOps > 0 && len(batch.operations) > v.maxBatchOps {
		return NewValidationError("batch", "too many operations in batch")
	}

	// Check total batch size
	if v.maxBatchSize > 0 && batch.size > v.maxBatchSize {
		return NewValidationError("batch", "batch size exceeds limit")
	}

	// Validate each operation
	for _, op := range batch.operations {
		if op.Key == "" {
			return ErrEmptyKey
		}

		if v.maxKeySize > 0 && len(op.Key) > v.maxKeySize {
			return NewKeyError(op.Key, NewValidationError("key", "key size exceeds limit"))
		}

		if op.Type == OpPut || op.Type == OpPutWithTTL {
			if v.maxValueSize > 0 && len(op.Value) > v.maxValueSize {
				return NewKeyError(op.Key, NewValidationError("value", "value size exceeds limit"))
			}
		}

		if op.Type == OpPutWithTTL && op.TTL < 0 {
			return NewKeyError(op.Key, ErrInvalidTTL)
		}
	}

	return nil
}

// BatchStats provides statistics about a batch.
type BatchStats struct {
	OperationCount   int
	PutCount         int
	DeleteCount      int
	TotalSize        int
	AverageKeySize   float64
	AverageValueSize float64
}

// Stats calculates statistics for a batch.
func (b *Batch) Stats() BatchStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	stats := BatchStats{
		OperationCount: len(b.operations),
		TotalSize:      b.size,
	}

	var totalKeySize, totalValueSize int

	for _, op := range b.operations {
		totalKeySize += len(op.Key)

		switch op.Type {
		case OpPut, OpPutWithTTL:
			stats.PutCount++
			totalValueSize += len(op.Value)
		case OpDelete:
			stats.DeleteCount++
		}
	}

	if stats.OperationCount > 0 {
		stats.AverageKeySize = float64(totalKeySize) / float64(stats.OperationCount)
	}

	if stats.PutCount > 0 {
		stats.AverageValueSize = float64(totalValueSize) / float64(stats.PutCount)
	}

	return stats
}

// Clone creates a deep copy of the batch.
func (b *Batch) Clone() *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	clone := &Batch{
		operations: make([]Operation, len(b.operations)),
		committed:  b.committed,
		size:       b.size,
	}

	for i, op := range b.operations {
		clone.operations[i] = Operation{
			Type: op.Type,
			Key:  op.Key,
			TTL:  op.TTL,
		}
		if len(op.Value) > 0 {
			clone.operations[i].Value = make([]byte, len(op.Value))
			copy(clone.operations[i].Value, op.Value)
		}
	}

	return clone
}

// Merge combines two batches into a new batch.
// The operations from other are appended after the operations from b.
func (b *Batch) Merge(other *Batch) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	other.mu.Lock()
	defer other.mu.Unlock()

	merged := &Batch{
		operations: make([]Operation, 0, len(b.operations)+len(other.operations)),
		size:       b.size + other.size,
	}

	// Copy operations from b
	for _, op := range b.operations {
		newOp := Operation{
			Type: op.Type,
			Key:  op.Key,
			TTL:  op.TTL,
		}
		if len(op.Value) > 0 {
			newOp.Value = make([]byte, len(op.Value))
			copy(newOp.Value, op.Value)
		}
		merged.operations = append(merged.operations, newOp)
	}

	// Copy operations from other
	for _, op := range other.operations {
		newOp := Operation{
			Type: op.Type,
			Key:  op.Key,
			TTL:  op.TTL,
		}
		if len(op.Value) > 0 {
			newOp.Value = make([]byte, len(op.Value))
			copy(newOp.Value, op.Value)
		}
		merged.operations = append(merged.operations, newOp)
	}

	return merged
}

// Filter returns a new batch containing only operations that match the predicate.
func (b *Batch) Filter(predicate func(Operation) bool) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()

	filtered := NewBatch()

	for _, op := range b.operations {
		if predicate(op) {
			switch op.Type {
			case OpPut:
				filtered.Put(op.Key, op.Value)
			case OpPutWithTTL:
				filtered.PutWithTTL(op.Key, op.Value, op.TTL)
			case OpDelete:
				filtered.Delete(op.Key)
			}
		}
	}

	return filtered
}
