// Package dlq provides Dead Letter Queue functionality for failed messages.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/message"
	"github.com/messagequeue/internal/storage"
)

// Common errors returned by DLQ operations.
var (
	ErrDLQClosed     = errors.New("dead letter queue is closed")
	ErrDLQFull       = errors.New("dead letter queue is full")
	ErrMessageNil    = errors.New("message cannot be nil")
	ErrNotFound      = errors.New("message not found in DLQ")
	ErrAlreadyExists = errors.New("message already exists in DLQ")
)

// Reason represents the reason a message was dead-lettered.
type Reason string

const (
	ReasonMaxRetries   Reason = "max_retries_exceeded"
	ReasonAckTimeout   Reason = "ack_timeout"
	ReasonRejected     Reason = "rejected"
	ReasonExpired      Reason = "expired"
	ReasonInvalidMsg   Reason = "invalid_message"
	ReasonProcessError Reason = "processing_error"
	ReasonUnknown      Reason = "unknown"
)

// Config holds configuration for a Dead Letter Queue.
type Config struct {
	// Name is the name of the DLQ
	Name string

	// MaxSize is the maximum number of messages the DLQ can hold (0 = unlimited)
	MaxSize int

	// RetentionPeriod is how long to keep messages in the DLQ
	RetentionPeriod time.Duration

	// EnableMetrics enables metrics collection
	EnableMetrics bool
}

// DefaultConfig returns a default DLQ configuration.
func DefaultConfig(name string) Config {
	return Config{
		Name:            name,
		MaxSize:         100000,
		RetentionPeriod: 7 * 24 * time.Hour, // 7 days
		EnableMetrics:   true,
	}
}

// DeadLetter represents a dead-lettered message with metadata.
type DeadLetter struct {
	Message       *message.Message
	Reason        Reason
	ReasonDetail  string
	OriginalQueue string
	OriginalTopic string
	DeadAt        time.Time
	DeliveryCount int
	LastError     string
}

// Stats holds statistics about the DLQ.
type Stats struct {
	Name             string
	Size             int64
	TotalReceived    int64
	TotalReprocessed int64
	TotalDiscarded   int64
	ByReason         map[Reason]int64
	OldestMessage    time.Time
	NewestMessage    time.Time
}

// DeadLetterQueue handles messages that failed processing.
type DeadLetterQueue struct {
	mu sync.RWMutex

	config  Config
	storage storage.Storage

	// Message management
	messages map[string]*DeadLetter
	order    []string // Maintains insertion order

	// Stats
	totalReceived    int64
	totalReprocessed int64
	totalDiscarded   int64
	byReason         map[Reason]*int64

	// Callbacks
	onMessageAdded   func(dl *DeadLetter)
	onMessageRemoved func(msgID string, reason string)

	// Lifecycle
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// New creates a new Dead Letter Queue.
func New(cfg Config) *DeadLetterQueue {
	dlq := &DeadLetterQueue{
		config:   cfg,
		storage:  storage.NewMemoryStorage(storage.DefaultMemoryConfig()),
		messages: make(map[string]*DeadLetter),
		order:    make([]string, 0),
		byReason: make(map[Reason]*int64),
		closeCh:  make(chan struct{}),
	}

	// Initialize reason counters
	reasons := []Reason{
		ReasonMaxRetries, ReasonAckTimeout, ReasonRejected,
		ReasonExpired, ReasonInvalidMsg, ReasonProcessError, ReasonUnknown,
	}
	for _, r := range reasons {
		counter := int64(0)
		dlq.byReason[r] = &counter
	}

	// Start cleanup worker
	dlq.wg.Add(1)
	go dlq.cleanupWorker()

	return dlq
}

// Name returns the DLQ name.
func (d *DeadLetterQueue) Name() string {
	return d.config.Name
}

// Add adds a message to the dead letter queue.
func (d *DeadLetterQueue) Add(ctx context.Context, msg *message.Message, reason Reason, detail string) error {
	if msg == nil {
		return ErrMessageNil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	// Check capacity
	if d.config.MaxSize > 0 && len(d.messages) >= d.config.MaxSize {
		return ErrDLQFull
	}

	// Check if already exists
	if _, exists := d.messages[msg.ID()]; exists {
		return ErrAlreadyExists
	}

	// Create dead letter entry
	dl := &DeadLetter{
		Message:       msg,
		Reason:        reason,
		ReasonDetail:  detail,
		OriginalQueue: msg.QueueName(),
		OriginalTopic: msg.TopicName(),
		DeadAt:        time.Now(),
		DeliveryCount: msg.DeliveryCount(),
		LastError:     msg.Header("x-last-error"),
	}

	// Update message state and headers
	msg.SetState(message.StateDeadLettered)
	msg.SetHeader("x-dlq-reason", string(reason))
	msg.SetHeader("x-dlq-reason-detail", detail)
	msg.SetHeader("x-dlq-time", dl.DeadAt.Format(time.RFC3339))
	msg.SetHeader("x-dlq-name", d.config.Name)

	// Store
	d.messages[msg.ID()] = dl
	d.order = append(d.order, msg.ID())

	// Update stats
	atomic.AddInt64(&d.totalReceived, 1)
	if counter, ok := d.byReason[reason]; ok {
		atomic.AddInt64(counter, 1)
	}

	// Store in storage backend
	data := &storage.MessageData{
		ID:        msg.ID(),
		Payload:   msg.Payload(),
		Headers:   msg.Headers(),
		Priority:  msg.Priority(),
		Timestamp: msg.Timestamp(),
	}
	d.storage.Store(data)

	// Callback
	if d.onMessageAdded != nil {
		go d.onMessageAdded(dl)
	}

	return nil
}

// AddFromQueue adds a message from a queue to the DLQ.
func (d *DeadLetterQueue) AddFromQueue(ctx context.Context, msg *message.Message, queueName string, reason Reason, detail string) error {
	msg.SetQueueName(queueName)
	return d.Add(ctx, msg, reason, detail)
}

// Get retrieves a dead letter by message ID.
func (d *DeadLetterQueue) Get(msgID string) (*DeadLetter, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrDLQClosed
	}

	dl, exists := d.messages[msgID]
	if !exists {
		return nil, ErrNotFound
	}

	return dl, nil
}

// Remove removes a message from the DLQ.
func (d *DeadLetterQueue) Remove(msgID string, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	if _, exists := d.messages[msgID]; !exists {
		return ErrNotFound
	}

	delete(d.messages, msgID)

	// Remove from order
	for i, id := range d.order {
		if id == msgID {
			d.order = append(d.order[:i], d.order[i+1:]...)
			break
		}
	}

	// Remove from storage
	d.storage.Delete(msgID)

	// Callback
	if d.onMessageRemoved != nil {
		go d.onMessageRemoved(msgID, reason)
	}

	return nil
}

// Reprocess retrieves and removes a message for reprocessing.
// Returns the message for republishing to the original queue.
func (d *DeadLetterQueue) Reprocess(msgID string) (*message.Message, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, "", ErrDLQClosed
	}

	dl, exists := d.messages[msgID]
	if !exists {
		return nil, "", ErrNotFound
	}

	// Clone the message for reprocessing
	msg := dl.Message.Clone()
	msg.SetState(message.StatePending)
	msg.SetHeader("x-reprocessed-from-dlq", d.config.Name)
	msg.SetHeader("x-reprocessed-at", time.Now().Format(time.RFC3339))

	originalQueue := dl.OriginalQueue

	// Remove from DLQ
	delete(d.messages, msgID)
	for i, id := range d.order {
		if id == msgID {
			d.order = append(d.order[:i], d.order[i+1:]...)
			break
		}
	}
	d.storage.Delete(msgID)

	atomic.AddInt64(&d.totalReprocessed, 1)

	return msg, originalQueue, nil
}

// ReprocessAll reprocesses all messages in the DLQ.
// Returns messages grouped by their original queue.
func (d *DeadLetterQueue) ReprocessAll() (map[string][]*message.Message, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDLQClosed
	}

	result := make(map[string][]*message.Message)

	for _, dl := range d.messages {
		msg := dl.Message.Clone()
		msg.SetState(message.StatePending)
		msg.SetHeader("x-reprocessed-from-dlq", d.config.Name)
		msg.SetHeader("x-reprocessed-at", time.Now().Format(time.RFC3339))

		queue := dl.OriginalQueue
		if queue == "" {
			queue = "_unknown"
		}

		result[queue] = append(result[queue], msg)
	}

	count := int64(len(d.messages))

	// Clear DLQ
	d.messages = make(map[string]*DeadLetter)
	d.order = make([]string, 0)
	d.storage.Clear()

	atomic.AddInt64(&d.totalReprocessed, count)

	return result, nil
}

// Discard removes a message permanently without reprocessing.
func (d *DeadLetterQueue) Discard(msgID string) error {
	err := d.Remove(msgID, "discarded")
	if err == nil {
		atomic.AddInt64(&d.totalDiscarded, 1)
	}
	return err
}

// DiscardAll removes all messages from the DLQ.
func (d *DeadLetterQueue) DiscardAll() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDLQClosed
	}

	count := len(d.messages)

	d.messages = make(map[string]*DeadLetter)
	d.order = make([]string, 0)
	d.storage.Clear()

	atomic.AddInt64(&d.totalDiscarded, int64(count))

	return count, nil
}

// List returns all dead letters in the DLQ.
func (d *DeadLetterQueue) List() []*DeadLetter {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]*DeadLetter, 0, len(d.order))
	for _, id := range d.order {
		if dl, exists := d.messages[id]; exists {
			result = append(result, dl)
		}
	}

	return result
}

// ListByReason returns dead letters filtered by reason.
func (d *DeadLetterQueue) ListByReason(reason Reason) []*DeadLetter {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []*DeadLetter
	for _, id := range d.order {
		if dl, exists := d.messages[id]; exists && dl.Reason == reason {
			result = append(result, dl)
		}
	}

	return result
}

// ListByQueue returns dead letters from a specific original queue.
func (d *DeadLetterQueue) ListByQueue(queueName string) []*DeadLetter {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []*DeadLetter
	for _, id := range d.order {
		if dl, exists := d.messages[id]; exists && dl.OriginalQueue == queueName {
			result = append(result, dl)
		}
	}

	return result
}

// Size returns the number of messages in the DLQ.
func (d *DeadLetterQueue) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.messages)
}

// Stats returns statistics about the DLQ.
func (d *DeadLetterQueue) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := Stats{
		Name:             d.config.Name,
		Size:             int64(len(d.messages)),
		TotalReceived:    atomic.LoadInt64(&d.totalReceived),
		TotalReprocessed: atomic.LoadInt64(&d.totalReprocessed),
		TotalDiscarded:   atomic.LoadInt64(&d.totalDiscarded),
		ByReason:         make(map[Reason]int64),
	}

	for reason, counter := range d.byReason {
		stats.ByReason[reason] = atomic.LoadInt64(counter)
	}

	// Find oldest and newest
	if len(d.order) > 0 {
		if oldest, exists := d.messages[d.order[0]]; exists {
			stats.OldestMessage = oldest.DeadAt
		}
		if newest, exists := d.messages[d.order[len(d.order)-1]]; exists {
			stats.NewestMessage = newest.DeadAt
		}
	}

	return stats
}

// OnMessageAdded sets a callback for when a message is added to the DLQ.
func (d *DeadLetterQueue) OnMessageAdded(fn func(dl *DeadLetter)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onMessageAdded = fn
}

// OnMessageRemoved sets a callback for when a message is removed from the DLQ.
func (d *DeadLetterQueue) OnMessageRemoved(fn func(msgID string, reason string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onMessageRemoved = fn
}

// Close closes the DLQ and releases resources.
func (d *DeadLetterQueue) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDLQClosed
	}
	d.closed = true
	close(d.closeCh)
	d.mu.Unlock()

	// Wait for workers
	d.wg.Wait()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.storage.Close()
	d.messages = nil
	d.order = nil

	return nil
}

// IsClosed returns whether the DLQ is closed.
func (d *DeadLetterQueue) IsClosed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.closed
}

// cleanupWorker periodically removes expired messages from the DLQ.
func (d *DeadLetterQueue) cleanupWorker() {
	defer d.wg.Done()

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-d.closeCh:
			return
		case <-ticker.C:
			d.cleanup()
		}
	}
}

// cleanup removes messages that have exceeded the retention period.
func (d *DeadLetterQueue) cleanup() {
	if d.config.RetentionPeriod <= 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}

	cutoff := time.Now().Add(-d.config.RetentionPeriod)
	var toRemove []string

	for id, dl := range d.messages {
		if dl.DeadAt.Before(cutoff) {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		delete(d.messages, id)
		d.storage.Delete(id)

		// Remove from order
		for i, orderedID := range d.order {
			if orderedID == id {
				d.order = append(d.order[:i], d.order[i+1:]...)
				break
			}
		}

		atomic.AddInt64(&d.totalDiscarded, 1)
	}
}

// String returns a string representation of a DeadLetter.
func (dl *DeadLetter) String() string {
	return fmt.Sprintf("DeadLetter{ID=%s, Reason=%s, Queue=%s, DeadAt=%s}",
		dl.Message.ID(), dl.Reason, dl.OriginalQueue, dl.DeadAt.Format(time.RFC3339))
}
