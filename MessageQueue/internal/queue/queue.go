// Package queue provides the Queue implementation for point-to-point messaging.
package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/message"
	"github.com/messagequeue/internal/storage"
)

// Common errors returned by Queue operations.
var (
	ErrQueueClosed      = errors.New("queue is closed")
	ErrQueueFull        = errors.New("queue is full")
	ErrMessageNil       = errors.New("message cannot be nil")
	ErrConsumerExists   = errors.New("consumer already exists")
	ErrConsumerNotFound = errors.New("consumer not found")
	ErrNoConsumers      = errors.New("no consumers available")
	ErrAckTimeout       = errors.New("acknowledgment timeout")
)

// Config holds configuration for a Queue.
type Config struct {
	// Name of the queue
	Name string

	// MaxSize is the maximum number of messages the queue can hold (0 = unlimited)
	MaxSize int

	// DefaultTTL is the default time-to-live for messages
	DefaultTTL time.Duration

	// AckTimeout is the time to wait for acknowledgment before redelivery
	AckTimeout time.Duration

	// MaxRetries is the maximum number of delivery attempts before dead-lettering
	MaxRetries int

	// DeadLetterQueue is the name of the dead letter queue (empty = no DLQ)
	DeadLetterQueue string

	// EnablePriority enables priority-based message ordering
	EnablePriority bool

	// PrefetchCount is the number of messages to prefetch per consumer
	PrefetchCount int
}

// DefaultConfig returns a default Queue configuration.
func DefaultConfig(name string) Config {
	return Config{
		Name:           name,
		MaxSize:        100000,
		DefaultTTL:     24 * time.Hour,
		AckTimeout:     30 * time.Second,
		MaxRetries:     3,
		EnablePriority: true,
		PrefetchCount:  10,
	}
}

// Stats holds statistics about a queue.
type Stats struct {
	Name              string
	Depth             int64
	EnqueueCount      int64
	DequeueCount      int64
	AckCount          int64
	NackCount         int64
	ExpiredCount      int64
	DeadLetteredCount int64
	ConsumerCount     int
	CreatedAt         time.Time
}

// Queue represents a point-to-point message queue.
type Queue struct {
	mu sync.RWMutex

	config  Config
	storage storage.Storage

	// Message management
	pending *priorityQueue             // Messages waiting to be delivered
	unacked map[string]*unackedMessage // Messages delivered but not acknowledged
	dedup   map[string]time.Time       // Deduplication cache

	// Consumer management
	consumers    map[string]*Consumer
	consumerList []*Consumer
	nextConsumer int

	// Stats
	enqueueCount      int64
	dequeueCount      int64
	ackCount          int64
	nackCount         int64
	expiredCount      int64
	deadLetteredCount int64

	// Lifecycle
	createdAt time.Time
	closed    bool
	closeCh   chan struct{}
	wg        sync.WaitGroup

	// Dead letter queue reference (set by broker)
	dlq *Queue
}

// unackedMessage tracks an unacknowledged message.
type unackedMessage struct {
	msg         *message.Message
	deliveredAt time.Time
	consumerID  string
	timer       *time.Timer
}

// Consumer represents a queue consumer.
type Consumer struct {
	mu sync.RWMutex

	id       string
	queue    *Queue
	messages chan *message.Message
	closed   bool
	closeCh  chan struct{}

	// Stats
	deliveredCount int64
	ackedCount     int64
	nackedCount    int64
}

// NewConsumer creates a new consumer for a queue.
func newConsumer(id string, q *Queue, bufferSize int) *Consumer {
	return &Consumer{
		id:       id,
		queue:    q,
		messages: make(chan *message.Message, bufferSize),
		closeCh:  make(chan struct{}),
	}
}

// ID returns the consumer's identifier.
func (c *Consumer) ID() string {
	return c.id
}

// Messages returns the channel for receiving messages.
func (c *Consumer) Messages() <-chan *message.Message {
	return c.messages
}

// Close closes the consumer and stops receiving messages.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	close(c.closeCh)
	close(c.messages)

	return c.queue.removeConsumer(c.id)
}

// IsClosed returns whether the consumer is closed.
func (c *Consumer) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// Stats returns consumer statistics.
func (c *Consumer) Stats() (delivered, acked, nacked int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.deliveredCount, c.ackedCount, c.nackedCount
}

// priorityQueue implements heap.Interface for priority-based message ordering.
type priorityQueue []*message.Message

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// Higher priority first
	if pq[i].Priority() != pq[j].Priority() {
		return pq[i].Priority() > pq[j].Priority()
	}
	// Same priority: FIFO by timestamp
	return pq[i].Timestamp().Before(pq[j].Timestamp())
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*message.Message))
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	msg := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return msg
}

// New creates a new Queue with the given configuration.
func New(cfg Config) *Queue {
	q := &Queue{
		config:    cfg,
		storage:   storage.NewMemoryStorage(storage.DefaultMemoryConfig()),
		pending:   &priorityQueue{},
		unacked:   make(map[string]*unackedMessage),
		dedup:     make(map[string]time.Time),
		consumers: make(map[string]*Consumer),
		createdAt: time.Now(),
		closeCh:   make(chan struct{}),
	}

	heap.Init(q.pending)

	// Start background workers
	q.wg.Add(2)
	go q.expirationWorker()
	go q.redeliveryWorker()

	return q
}

// NewWithStorage creates a new Queue with custom storage.
func NewWithStorage(cfg Config, store storage.Storage) *Queue {
	q := New(cfg)
	q.storage = store
	return q
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.config.Name
}

// Config returns the queue configuration.
func (q *Queue) Config() Config {
	return q.config
}

// SetDLQ sets the dead letter queue.
func (q *Queue) SetDLQ(dlq *Queue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dlq = dlq
}

// Publish adds a message to the queue.
func (q *Queue) Publish(ctx context.Context, msg *message.Message) error {
	if msg == nil {
		return ErrMessageNil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	// Check queue capacity
	if q.config.MaxSize > 0 && q.pending.Len() >= q.config.MaxSize {
		return ErrQueueFull
	}

	// Handle deduplication
	if dedupID := msg.DeduplicationID(); dedupID != "" {
		if _, exists := q.dedup[dedupID]; exists {
			// Message already processed, silently ignore
			return nil
		}
		q.dedup[dedupID] = time.Now()
	}

	// Apply default TTL if not set
	if msg.TTL() == 0 && q.config.DefaultTTL > 0 {
		msg.SetTTL(q.config.DefaultTTL)
	}

	// Set queue name
	msg.SetQueueName(q.config.Name)

	// Store in storage
	if err := q.storeMessage(msg); err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}

	// Add to pending queue
	heap.Push(q.pending, msg)
	atomic.AddInt64(&q.enqueueCount, 1)

	// Try to dispatch immediately
	q.dispatchMessages()

	return nil
}

// PublishBatch adds multiple messages to the queue atomically.
func (q *Queue) PublishBatch(ctx context.Context, msgs []*message.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	// Check capacity
	if q.config.MaxSize > 0 && q.pending.Len()+len(msgs) > q.config.MaxSize {
		return ErrQueueFull
	}

	for _, msg := range msgs {
		if msg == nil {
			continue
		}

		// Handle deduplication
		if dedupID := msg.DeduplicationID(); dedupID != "" {
			if _, exists := q.dedup[dedupID]; exists {
				continue
			}
			q.dedup[dedupID] = time.Now()
		}

		// Apply default TTL
		if msg.TTL() == 0 && q.config.DefaultTTL > 0 {
			msg.SetTTL(q.config.DefaultTTL)
		}

		msg.SetQueueName(q.config.Name)

		if err := q.storeMessage(msg); err != nil {
			return fmt.Errorf("failed to store message %s: %w", msg.ID(), err)
		}

		heap.Push(q.pending, msg)
		atomic.AddInt64(&q.enqueueCount, 1)
	}

	q.dispatchMessages()

	return nil
}

// Subscribe creates a new consumer for this queue.
func (q *Queue) Subscribe(consumerID string) (*Consumer, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	if _, exists := q.consumers[consumerID]; exists {
		return nil, ErrConsumerExists
	}

	consumer := newConsumer(consumerID, q, q.config.PrefetchCount)
	q.consumers[consumerID] = consumer
	q.consumerList = append(q.consumerList, consumer)

	// Dispatch pending messages to the new consumer
	q.dispatchMessages()

	return consumer, nil
}

// removeConsumer removes a consumer from the queue.
func (q *Queue) removeConsumer(consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	consumer, exists := q.consumers[consumerID]
	if !exists {
		return ErrConsumerNotFound
	}

	delete(q.consumers, consumerID)

	// Remove from list
	for i, c := range q.consumerList {
		if c == consumer {
			q.consumerList = append(q.consumerList[:i], q.consumerList[i+1:]...)
			break
		}
	}

	// Requeue any unacked messages from this consumer
	for id, unacked := range q.unacked {
		if unacked.consumerID == consumerID {
			if unacked.timer != nil {
				unacked.timer.Stop()
			}
			unacked.msg.SetState(message.StatePending)
			heap.Push(q.pending, unacked.msg)
			delete(q.unacked, id)
		}
	}

	return nil
}

// Ack acknowledges a message, removing it from the queue.
func (q *Queue) Ack(msgID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	unacked, exists := q.unacked[msgID]
	if !exists {
		return fmt.Errorf("message %s not found in unacked messages", msgID)
	}

	if unacked.timer != nil {
		unacked.timer.Stop()
	}

	// Remove from storage
	if err := q.storage.Delete(msgID); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("failed to delete message from storage: %w", err)
	}

	delete(q.unacked, msgID)
	atomic.AddInt64(&q.ackCount, 1)

	// Update consumer stats
	if consumer, ok := q.consumers[unacked.consumerID]; ok {
		consumer.mu.Lock()
		consumer.ackedCount++
		consumer.mu.Unlock()
	}

	return nil
}

// Nack negatively acknowledges a message.
func (q *Queue) Nack(msgID string, requeue bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	unacked, exists := q.unacked[msgID]
	if !exists {
		return fmt.Errorf("message %s not found in unacked messages", msgID)
	}

	if unacked.timer != nil {
		unacked.timer.Stop()
	}

	delete(q.unacked, msgID)
	atomic.AddInt64(&q.nackCount, 1)

	// Update consumer stats
	if consumer, ok := q.consumers[unacked.consumerID]; ok {
		consumer.mu.Lock()
		consumer.nackedCount++
		consumer.mu.Unlock()
	}

	msg := unacked.msg
	deliveryCount := msg.DeliveryCount()

	if !requeue {
		// Move to DLQ if configured
		q.moveToDeadLetter(msg, "rejected by consumer")
		return nil
	}

	// Check retry limit
	if q.config.MaxRetries > 0 && deliveryCount >= q.config.MaxRetries {
		q.moveToDeadLetter(msg, fmt.Sprintf("max retries exceeded (%d)", q.config.MaxRetries))
		return nil
	}

	// Requeue for redelivery
	msg.SetState(message.StatePending)
	heap.Push(q.pending, msg)

	// Try to dispatch
	q.dispatchMessages()

	return nil
}

// Depth returns the number of pending messages in the queue.
func (q *Queue) Depth() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return int64(q.pending.Len())
}

// UnackedCount returns the number of unacknowledged messages.
func (q *Queue) UnackedCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.unacked)
}

// ConsumerCount returns the number of active consumers.
func (q *Queue) ConsumerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.consumers)
}

// Stats returns queue statistics.
func (q *Queue) Stats() Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return Stats{
		Name:              q.config.Name,
		Depth:             int64(q.pending.Len()),
		EnqueueCount:      atomic.LoadInt64(&q.enqueueCount),
		DequeueCount:      atomic.LoadInt64(&q.dequeueCount),
		AckCount:          atomic.LoadInt64(&q.ackCount),
		NackCount:         atomic.LoadInt64(&q.nackCount),
		ExpiredCount:      atomic.LoadInt64(&q.expiredCount),
		DeadLetteredCount: atomic.LoadInt64(&q.deadLetteredCount),
		ConsumerCount:     len(q.consumers),
		CreatedAt:         q.createdAt,
	}
}

// Purge removes all pending messages from the queue.
func (q *Queue) Purge() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	// Clear pending messages
	for q.pending.Len() > 0 {
		msg := heap.Pop(q.pending).(*message.Message)
		q.storage.Delete(msg.ID())
	}

	return nil
}

// Close closes the queue and releases resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	q.closed = true
	close(q.closeCh)
	q.mu.Unlock()

	// Wait for background workers
	q.wg.Wait()

	q.mu.Lock()
	defer q.mu.Unlock()

	// Close all consumers
	for _, consumer := range q.consumers {
		consumer.Close()
	}

	// Cancel all ack timers
	for _, unacked := range q.unacked {
		if unacked.timer != nil {
			unacked.timer.Stop()
		}
	}

	// Close storage
	if q.storage != nil {
		q.storage.Close()
	}

	return nil
}

// IsClosed returns whether the queue is closed.
func (q *Queue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// storeMessage stores a message in the storage backend.
func (q *Queue) storeMessage(msg *message.Message) error {
	data := &storage.MessageData{
		ID:              msg.ID(),
		Payload:         msg.Payload(),
		Headers:         msg.Headers(),
		Priority:        msg.Priority(),
		Timestamp:       msg.Timestamp(),
		TTL:             msg.TTL(),
		DeliveryTime:    msg.DeliverAt(),
		DeduplicationID: msg.DeduplicationID(),
	}
	return q.storage.Store(data)
}

// dispatchMessages dispatches pending messages to available consumers.
// Caller must hold the mutex.
func (q *Queue) dispatchMessages() {
	if len(q.consumerList) == 0 {
		return
	}

	for q.pending.Len() > 0 {
		// Get the next message without removing it yet
		msg := (*q.pending)[0]

		// Check if message is ready for delivery (delayed messages)
		if !msg.IsReady() {
			break
		}

		// Check if message has expired
		if msg.IsExpired() {
			heap.Pop(q.pending)
			q.storage.Delete(msg.ID())
			atomic.AddInt64(&q.expiredCount, 1)
			continue
		}

		// Find an available consumer (round-robin)
		delivered := false
		startIdx := q.nextConsumer
		for {
			consumer := q.consumerList[q.nextConsumer]
			q.nextConsumer = (q.nextConsumer + 1) % len(q.consumerList)

			if !consumer.IsClosed() {
				// Try to send to consumer
				select {
				case consumer.messages <- msg:
					// Successfully sent
					heap.Pop(q.pending)
					q.deliverMessage(msg, consumer)
					delivered = true
				default:
					// Consumer buffer is full, try next
				}
			}

			if delivered || q.nextConsumer == startIdx {
				break
			}
		}

		if !delivered {
			// No consumer available, stop dispatching
			break
		}
	}
}

// deliverMessage marks a message as delivered and sets up ack timeout.
func (q *Queue) deliverMessage(msg *message.Message, consumer *Consumer) {
	msg.MarkDelivered()
	atomic.AddInt64(&q.dequeueCount, 1)

	consumer.mu.Lock()
	consumer.deliveredCount++
	consumer.mu.Unlock()

	// Setup acknowledgment callbacks
	msg.SetAckFunc(func() error {
		return q.Ack(msg.ID())
	})
	msg.SetNackFunc(func(requeue bool) error {
		return q.Nack(msg.ID(), requeue)
	})

	// Track unacked message
	unacked := &unackedMessage{
		msg:         msg,
		deliveredAt: time.Now(),
		consumerID:  consumer.id,
	}

	// Setup ack timeout
	if q.config.AckTimeout > 0 {
		unacked.timer = time.AfterFunc(q.config.AckTimeout, func() {
			q.handleAckTimeout(msg.ID())
		})
	}

	q.unacked[msg.ID()] = unacked
}

// handleAckTimeout handles acknowledgment timeout for a message.
func (q *Queue) handleAckTimeout(msgID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	unacked, exists := q.unacked[msgID]
	if !exists {
		return
	}

	delete(q.unacked, msgID)

	msg := unacked.msg
	deliveryCount := msg.DeliveryCount()

	// Check retry limit
	if q.config.MaxRetries > 0 && deliveryCount >= q.config.MaxRetries {
		q.moveToDeadLetter(msg, fmt.Sprintf("ack timeout after %d attempts", deliveryCount))
		return
	}

	// Requeue for redelivery
	msg.SetState(message.StatePending)
	heap.Push(q.pending, msg)

	// Try to dispatch
	q.dispatchMessages()
}

// moveToDeadLetter moves a message to the dead letter queue.
// Caller must hold the mutex.
func (q *Queue) moveToDeadLetter(msg *message.Message, reason string) {
	atomic.AddInt64(&q.deadLetteredCount, 1)

	// Remove from storage
	q.storage.Delete(msg.ID())

	if q.dlq == nil {
		return
	}

	// Clone and add error headers
	dlqMsg := msg.Clone()
	dlqMsg.SetHeader("x-death-reason", reason)
	dlqMsg.SetHeader("x-original-queue", q.config.Name)
	dlqMsg.SetHeader("x-death-time", time.Now().Format(time.RFC3339))
	dlqMsg.SetHeader("x-delivery-count", fmt.Sprintf("%d", msg.DeliveryCount()))
	dlqMsg.SetState(message.StateDeadLettered)

	// Publish to DLQ (without holding lock to avoid deadlock)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		q.dlq.Publish(ctx, dlqMsg)
	}()
}

// expirationWorker periodically removes expired messages.
func (q *Queue) expirationWorker() {
	defer q.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-q.closeCh:
			return
		case <-ticker.C:
			q.removeExpiredMessages()
		}
	}
}

// removeExpiredMessages removes expired messages from the queue.
func (q *Queue) removeExpiredMessages() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	// Check pending messages
	newPending := &priorityQueue{}
	heap.Init(newPending)

	for q.pending.Len() > 0 {
		msg := heap.Pop(q.pending).(*message.Message)
		if msg.IsExpired() {
			q.storage.Delete(msg.ID())
			atomic.AddInt64(&q.expiredCount, 1)
		} else {
			heap.Push(newPending, msg)
		}
	}

	q.pending = newPending

	// Clean up deduplication cache (remove entries older than 24 hours)
	cutoff := time.Now().Add(-24 * time.Hour)
	for id, t := range q.dedup {
		if t.Before(cutoff) {
			delete(q.dedup, id)
		}
	}
}

// redeliveryWorker handles redelivery of delayed messages.
func (q *Queue) redeliveryWorker() {
	defer q.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-q.closeCh:
			return
		case <-ticker.C:
			q.mu.Lock()
			if !q.closed {
				q.dispatchMessages()
			}
			q.mu.Unlock()
		}
	}
}
