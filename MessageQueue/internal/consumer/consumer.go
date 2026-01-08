// Package consumer provides consumer and consumer group implementations for the message queue.
package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/message"
)

// Common errors returned by consumer operations.
var (
	ErrConsumerClosed    = errors.New("consumer is closed")
	ErrConsumerExists    = errors.New("consumer already exists in group")
	ErrConsumerNotFound  = errors.New("consumer not found in group")
	ErrGroupClosed       = errors.New("consumer group is closed")
	ErrNoConsumers       = errors.New("no consumers in group")
	ErrAckTimeout        = errors.New("acknowledgment timeout")
	ErrMaxRetriesReached = errors.New("maximum retries reached")
)

// Config holds configuration for a consumer.
type Config struct {
	// BufferSize is the size of the message channel buffer.
	BufferSize int

	// AckTimeout is the maximum time to wait for acknowledgment.
	AckTimeout time.Duration

	// MaxRetries is the maximum number of delivery attempts.
	MaxRetries int

	// AutoAck enables automatic acknowledgment on receive.
	AutoAck bool

	// PrefetchCount is the number of messages to prefetch.
	PrefetchCount int
}

// DefaultConfig returns a default consumer configuration.
func DefaultConfig() Config {
	return Config{
		BufferSize:    100,
		AckTimeout:    30 * time.Second,
		MaxRetries:    3,
		AutoAck:       false,
		PrefetchCount: 10,
	}
}

// Consumer represents a message consumer that receives messages from a queue or topic.
type Consumer struct {
	mu sync.RWMutex

	// Identification
	id        string
	queueName string
	groupID   string

	// Configuration
	config Config

	// Message channels
	messages    chan *message.Message
	pending     map[string]*message.Message // Messages awaiting acknowledgment
	pendingLock sync.RWMutex

	// State
	closed   atomic.Bool
	paused   atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// Callbacks
	onAck     func(msgID string)
	onNack    func(msgID string, requeue bool)
	onReject  func(msgID string)
	onTimeout func(msg *message.Message)

	// Metrics
	received      atomic.Int64
	acknowledged  atomic.Int64
	rejected      atomic.Int64
	redelivered   atomic.Int64
	lastMessageAt atomic.Value // time.Time
}

// New creates a new consumer with the given ID and configuration.
func New(id string, cfg Config) *Consumer {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}

	c := &Consumer{
		id:       id,
		config:   cfg,
		messages: make(chan *message.Message, cfg.BufferSize),
		pending:  make(map[string]*message.Message),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	// Start the acknowledgment timeout checker
	go c.ackTimeoutChecker()

	return c
}

// ID returns the consumer's unique identifier.
func (c *Consumer) ID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

// QueueName returns the name of the queue this consumer is subscribed to.
func (c *Consumer) QueueName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.queueName
}

// SetQueueName sets the queue name for this consumer.
func (c *Consumer) SetQueueName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueName = name
}

// GroupID returns the consumer group ID if any.
func (c *Consumer) GroupID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groupID
}

// SetGroupID sets the consumer group ID.
func (c *Consumer) SetGroupID(groupID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupID = groupID
}

// Messages returns the channel for receiving messages.
func (c *Consumer) Messages() <-chan *message.Message {
	return c.messages
}

// Deliver sends a message to the consumer.
// Returns an error if the consumer is closed or the buffer is full.
func (c *Consumer) Deliver(msg *message.Message) error {
	if c.closed.Load() {
		return ErrConsumerClosed
	}

	if c.paused.Load() {
		return errors.New("consumer is paused")
	}

	// Setup acknowledgment callbacks
	msg.SetAckFunc(func() error {
		return c.ack(msg.ID())
	})
	msg.SetNackFunc(func(requeue bool) error {
		return c.nack(msg.ID(), requeue)
	})

	// Track pending message
	c.pendingLock.Lock()
	c.pending[msg.ID()] = msg
	c.pendingLock.Unlock()

	// Mark as delivered
	msg.MarkDelivered()

	// Auto-ack if configured
	if c.config.AutoAck {
		defer c.ack(msg.ID())
	}

	select {
	case c.messages <- msg:
		c.received.Add(1)
		c.lastMessageAt.Store(time.Now())
		return nil
	case <-c.stopCh:
		return ErrConsumerClosed
	default:
		// Remove from pending if we can't deliver
		c.pendingLock.Lock()
		delete(c.pending, msg.ID())
		c.pendingLock.Unlock()
		return errors.New("consumer buffer is full")
	}
}

// ack acknowledges a message by ID.
func (c *Consumer) ack(msgID string) error {
	c.pendingLock.Lock()
	msg, exists := c.pending[msgID]
	if exists {
		delete(c.pending, msgID)
	}
	c.pendingLock.Unlock()

	if !exists {
		return errors.New("message not found in pending")
	}

	msg.SetState(message.StateAcknowledged)
	c.acknowledged.Add(1)

	if c.onAck != nil {
		c.onAck(msgID)
	}

	return nil
}

// nack negatively acknowledges a message.
func (c *Consumer) nack(msgID string, requeue bool) error {
	c.pendingLock.Lock()
	msg, exists := c.pending[msgID]
	if exists {
		delete(c.pending, msgID)
	}
	c.pendingLock.Unlock()

	if !exists {
		return errors.New("message not found in pending")
	}

	if requeue {
		msg.SetState(message.StatePending)
		c.redelivered.Add(1)
	} else {
		msg.SetState(message.StateRejected)
		c.rejected.Add(1)
	}

	if c.onNack != nil {
		c.onNack(msgID, requeue)
	}

	return nil
}

// ackTimeoutChecker periodically checks for messages that have exceeded the ack timeout.
func (c *Consumer) ackTimeoutChecker() {
	ticker := time.NewTicker(c.config.AckTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkAckTimeouts()
		}
	}
}

// checkAckTimeouts handles messages that have timed out waiting for acknowledgment.
func (c *Consumer) checkAckTimeouts() {
	now := time.Now()
	var timedOut []*message.Message

	c.pendingLock.Lock()
	for id, msg := range c.pending {
		if now.Sub(msg.LastDelivered()) > c.config.AckTimeout {
			timedOut = append(timedOut, msg)
			delete(c.pending, id)
		}
	}
	c.pendingLock.Unlock()

	for _, msg := range timedOut {
		if c.onTimeout != nil {
			c.onTimeout(msg)
		}
	}
}

// Pause pauses message delivery to this consumer.
func (c *Consumer) Pause() {
	c.paused.Store(true)
}

// Resume resumes message delivery to this consumer.
func (c *Consumer) Resume() {
	c.paused.Store(false)
}

// IsPaused returns true if the consumer is paused.
func (c *Consumer) IsPaused() bool {
	return c.paused.Load()
}

// IsClosed returns true if the consumer is closed.
func (c *Consumer) IsClosed() bool {
	return c.closed.Load()
}

// PendingCount returns the number of messages awaiting acknowledgment.
func (c *Consumer) PendingCount() int {
	c.pendingLock.RLock()
	defer c.pendingLock.RUnlock()
	return len(c.pending)
}

// OnAck sets the callback for when a message is acknowledged.
func (c *Consumer) OnAck(fn func(msgID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onAck = fn
}

// OnNack sets the callback for negative acknowledgments.
func (c *Consumer) OnNack(fn func(msgID string, requeue bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onNack = fn
}

// OnTimeout sets the callback for acknowledgment timeouts.
func (c *Consumer) OnTimeout(fn func(msg *message.Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onTimeout = fn
}

// Stats returns consumer statistics.
func (c *Consumer) Stats() ConsumerStats {
	var lastMsg time.Time
	if v := c.lastMessageAt.Load(); v != nil {
		lastMsg = v.(time.Time)
	}

	return ConsumerStats{
		ID:            c.id,
		QueueName:     c.queueName,
		GroupID:       c.groupID,
		Received:      c.received.Load(),
		Acknowledged:  c.acknowledged.Load(),
		Rejected:      c.rejected.Load(),
		Redelivered:   c.redelivered.Load(),
		Pending:       int64(c.PendingCount()),
		IsPaused:      c.paused.Load(),
		IsClosed:      c.closed.Load(),
		LastMessageAt: lastMsg,
	}
}

// Close closes the consumer and releases resources.
func (c *Consumer) Close() error {
	var err error
	c.stopOnce.Do(func() {
		c.closed.Store(true)
		close(c.stopCh)
		close(c.messages)

		// Return pending messages via nack
		c.pendingLock.Lock()
		for _, msg := range c.pending {
			msg.SetState(message.StatePending)
		}
		c.pending = nil
		c.pendingLock.Unlock()

		close(c.doneCh)
	})
	return err
}

// Done returns a channel that's closed when the consumer is fully stopped.
func (c *Consumer) Done() <-chan struct{} {
	return c.doneCh
}

// ConsumerStats holds statistics for a consumer.
type ConsumerStats struct {
	ID            string
	QueueName     string
	GroupID       string
	Received      int64
	Acknowledged  int64
	Rejected      int64
	Redelivered   int64
	Pending       int64
	IsPaused      bool
	IsClosed      bool
	LastMessageAt time.Time
}

// Group represents a consumer group that distributes messages among multiple consumers.
type Group struct {
	mu sync.RWMutex

	id        string
	queueName string
	consumers map[string]*Consumer
	order     []string // Maintains insertion order for round-robin

	// Round-robin state
	nextIndex atomic.Int64

	// Configuration
	config GroupConfig

	// State
	closed atomic.Bool

	// Callbacks
	onConsumerAdded   func(consumerID string)
	onConsumerRemoved func(consumerID string)
}

// GroupConfig holds configuration for a consumer group.
type GroupConfig struct {
	// MaxConsumers is the maximum number of consumers in the group (0 = unlimited).
	MaxConsumers int

	// Strategy is the message distribution strategy.
	Strategy DistributionStrategy
}

// DistributionStrategy defines how messages are distributed to consumers.
type DistributionStrategy int

const (
	// RoundRobin distributes messages evenly across consumers.
	RoundRobin DistributionStrategy = iota

	// Broadcast sends messages to all consumers.
	Broadcast

	// Random distributes messages randomly.
	Random
)

// DefaultGroupConfig returns default group configuration.
func DefaultGroupConfig() GroupConfig {
	return GroupConfig{
		MaxConsumers: 0,
		Strategy:     RoundRobin,
	}
}

// NewGroup creates a new consumer group.
func NewGroup(id string, cfg GroupConfig) *Group {
	return &Group{
		id:        id,
		consumers: make(map[string]*Consumer),
		order:     make([]string, 0),
		config:    cfg,
	}
}

// ID returns the group's unique identifier.
func (g *Group) ID() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.id
}

// QueueName returns the queue name for this group.
func (g *Group) QueueName() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.queueName
}

// SetQueueName sets the queue name for this group.
func (g *Group) SetQueueName(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queueName = name
}

// AddConsumer adds a consumer to the group.
func (g *Group) AddConsumer(c *Consumer) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.consumers[c.ID()]; exists {
		return ErrConsumerExists
	}

	if g.config.MaxConsumers > 0 && len(g.consumers) >= g.config.MaxConsumers {
		return errors.New("consumer group is full")
	}

	c.SetGroupID(g.id)
	c.SetQueueName(g.queueName)
	g.consumers[c.ID()] = c
	g.order = append(g.order, c.ID())

	if g.onConsumerAdded != nil {
		g.onConsumerAdded(c.ID())
	}

	return nil
}

// RemoveConsumer removes a consumer from the group.
func (g *Group) RemoveConsumer(consumerID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	consumer, exists := g.consumers[consumerID]
	if !exists {
		return ErrConsumerNotFound
	}

	delete(g.consumers, consumerID)

	// Remove from order slice
	for i, id := range g.order {
		if id == consumerID {
			g.order = append(g.order[:i], g.order[i+1:]...)
			break
		}
	}

	consumer.SetGroupID("")

	if g.onConsumerRemoved != nil {
		g.onConsumerRemoved(consumerID)
	}

	return nil
}

// GetConsumer returns a consumer by ID.
func (g *Group) GetConsumer(consumerID string) (*Consumer, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	consumer, exists := g.consumers[consumerID]
	if !exists {
		return nil, ErrConsumerNotFound
	}

	return consumer, nil
}

// Consumers returns all consumers in the group.
func (g *Group) Consumers() []*Consumer {
	g.mu.RLock()
	defer g.mu.RUnlock()

	consumers := make([]*Consumer, 0, len(g.consumers))
	for _, c := range g.consumers {
		consumers = append(consumers, c)
	}
	return consumers
}

// ConsumerCount returns the number of consumers in the group.
func (g *Group) ConsumerCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.consumers)
}

// Deliver delivers a message to the group according to the distribution strategy.
func (g *Group) Deliver(ctx context.Context, msg *message.Message) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}

	g.mu.RLock()
	if len(g.consumers) == 0 {
		g.mu.RUnlock()
		return ErrNoConsumers
	}

	switch g.config.Strategy {
	case Broadcast:
		return g.deliverBroadcast(ctx, msg)
	case Random:
		return g.deliverRandom(msg)
	default:
		return g.deliverRoundRobin(msg)
	}
}

// deliverRoundRobin delivers the message to the next available consumer.
func (g *Group) deliverRoundRobin(msg *message.Message) error {
	attempts := len(g.order)
	defer g.mu.RUnlock()

	for i := 0; i < attempts; i++ {
		idx := int(g.nextIndex.Add(1)-1) % len(g.order)
		consumerID := g.order[idx]
		consumer := g.consumers[consumerID]

		if consumer != nil && !consumer.IsClosed() && !consumer.IsPaused() {
			if err := consumer.Deliver(msg); err == nil {
				return nil
			}
		}
	}

	return errors.New("no available consumers")
}

// deliverBroadcast delivers the message to all consumers.
func (g *Group) deliverBroadcast(ctx context.Context, msg *message.Message) error {
	consumers := make([]*Consumer, 0, len(g.consumers))
	for _, c := range g.consumers {
		consumers = append(consumers, c)
	}
	g.mu.RUnlock()

	var lastErr error
	delivered := 0

	for _, consumer := range consumers {
		if consumer.IsClosed() || consumer.IsPaused() {
			continue
		}

		// Clone message for each consumer
		msgCopy := msg.Clone()
		if err := consumer.Deliver(msgCopy); err != nil {
			lastErr = err
		} else {
			delivered++
		}
	}

	if delivered == 0 && lastErr != nil {
		return lastErr
	}

	return nil
}

// deliverRandom delivers the message to a random available consumer.
func (g *Group) deliverRandom(msg *message.Message) error {
	defer g.mu.RUnlock()

	// Try all consumers in order (simplified from truly random for determinism)
	for _, consumer := range g.consumers {
		if !consumer.IsClosed() && !consumer.IsPaused() {
			if err := consumer.Deliver(msg); err == nil {
				return nil
			}
		}
	}

	return errors.New("no available consumers")
}

// OnConsumerAdded sets the callback for when a consumer is added.
func (g *Group) OnConsumerAdded(fn func(consumerID string)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onConsumerAdded = fn
}

// OnConsumerRemoved sets the callback for when a consumer is removed.
func (g *Group) OnConsumerRemoved(fn func(consumerID string)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onConsumerRemoved = fn
}

// Stats returns statistics for the consumer group.
func (g *Group) Stats() GroupStats {
	g.mu.RLock()
	defer g.mu.RUnlock()

	stats := GroupStats{
		ID:            g.id,
		QueueName:     g.queueName,
		ConsumerCount: len(g.consumers),
		ConsumerStats: make([]ConsumerStats, 0, len(g.consumers)),
	}

	for _, c := range g.consumers {
		cStats := c.Stats()
		stats.ConsumerStats = append(stats.ConsumerStats, cStats)
		stats.TotalReceived += cStats.Received
		stats.TotalAcknowledged += cStats.Acknowledged
		stats.TotalRejected += cStats.Rejected
		stats.TotalPending += cStats.Pending
	}

	return stats
}

// Close closes the consumer group and all its consumers.
func (g *Group) Close() error {
	if g.closed.Swap(true) {
		return ErrGroupClosed
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	var lastErr error
	for _, consumer := range g.consumers {
		if err := consumer.Close(); err != nil {
			lastErr = err
		}
	}

	g.consumers = nil
	g.order = nil

	return lastErr
}

// IsClosed returns true if the group is closed.
func (g *Group) IsClosed() bool {
	return g.closed.Load()
}

// GroupStats holds statistics for a consumer group.
type GroupStats struct {
	ID                string
	QueueName         string
	ConsumerCount     int
	TotalReceived     int64
	TotalAcknowledged int64
	TotalRejected     int64
	TotalPending      int64
	ConsumerStats     []ConsumerStats
}
