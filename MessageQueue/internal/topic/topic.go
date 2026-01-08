// Package topic provides the Topic implementation for publish-subscribe messaging.
package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/message"
)

// Common errors returned by Topic operations.
var (
	ErrTopicClosed        = errors.New("topic is closed")
	ErrSubscriberExists   = errors.New("subscriber already exists")
	ErrSubscriberNotFound = errors.New("subscriber not found")
	ErrNoSubscribers      = errors.New("no subscribers")
)

// Config holds configuration for a Topic.
type Config struct {
	// Name of the topic
	Name string

	// BufferSize is the size of each subscriber's message buffer
	BufferSize int

	// DefaultTTL is the default time-to-live for messages
	DefaultTTL time.Duration

	// RetainMessages determines if the topic should retain the last message
	// for new subscribers
	RetainMessages bool

	// MaxRetainedMessages is the maximum number of messages to retain
	MaxRetainedMessages int
}

// DefaultConfig returns a default Topic configuration.
func DefaultConfig(name string) Config {
	return Config{
		Name:                name,
		BufferSize:          100,
		DefaultTTL:          24 * time.Hour,
		RetainMessages:      false,
		MaxRetainedMessages: 100,
	}
}

// Stats holds statistics about a topic.
type Stats struct {
	Name            string
	PublishCount    int64
	DeliveryCount   int64
	SubscriberCount int
	RetainedCount   int
	CreatedAt       time.Time
}

// Subscriber represents a topic subscriber.
type Subscriber struct {
	mu sync.RWMutex

	id       string
	topic    *Topic
	messages chan *message.Message
	filter   FilterFunc
	closed   bool
	closeCh  chan struct{}

	// Stats
	receivedCount int64
	droppedCount  int64
}

// FilterFunc is a function type for filtering messages.
// Returns true if the message should be delivered to the subscriber.
type FilterFunc func(msg *message.Message) bool

// ID returns the subscriber's identifier.
func (s *Subscriber) ID() string {
	return s.id
}

// Messages returns the channel for receiving messages.
func (s *Subscriber) Messages() <-chan *message.Message {
	return s.messages
}

// SetFilter sets a filter function for this subscriber.
// Only messages that pass the filter will be delivered.
func (s *Subscriber) SetFilter(filter FilterFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filter = filter
}

// Close unsubscribes and closes the subscriber.
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closeCh)
	close(s.messages)

	return s.topic.Unsubscribe(s.id)
}

// IsClosed returns whether the subscriber is closed.
func (s *Subscriber) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// Stats returns subscriber statistics.
func (s *Subscriber) Stats() (received, dropped int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return atomic.LoadInt64(&s.receivedCount), atomic.LoadInt64(&s.droppedCount)
}

// Topic represents a publish-subscribe topic.
type Topic struct {
	mu sync.RWMutex

	config      Config
	subscribers map[string]*Subscriber

	// Message retention
	retained []*message.Message

	// Stats
	publishCount  int64
	deliveryCount int64

	// Lifecycle
	createdAt time.Time
	closed    bool
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// New creates a new Topic with the given configuration.
func New(cfg Config) *Topic {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}

	t := &Topic{
		config:      cfg,
		subscribers: make(map[string]*Subscriber),
		retained:    make([]*message.Message, 0),
		createdAt:   time.Now(),
		closeCh:     make(chan struct{}),
	}

	// Start expiration worker if TTL is set
	if cfg.DefaultTTL > 0 {
		t.wg.Add(1)
		go t.expirationWorker()
	}

	return t
}

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.config.Name
}

// Config returns the topic configuration.
func (t *Topic) Config() Config {
	return t.config
}

// Publish broadcasts a message to all subscribers.
func (t *Topic) Publish(ctx context.Context, msg *message.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTopicClosed
	}

	// Apply default TTL if not set
	if msg.TTL() == 0 && t.config.DefaultTTL > 0 {
		msg.SetTTL(t.config.DefaultTTL)
	}

	// Set topic name
	msg.SetTopicName(t.config.Name)

	atomic.AddInt64(&t.publishCount, 1)

	// Retain message if configured
	if t.config.RetainMessages {
		t.retainMessage(msg)
	}

	// Deliver to all subscribers
	t.broadcastMessage(ctx, msg)

	return nil
}

// PublishAsync publishes a message asynchronously without waiting for delivery.
func (t *Topic) PublishAsync(msg *message.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return ErrTopicClosed
	}
	t.mu.RUnlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t.Publish(ctx, msg)
	}()

	return nil
}

// Subscribe creates a new subscriber for this topic.
func (t *Topic) Subscribe(subscriberID string) (*Subscriber, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, ErrTopicClosed
	}

	if _, exists := t.subscribers[subscriberID]; exists {
		return nil, ErrSubscriberExists
	}

	subscriber := &Subscriber{
		id:       subscriberID,
		topic:    t,
		messages: make(chan *message.Message, t.config.BufferSize),
		closeCh:  make(chan struct{}),
	}

	t.subscribers[subscriberID] = subscriber

	// Send retained messages to new subscriber
	if t.config.RetainMessages && len(t.retained) > 0 {
		go t.sendRetainedMessages(subscriber)
	}

	return subscriber, nil
}

// SubscribeWithFilter creates a subscriber with a message filter.
func (t *Topic) SubscribeWithFilter(subscriberID string, filter FilterFunc) (*Subscriber, error) {
	subscriber, err := t.Subscribe(subscriberID)
	if err != nil {
		return nil, err
	}

	subscriber.SetFilter(filter)
	return subscriber, nil
}

// Unsubscribe removes a subscriber from the topic.
func (t *Topic) Unsubscribe(subscriberID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.subscribers[subscriberID]; !exists {
		return ErrSubscriberNotFound
	}

	delete(t.subscribers, subscriberID)
	return nil
}

// GetSubscriber returns a subscriber by ID.
func (t *Topic) GetSubscriber(subscriberID string) (*Subscriber, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	subscriber, exists := t.subscribers[subscriberID]
	if !exists {
		return nil, ErrSubscriberNotFound
	}

	return subscriber, nil
}

// Subscribers returns all subscriber IDs.
func (t *Topic) Subscribers() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ids := make([]string, 0, len(t.subscribers))
	for id := range t.subscribers {
		ids = append(ids, id)
	}
	return ids
}

// SubscriberCount returns the number of active subscribers.
func (t *Topic) SubscriberCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers)
}

// Stats returns topic statistics.
func (t *Topic) Stats() Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return Stats{
		Name:            t.config.Name,
		PublishCount:    atomic.LoadInt64(&t.publishCount),
		DeliveryCount:   atomic.LoadInt64(&t.deliveryCount),
		SubscriberCount: len(t.subscribers),
		RetainedCount:   len(t.retained),
		CreatedAt:       t.createdAt,
	}
}

// ClearRetained removes all retained messages.
func (t *Topic) ClearRetained() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retained = make([]*message.Message, 0)
}

// Close closes the topic and all subscribers.
func (t *Topic) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrTopicClosed
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	// Wait for background workers
	t.wg.Wait()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Close all subscribers
	for _, subscriber := range t.subscribers {
		subscriber.mu.Lock()
		if !subscriber.closed {
			subscriber.closed = true
			close(subscriber.closeCh)
			close(subscriber.messages)
		}
		subscriber.mu.Unlock()
	}

	t.subscribers = nil
	t.retained = nil

	return nil
}

// IsClosed returns whether the topic is closed.
func (t *Topic) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

// broadcastMessage sends a message to all subscribers.
// Caller must hold the mutex.
func (t *Topic) broadcastMessage(ctx context.Context, msg *message.Message) {
	for _, subscriber := range t.subscribers {
		if subscriber.IsClosed() {
			continue
		}

		// Check filter
		subscriber.mu.RLock()
		filter := subscriber.filter
		subscriber.mu.RUnlock()

		if filter != nil && !filter(msg) {
			continue
		}

		// Clone message for each subscriber to ensure independence
		msgCopy := msg.Clone()
		msgCopy.SetState(message.StateDelivered)

		// Try to send without blocking
		select {
		case subscriber.messages <- msgCopy:
			atomic.AddInt64(&subscriber.receivedCount, 1)
			atomic.AddInt64(&t.deliveryCount, 1)
		default:
			// Buffer full, drop message for this subscriber
			atomic.AddInt64(&subscriber.droppedCount, 1)
		}
	}
}

// retainMessage adds a message to the retention buffer.
// Caller must hold the mutex.
func (t *Topic) retainMessage(msg *message.Message) {
	// Clone message for retention
	retained := msg.Clone()

	// Add to retained messages
	t.retained = append(t.retained, retained)

	// Trim if over limit
	if t.config.MaxRetainedMessages > 0 && len(t.retained) > t.config.MaxRetainedMessages {
		// Remove oldest messages
		excess := len(t.retained) - t.config.MaxRetainedMessages
		t.retained = t.retained[excess:]
	}
}

// sendRetainedMessages sends retained messages to a new subscriber.
func (t *Topic) sendRetainedMessages(subscriber *Subscriber) {
	t.mu.RLock()
	retained := make([]*message.Message, len(t.retained))
	copy(retained, t.retained)
	t.mu.RUnlock()

	for _, msg := range retained {
		if subscriber.IsClosed() {
			return
		}

		// Check filter
		subscriber.mu.RLock()
		filter := subscriber.filter
		subscriber.mu.RUnlock()

		if filter != nil && !filter(msg) {
			continue
		}

		// Clone for the subscriber
		msgCopy := msg.Clone()
		msgCopy.SetHeader("x-retained", "true")

		select {
		case subscriber.messages <- msgCopy:
			atomic.AddInt64(&subscriber.receivedCount, 1)
		case <-subscriber.closeCh:
			return
		case <-time.After(5 * time.Second):
			// Timeout, skip this message
			atomic.AddInt64(&subscriber.droppedCount, 1)
		}
	}
}

// expirationWorker periodically removes expired retained messages.
func (t *Topic) expirationWorker() {
	defer t.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			t.removeExpiredRetained()
		}
	}
}

// removeExpiredRetained removes expired messages from the retention buffer.
func (t *Topic) removeExpiredRetained() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || len(t.retained) == 0 {
		return
	}

	var valid []*message.Message
	for _, msg := range t.retained {
		if !msg.IsExpired() {
			valid = append(valid, msg)
		}
	}

	t.retained = valid
}

// MultiTopic allows publishing to multiple topics at once.
type MultiTopic struct {
	topics []*Topic
}

// NewMultiTopic creates a new MultiTopic.
func NewMultiTopic(topics ...*Topic) *MultiTopic {
	return &MultiTopic{topics: topics}
}

// Publish publishes a message to all topics.
func (mt *MultiTopic) Publish(ctx context.Context, msg *message.Message) error {
	var lastErr error
	for _, topic := range mt.topics {
		// Clone for each topic
		msgCopy := msg.Clone()
		if err := topic.Publish(ctx, msgCopy); err != nil {
			lastErr = fmt.Errorf("failed to publish to topic %s: %w", topic.Name(), err)
		}
	}
	return lastErr
}

// TopicOption is a functional option for configuring a Topic.
type TopicOption func(*Config)

// WithBufferSize sets the subscriber buffer size.
func WithBufferSize(size int) TopicOption {
	return func(c *Config) {
		c.BufferSize = size
	}
}

// WithTTL sets the default message TTL.
func WithTTL(ttl time.Duration) TopicOption {
	return func(c *Config) {
		c.DefaultTTL = ttl
	}
}

// WithRetention enables message retention.
func WithRetention(maxMessages int) TopicOption {
	return func(c *Config) {
		c.RetainMessages = true
		c.MaxRetainedMessages = maxMessages
	}
}

// NewWithOptions creates a new Topic with functional options.
func NewWithOptions(name string, opts ...TopicOption) *Topic {
	cfg := DefaultConfig(name)
	for _, opt := range opts {
		opt(&cfg)
	}
	return New(cfg)
}
