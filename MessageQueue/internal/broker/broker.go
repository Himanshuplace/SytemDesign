// Package broker provides the central message broker that coordinates queues, topics, and routing.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/dlq"
	"github.com/messagequeue/internal/message"
	"github.com/messagequeue/internal/queue"
	"github.com/messagequeue/internal/topic"
)

// Common errors returned by Broker operations.
var (
	ErrBrokerClosed     = errors.New("broker is closed")
	ErrBrokerNotStarted = errors.New("broker has not been started")
	ErrQueueExists      = errors.New("queue already exists")
	ErrQueueNotFound    = errors.New("queue not found")
	ErrTopicExists      = errors.New("topic already exists")
	ErrTopicNotFound    = errors.New("topic not found")
	ErrDLQNotFound      = errors.New("dead letter queue not found")
	ErrInvalidConfig    = errors.New("invalid configuration")
)

// Config holds configuration for the Broker.
type Config struct {
	// DefaultQueueConfig is the default configuration for new queues
	DefaultQueueConfig queue.Config

	// DefaultTopicConfig is the default configuration for new topics
	DefaultTopicConfig topic.Config

	// DefaultTTL is the default message time-to-live
	DefaultTTL time.Duration

	// MaxQueues is the maximum number of queues (0 = unlimited)
	MaxQueues int

	// MaxTopics is the maximum number of topics (0 = unlimited)
	MaxTopics int

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// MetricsInterval is the interval for metrics collection
	MetricsInterval time.Duration

	// EnableDLQ enables automatic dead letter queue creation
	EnableDLQ bool

	// DLQSuffix is the suffix appended to queue names for DLQ
	DLQSuffix string

	// ShutdownTimeout is the maximum time to wait for graceful shutdown
	ShutdownTimeout time.Duration
}

// DefaultConfig returns a default Broker configuration.
func DefaultConfig() Config {
	return Config{
		DefaultQueueConfig: queue.DefaultConfig(""),
		DefaultTopicConfig: topic.DefaultConfig(""),
		DefaultTTL:         24 * time.Hour,
		MaxQueues:          1000,
		MaxTopics:          1000,
		EnableMetrics:      true,
		MetricsInterval:    10 * time.Second,
		EnableDLQ:          true,
		DLQSuffix:          "-dlq",
		ShutdownTimeout:    30 * time.Second,
	}
}

// Stats holds statistics about the broker.
type Stats struct {
	QueueCount        int
	TopicCount        int
	TotalMessages     int64
	TotalPublished    int64
	TotalConsumed     int64
	TotalDeadLettered int64
	Uptime            time.Duration
	StartedAt         time.Time
}

// Broker is the central message broker that manages queues and topics.
type Broker struct {
	mu sync.RWMutex

	config Config

	// Queue management
	queues map[string]*queue.Queue
	dlqs   map[string]*dlq.DeadLetterQueue

	// Topic management
	topics map[string]*topic.Topic

	// Stats
	totalPublished    int64
	totalConsumed     int64
	totalDeadLettered int64

	// Lifecycle
	started   bool
	closed    bool
	startedAt time.Time
	closeCh   chan struct{}
	wg        sync.WaitGroup

	// Callbacks
	onQueueCreated func(name string)
	onQueueDeleted func(name string)
	onTopicCreated func(name string)
	onTopicDeleted func(name string)
	onMessageDead  func(msg *message.Message, reason string)
}

// New creates a new Broker with the given configuration.
func New(cfg Config) *Broker {
	return &Broker{
		config:  cfg,
		queues:  make(map[string]*queue.Queue),
		dlqs:    make(map[string]*dlq.DeadLetterQueue),
		topics:  make(map[string]*topic.Topic),
		closeCh: make(chan struct{}),
	}
}

// Start starts the broker and its background workers.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	if b.started {
		return nil // Already started
	}

	b.started = true
	b.startedAt = time.Now()

	// Start metrics collector if enabled
	if b.config.EnableMetrics {
		b.wg.Add(1)
		go b.metricsCollector()
	}

	return nil
}

// CreateQueue creates a new queue with the given name.
func (b *Broker) CreateQueue(name string, opts ...QueueOption) (*queue.Queue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	if _, exists := b.queues[name]; exists {
		return nil, ErrQueueExists
	}

	if b.config.MaxQueues > 0 && len(b.queues) >= b.config.MaxQueues {
		return nil, errors.New("maximum number of queues reached")
	}

	// Create queue config from defaults
	cfg := b.config.DefaultQueueConfig
	cfg.Name = name
	cfg.DefaultTTL = b.config.DefaultTTL

	// Apply options
	for _, opt := range opts {
		opt(&cfg)
	}

	// Create the queue
	q := queue.New(cfg)
	b.queues[name] = q

	// Create associated DLQ if enabled
	if b.config.EnableDLQ && cfg.DeadLetterQueue == "" {
		dlqName := name + b.config.DLQSuffix
		cfg.DeadLetterQueue = dlqName

		dlqConfig := dlq.DefaultConfig(dlqName)
		d := dlq.New(dlqConfig)
		b.dlqs[dlqName] = d

		// Create a queue for the DLQ messages
		dlqQueueCfg := b.config.DefaultQueueConfig
		dlqQueueCfg.Name = dlqName
		dlqQueueCfg.MaxRetries = 0 // DLQ messages don't retry
		dlqQueue := queue.New(dlqQueueCfg)
		b.queues[dlqName] = dlqQueue

		// Set DLQ on the main queue
		q.SetDLQ(dlqQueue)
	}

	if b.onQueueCreated != nil {
		go b.onQueueCreated(name)
	}

	return q, nil
}

// GetQueue returns a queue by name.
func (b *Broker) GetQueue(name string) (*queue.Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	q, exists := b.queues[name]
	if !exists {
		return nil, ErrQueueNotFound
	}

	return q, nil
}

// DeleteQueue deletes a queue by name.
func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	q, exists := b.queues[name]
	if !exists {
		return ErrQueueNotFound
	}

	// Close the queue
	if err := q.Close(); err != nil {
		return fmt.Errorf("failed to close queue: %w", err)
	}

	delete(b.queues, name)

	// Also delete associated DLQ if it exists
	dlqName := name + b.config.DLQSuffix
	if d, exists := b.dlqs[dlqName]; exists {
		d.Close()
		delete(b.dlqs, dlqName)
	}
	if dlqQueue, exists := b.queues[dlqName]; exists {
		dlqQueue.Close()
		delete(b.queues, dlqName)
	}

	if b.onQueueDeleted != nil {
		go b.onQueueDeleted(name)
	}

	return nil
}

// ListQueues returns all queue names.
func (b *Broker) ListQueues() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.queues))
	for name := range b.queues {
		names = append(names, name)
	}
	return names
}

// CreateTopic creates a new topic with the given name.
func (b *Broker) CreateTopic(name string, opts ...TopicOption) (*topic.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	if _, exists := b.topics[name]; exists {
		return nil, ErrTopicExists
	}

	if b.config.MaxTopics > 0 && len(b.topics) >= b.config.MaxTopics {
		return nil, errors.New("maximum number of topics reached")
	}

	// Create topic config from defaults
	cfg := b.config.DefaultTopicConfig
	cfg.Name = name
	cfg.DefaultTTL = b.config.DefaultTTL

	// Apply options
	for _, opt := range opts {
		opt(&cfg)
	}

	// Create the topic
	t := topic.New(cfg)
	b.topics[name] = t

	if b.onTopicCreated != nil {
		go b.onTopicCreated(name)
	}

	return t, nil
}

// GetTopic returns a topic by name.
func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	t, exists := b.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return t, nil
}

// DeleteTopic deletes a topic by name.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	t, exists := b.topics[name]
	if !exists {
		return ErrTopicNotFound
	}

	// Close the topic
	if err := t.Close(); err != nil {
		return fmt.Errorf("failed to close topic: %w", err)
	}

	delete(b.topics, name)

	if b.onTopicDeleted != nil {
		go b.onTopicDeleted(name)
	}

	return nil
}

// ListTopics returns all topic names.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// Publish publishes a message to a queue.
func (b *Broker) Publish(ctx context.Context, queueName string, msg *message.Message) error {
	q, err := b.GetQueue(queueName)
	if err != nil {
		return err
	}

	if err := q.Publish(ctx, msg); err != nil {
		return err
	}

	atomic.AddInt64(&b.totalPublished, 1)
	return nil
}

// PublishToTopic publishes a message to a topic.
func (b *Broker) PublishToTopic(ctx context.Context, topicName string, msg *message.Message) error {
	t, err := b.GetTopic(topicName)
	if err != nil {
		return err
	}

	if err := t.Publish(ctx, msg); err != nil {
		return err
	}

	atomic.AddInt64(&b.totalPublished, 1)
	return nil
}

// Subscribe subscribes to a queue.
func (b *Broker) Subscribe(queueName, consumerID string) (*queue.Consumer, error) {
	q, err := b.GetQueue(queueName)
	if err != nil {
		return nil, err
	}

	return q.Subscribe(consumerID)
}

// SubscribeToTopic subscribes to a topic.
func (b *Broker) SubscribeToTopic(topicName, subscriberID string) (*topic.Subscriber, error) {
	t, err := b.GetTopic(topicName)
	if err != nil {
		return nil, err
	}

	return t.Subscribe(subscriberID)
}

// GetDLQ returns a dead letter queue by name.
func (b *Broker) GetDLQ(name string) (*dlq.DeadLetterQueue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	d, exists := b.dlqs[name]
	if !exists {
		return nil, ErrDLQNotFound
	}

	return d, nil
}

// Stats returns broker statistics.
func (b *Broker) Stats() Stats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var totalMessages int64
	for _, q := range b.queues {
		totalMessages += q.Depth()
	}

	var uptime time.Duration
	if b.started {
		uptime = time.Since(b.startedAt)
	}

	return Stats{
		QueueCount:        len(b.queues),
		TopicCount:        len(b.topics),
		TotalMessages:     totalMessages,
		TotalPublished:    atomic.LoadInt64(&b.totalPublished),
		TotalConsumed:     atomic.LoadInt64(&b.totalConsumed),
		TotalDeadLettered: atomic.LoadInt64(&b.totalDeadLettered),
		Uptime:            uptime,
		StartedAt:         b.startedAt,
	}
}

// QueueStats returns statistics for a specific queue.
func (b *Broker) QueueStats(name string) (queue.Stats, error) {
	q, err := b.GetQueue(name)
	if err != nil {
		return queue.Stats{}, err
	}
	return q.Stats(), nil
}

// TopicStats returns statistics for a specific topic.
func (b *Broker) TopicStats(name string) (topic.Stats, error) {
	t, err := b.GetTopic(name)
	if err != nil {
		return topic.Stats{}, err
	}
	return t.Stats(), nil
}

// OnQueueCreated sets a callback for when a queue is created.
func (b *Broker) OnQueueCreated(fn func(name string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onQueueCreated = fn
}

// OnQueueDeleted sets a callback for when a queue is deleted.
func (b *Broker) OnQueueDeleted(fn func(name string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onQueueDeleted = fn
}

// OnTopicCreated sets a callback for when a topic is created.
func (b *Broker) OnTopicCreated(fn func(name string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onTopicCreated = fn
}

// OnTopicDeleted sets a callback for when a topic is deleted.
func (b *Broker) OnTopicDeleted(fn func(name string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onTopicDeleted = fn
}

// OnMessageDead sets a callback for when a message is dead-lettered.
func (b *Broker) OnMessageDead(fn func(msg *message.Message, reason string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onMessageDead = fn
}

// Shutdown gracefully shuts down the broker.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	b.closed = true
	close(b.closeCh)
	b.mu.Unlock()

	// Create a channel to signal completion
	done := make(chan struct{})

	go func() {
		// Wait for background workers
		b.wg.Wait()

		// Close all queues
		b.mu.Lock()
		for _, q := range b.queues {
			q.Close()
		}

		// Close all topics
		for _, t := range b.topics {
			t.Close()
		}

		// Close all DLQs
		for _, d := range b.dlqs {
			d.Close()
		}
		b.mu.Unlock()

		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsRunning returns whether the broker is running.
func (b *Broker) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.started && !b.closed
}

// metricsCollector periodically collects and logs metrics.
func (b *Broker) metricsCollector() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.closeCh:
			return
		case <-ticker.C:
			// Metrics collection logic would go here
			// This could be extended to publish metrics to external systems
		}
	}
}

// QueueOption is a functional option for configuring a queue.
type QueueOption func(*queue.Config)

// WithQueueMaxSize sets the maximum queue size.
func WithQueueMaxSize(size int) QueueOption {
	return func(c *queue.Config) {
		c.MaxSize = size
	}
}

// WithQueueTTL sets the default TTL for queue messages.
func WithQueueTTL(ttl time.Duration) QueueOption {
	return func(c *queue.Config) {
		c.DefaultTTL = ttl
	}
}

// WithQueueMaxRetries sets the maximum retry count.
func WithQueueMaxRetries(retries int) QueueOption {
	return func(c *queue.Config) {
		c.MaxRetries = retries
	}
}

// WithQueueAckTimeout sets the acknowledgment timeout.
func WithQueueAckTimeout(timeout time.Duration) QueueOption {
	return func(c *queue.Config) {
		c.AckTimeout = timeout
	}
}

// WithQueueDLQ sets the dead letter queue name.
func WithQueueDLQ(dlqName string) QueueOption {
	return func(c *queue.Config) {
		c.DeadLetterQueue = dlqName
	}
}

// WithQueuePriority enables/disables priority ordering.
func WithQueuePriority(enabled bool) QueueOption {
	return func(c *queue.Config) {
		c.EnablePriority = enabled
	}
}

// TopicOption is a functional option for configuring a topic.
type TopicOption func(*topic.Config)

// WithTopicBufferSize sets the subscriber buffer size.
func WithTopicBufferSize(size int) TopicOption {
	return func(c *topic.Config) {
		c.BufferSize = size
	}
}

// WithTopicTTL sets the default TTL for topic messages.
func WithTopicTTL(ttl time.Duration) TopicOption {
	return func(c *topic.Config) {
		c.DefaultTTL = ttl
	}
}

// WithTopicRetention enables message retention.
func WithTopicRetention(maxMessages int) TopicOption {
	return func(c *topic.Config) {
		c.RetainMessages = true
		c.MaxRetainedMessages = maxMessages
	}
}

// GetOrCreateQueue gets a queue by name, creating it if it doesn't exist.
func (b *Broker) GetOrCreateQueue(name string, opts ...QueueOption) (*queue.Queue, error) {
	q, err := b.GetQueue(name)
	if err == nil {
		return q, nil
	}

	if errors.Is(err, ErrQueueNotFound) {
		return b.CreateQueue(name, opts...)
	}

	return nil, err
}

// GetOrCreateTopic gets a topic by name, creating it if it doesn't exist.
func (b *Broker) GetOrCreateTopic(name string, opts ...TopicOption) (*topic.Topic, error) {
	t, err := b.GetTopic(name)
	if err == nil {
		return t, nil
	}

	if errors.Is(err, ErrTopicNotFound) {
		return b.CreateTopic(name, opts...)
	}

	return nil, err
}

// PurgeQueue removes all messages from a queue.
func (b *Broker) PurgeQueue(name string) error {
	q, err := b.GetQueue(name)
	if err != nil {
		return err
	}
	return q.Purge()
}
