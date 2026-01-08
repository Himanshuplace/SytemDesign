// Package message defines the Message type and related utilities for the message queue.
package message

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Priority levels for messages.
const (
	PriorityLow    = 0
	PriorityNormal = 5
	PriorityHigh   = 10
)

// State represents the current state of a message.
type State int

const (
	StatePending State = iota
	StateDelivered
	StateAcknowledged
	StateRejected
	StateExpired
	StateDeadLettered
)

// String returns the string representation of the message state.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateDelivered:
		return "delivered"
	case StateAcknowledged:
		return "acknowledged"
	case StateRejected:
		return "rejected"
	case StateExpired:
		return "expired"
	case StateDeadLettered:
		return "dead_lettered"
	default:
		return "unknown"
	}
}

// Common errors.
var (
	ErrAlreadyAcknowledged = errors.New("message already acknowledged")
	ErrMessageExpired      = errors.New("message has expired")
	ErrNilPayload          = errors.New("payload cannot be nil")
	ErrInvalidPriority     = errors.New("priority must be between 0 and 10")
)

// AckFunc is a function type for acknowledging messages.
type AckFunc func() error

// NackFunc is a function type for negative acknowledgment.
type NackFunc func(requeue bool) error

// Message represents a message in the queue.
type Message struct {
	mu sync.RWMutex

	// Core fields
	id        string
	payload   []byte
	timestamp time.Time

	// Metadata
	headers  map[string]string
	priority int

	// Timing
	ttl       time.Duration
	expiresAt time.Time
	delay     time.Duration
	deliverAt time.Time

	// Deduplication
	deduplicationID string

	// State tracking
	state         State
	deliveryCount int32
	lastDelivered time.Time

	// Acknowledgment callbacks
	ackFunc  AckFunc
	nackFunc NackFunc

	// Source tracking
	queueName string
	topicName string
}

// generateID generates a unique message ID.
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// Fallback to timestamp-based ID if random fails
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// New creates a new message with the given payload.
func New(payload []byte) *Message {
	now := time.Now()
	return &Message{
		id:        generateID(),
		payload:   payload,
		timestamp: now,
		headers:   make(map[string]string),
		priority:  PriorityNormal,
		state:     StatePending,
		deliverAt: now,
	}
}

// NewWithID creates a new message with a specific ID.
// Useful for message deduplication or idempotency.
func NewWithID(id string, payload []byte) *Message {
	msg := New(payload)
	msg.id = id
	return msg
}

// ID returns the unique identifier of the message.
func (m *Message) ID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.id
}

// Payload returns the message payload.
func (m *Message) Payload() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.payload
}

// PayloadString returns the message payload as a string.
func (m *Message) PayloadString() string {
	return string(m.Payload())
}

// Timestamp returns the time the message was created.
func (m *Message) Timestamp() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.timestamp
}

// SetHeader sets a header value on the message.
func (m *Message) SetHeader(key, value string) *Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headers[key] = value
	return m
}

// Header returns the value of a header.
func (m *Message) Header(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.headers[key]
}

// Headers returns a copy of all headers.
func (m *Message) Headers() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	headers := make(map[string]string, len(m.headers))
	for k, v := range m.headers {
		headers[k] = v
	}
	return headers
}

// SetPriority sets the message priority (0-10).
func (m *Message) SetPriority(priority int) *Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	if priority < 0 {
		priority = 0
	}
	if priority > 10 {
		priority = 10
	}
	m.priority = priority
	return m
}

// Priority returns the message priority.
func (m *Message) Priority() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.priority
}

// SetTTL sets the time-to-live for the message.
func (m *Message) SetTTL(ttl time.Duration) *Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ttl = ttl
	if ttl > 0 {
		m.expiresAt = m.timestamp.Add(ttl)
	} else {
		m.expiresAt = time.Time{}
	}
	return m
}

// TTL returns the time-to-live duration.
func (m *Message) TTL() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ttl
}

// ExpiresAt returns the expiration time.
func (m *Message) ExpiresAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.expiresAt
}

// IsExpired checks if the message has expired.
func (m *Message) IsExpired() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ttl == 0 {
		return false
	}
	return time.Now().After(m.expiresAt)
}

// SetDelay sets a delay before the message can be delivered.
func (m *Message) SetDelay(delay time.Duration) *Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.delay = delay
	m.deliverAt = m.timestamp.Add(delay)
	return m
}

// Delay returns the delay duration.
func (m *Message) Delay() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.delay
}

// DeliverAt returns the time when the message can be delivered.
func (m *Message) DeliverAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.deliverAt
}

// IsReady checks if the message is ready to be delivered.
func (m *Message) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return time.Now().After(m.deliverAt) || time.Now().Equal(m.deliverAt)
}

// SetDeduplicationID sets a deduplication ID for exactly-once delivery.
func (m *Message) SetDeduplicationID(id string) *Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deduplicationID = id
	return m
}

// DeduplicationID returns the deduplication ID.
func (m *Message) DeduplicationID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.deduplicationID
}

// State returns the current state of the message.
func (m *Message) State() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// SetState sets the message state.
func (m *Message) SetState(state State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
}

// DeliveryCount returns the number of delivery attempts.
func (m *Message) DeliveryCount() int {
	return int(atomic.LoadInt32(&m.deliveryCount))
}

// IncrementDeliveryCount increments and returns the delivery count.
func (m *Message) IncrementDeliveryCount() int {
	return int(atomic.AddInt32(&m.deliveryCount, 1))
}

// LastDelivered returns the time of the last delivery attempt.
func (m *Message) LastDelivered() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastDelivered
}

// MarkDelivered marks the message as delivered.
func (m *Message) MarkDelivered() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state = StateDelivered
	m.lastDelivered = time.Now()
	atomic.AddInt32(&m.deliveryCount, 1)
}

// SetAckFunc sets the acknowledgment callback function.
func (m *Message) SetAckFunc(fn AckFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackFunc = fn
}

// SetNackFunc sets the negative acknowledgment callback function.
func (m *Message) SetNackFunc(fn NackFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nackFunc = fn
}

// Ack acknowledges the message, indicating successful processing.
func (m *Message) Ack() error {
	m.mu.Lock()

	if m.state == StateAcknowledged {
		m.mu.Unlock()
		return ErrAlreadyAcknowledged
	}

	if m.IsExpired() {
		m.state = StateExpired
		m.mu.Unlock()
		return ErrMessageExpired
	}

	m.state = StateAcknowledged
	ackFunc := m.ackFunc
	m.mu.Unlock()

	if ackFunc != nil {
		return ackFunc()
	}
	return nil
}

// Nack negatively acknowledges the message.
// If requeue is true, the message will be requeued for redelivery.
func (m *Message) Nack(requeue bool) error {
	m.mu.Lock()

	if m.state == StateAcknowledged {
		m.mu.Unlock()
		return ErrAlreadyAcknowledged
	}

	if requeue {
		m.state = StatePending
	} else {
		m.state = StateRejected
	}

	nackFunc := m.nackFunc
	m.mu.Unlock()

	if nackFunc != nil {
		return nackFunc(requeue)
	}
	return nil
}

// Reject rejects the message without requeuing.
func (m *Message) Reject() error {
	return m.Nack(false)
}

// Requeue requeues the message for redelivery.
func (m *Message) Requeue() error {
	return m.Nack(true)
}

// SetQueueName sets the source queue name.
func (m *Message) SetQueueName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueName = name
}

// QueueName returns the source queue name.
func (m *Message) QueueName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queueName
}

// SetTopicName sets the source topic name.
func (m *Message) SetTopicName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topicName = name
}

// TopicName returns the source topic name.
func (m *Message) TopicName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topicName
}

// Clone creates a deep copy of the message with a new ID.
func (m *Message) Clone() *Message {
	m.mu.RLock()
	defer m.mu.RUnlock()

	payload := make([]byte, len(m.payload))
	copy(payload, m.payload)

	clone := &Message{
		id:              generateID(),
		payload:         payload,
		timestamp:       time.Now(),
		headers:         make(map[string]string, len(m.headers)),
		priority:        m.priority,
		ttl:             m.ttl,
		delay:           m.delay,
		deduplicationID: m.deduplicationID,
		state:           StatePending,
	}

	for k, v := range m.headers {
		clone.headers[k] = v
	}

	if clone.ttl > 0 {
		clone.expiresAt = clone.timestamp.Add(clone.ttl)
	}
	if clone.delay > 0 {
		clone.deliverAt = clone.timestamp.Add(clone.delay)
	} else {
		clone.deliverAt = clone.timestamp
	}

	return clone
}

// SerializedMessage is used for JSON serialization.
type SerializedMessage struct {
	ID              string            `json:"id"`
	Payload         []byte            `json:"payload"`
	Timestamp       time.Time         `json:"timestamp"`
	Headers         map[string]string `json:"headers"`
	Priority        int               `json:"priority"`
	TTL             time.Duration     `json:"ttl"`
	ExpiresAt       time.Time         `json:"expires_at,omitempty"`
	Delay           time.Duration     `json:"delay"`
	DeliverAt       time.Time         `json:"deliver_at"`
	DeduplicationID string            `json:"deduplication_id,omitempty"`
	State           State             `json:"state"`
	DeliveryCount   int               `json:"delivery_count"`
	QueueName       string            `json:"queue_name,omitempty"`
	TopicName       string            `json:"topic_name,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (m *Message) MarshalJSON() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return json.Marshal(SerializedMessage{
		ID:              m.id,
		Payload:         m.payload,
		Timestamp:       m.timestamp,
		Headers:         m.headers,
		Priority:        m.priority,
		TTL:             m.ttl,
		ExpiresAt:       m.expiresAt,
		Delay:           m.delay,
		DeliverAt:       m.deliverAt,
		DeduplicationID: m.deduplicationID,
		State:           m.state,
		DeliveryCount:   int(m.deliveryCount),
		QueueName:       m.queueName,
		TopicName:       m.topicName,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *Message) UnmarshalJSON(data []byte) error {
	var sm SerializedMessage
	if err := json.Unmarshal(data, &sm); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.id = sm.ID
	m.payload = sm.Payload
	m.timestamp = sm.Timestamp
	m.headers = sm.Headers
	m.priority = sm.Priority
	m.ttl = sm.TTL
	m.expiresAt = sm.ExpiresAt
	m.delay = sm.Delay
	m.deliverAt = sm.DeliverAt
	m.deduplicationID = sm.DeduplicationID
	m.state = sm.State
	m.deliveryCount = int32(sm.DeliveryCount)
	m.queueName = sm.QueueName
	m.topicName = sm.TopicName

	if m.headers == nil {
		m.headers = make(map[string]string)
	}

	return nil
}

// String returns a string representation of the message.
func (m *Message) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return "Message{id=" + m.id + ", state=" + m.state.String() + ", priority=" + string(rune('0'+m.priority)) + "}"
}

// Builder provides a fluent API for constructing messages.
type Builder struct {
	msg *Message
}

// NewBuilder creates a new message builder.
func NewBuilder() *Builder {
	return &Builder{msg: New(nil)}
}

// WithPayload sets the message payload.
func (b *Builder) WithPayload(payload []byte) *Builder {
	b.msg.payload = payload
	return b
}

// WithStringPayload sets the message payload from a string.
func (b *Builder) WithStringPayload(payload string) *Builder {
	b.msg.payload = []byte(payload)
	return b
}

// WithJSONPayload sets the message payload from a JSON-encodable value.
func (b *Builder) WithJSONPayload(v interface{}) *Builder {
	data, err := json.Marshal(v)
	if err == nil {
		b.msg.payload = data
		b.msg.SetHeader("content-type", "application/json")
	}
	return b
}

// WithID sets the message ID.
func (b *Builder) WithID(id string) *Builder {
	b.msg.id = id
	return b
}

// WithHeader adds a header to the message.
func (b *Builder) WithHeader(key, value string) *Builder {
	b.msg.SetHeader(key, value)
	return b
}

// WithPriority sets the message priority.
func (b *Builder) WithPriority(priority int) *Builder {
	b.msg.SetPriority(priority)
	return b
}

// WithTTL sets the message TTL.
func (b *Builder) WithTTL(ttl time.Duration) *Builder {
	b.msg.SetTTL(ttl)
	return b
}

// WithDelay sets the message delay.
func (b *Builder) WithDelay(delay time.Duration) *Builder {
	b.msg.SetDelay(delay)
	return b
}

// WithDeduplicationID sets the deduplication ID.
func (b *Builder) WithDeduplicationID(id string) *Builder {
	b.msg.SetDeduplicationID(id)
	return b
}

// Build returns the constructed message.
func (b *Builder) Build() *Message {
	return b.msg
}
