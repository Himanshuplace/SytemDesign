// Package storage provides storage backends for the message queue.
package storage

import (
	"errors"
	"sync"
	"time"
)

// Common errors returned by storage operations.
var (
	ErrNotFound      = errors.New("message not found")
	ErrAlreadyExists = errors.New("message already exists")
	ErrStorageFull   = errors.New("storage is full")
	ErrStorageClosed = errors.New("storage is closed")
)

// MessageData represents the stored form of a message.
type MessageData struct {
	ID              string
	Payload         []byte
	Headers         map[string]string
	Priority        int
	Timestamp       time.Time
	TTL             time.Duration
	DeliveryTime    time.Time // For delayed messages
	DeduplicationID string
	RetryCount      int
	LastRetryAt     time.Time
}

// Storage defines the interface for message storage backends.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Store saves a message to storage.
	Store(msg *MessageData) error

	// Get retrieves a message by ID without removing it.
	Get(id string) (*MessageData, error)

	// Delete removes a message from storage.
	Delete(id string) error

	// List returns all message IDs in storage.
	List() ([]string, error)

	// Count returns the number of messages in storage.
	Count() (int64, error)

	// Clear removes all messages from storage.
	Clear() error

	// Close closes the storage and releases resources.
	Close() error
}

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	mu       sync.RWMutex
	messages map[string]*MessageData
	maxSize  int
	closed   bool
}

// MemoryConfig holds configuration for MemoryStorage.
type MemoryConfig struct {
	MaxSize int // Maximum number of messages (0 = unlimited)
}

// DefaultMemoryConfig returns a default configuration.
func DefaultMemoryConfig() MemoryConfig {
	return MemoryConfig{
		MaxSize: 100000,
	}
}

// NewMemoryStorage creates a new in-memory storage with the given configuration.
func NewMemoryStorage(cfg MemoryConfig) *MemoryStorage {
	return &MemoryStorage{
		messages: make(map[string]*MessageData),
		maxSize:  cfg.MaxSize,
	}
}

// Store saves a message to storage.
func (s *MemoryStorage) Store(msg *MessageData) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if _, exists := s.messages[msg.ID]; exists {
		return ErrAlreadyExists
	}

	if s.maxSize > 0 && len(s.messages) >= s.maxSize {
		return ErrStorageFull
	}

	// Deep copy the message to prevent external modifications
	stored := &MessageData{
		ID:              msg.ID,
		Payload:         make([]byte, len(msg.Payload)),
		Headers:         make(map[string]string, len(msg.Headers)),
		Priority:        msg.Priority,
		Timestamp:       msg.Timestamp,
		TTL:             msg.TTL,
		DeliveryTime:    msg.DeliveryTime,
		DeduplicationID: msg.DeduplicationID,
		RetryCount:      msg.RetryCount,
		LastRetryAt:     msg.LastRetryAt,
	}
	copy(stored.Payload, msg.Payload)
	for k, v := range msg.Headers {
		stored.Headers[k] = v
	}

	s.messages[msg.ID] = stored
	return nil
}

// Get retrieves a message by ID.
func (s *MemoryStorage) Get(id string) (*MessageData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	msg, exists := s.messages[id]
	if !exists {
		return nil, ErrNotFound
	}

	// Return a copy to prevent external modifications
	result := &MessageData{
		ID:              msg.ID,
		Payload:         make([]byte, len(msg.Payload)),
		Headers:         make(map[string]string, len(msg.Headers)),
		Priority:        msg.Priority,
		Timestamp:       msg.Timestamp,
		TTL:             msg.TTL,
		DeliveryTime:    msg.DeliveryTime,
		DeduplicationID: msg.DeduplicationID,
		RetryCount:      msg.RetryCount,
		LastRetryAt:     msg.LastRetryAt,
	}
	copy(result.Payload, msg.Payload)
	for k, v := range msg.Headers {
		result.Headers[k] = v
	}

	return result, nil
}

// Delete removes a message from storage.
func (s *MemoryStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if _, exists := s.messages[id]; !exists {
		return ErrNotFound
	}

	delete(s.messages, id)
	return nil
}

// List returns all message IDs in storage.
func (s *MemoryStorage) List() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	ids := make([]string, 0, len(s.messages))
	for id := range s.messages {
		ids = append(ids, id)
	}
	return ids, nil
}

// Count returns the number of messages in storage.
func (s *MemoryStorage) Count() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrStorageClosed
	}

	return int64(len(s.messages)), nil
}

// Clear removes all messages from storage.
func (s *MemoryStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	s.messages = make(map[string]*MessageData)
	return nil
}

// Close closes the storage.
func (s *MemoryStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	s.closed = true
	s.messages = nil
	return nil
}

// Update modifies an existing message in storage.
func (s *MemoryStorage) Update(msg *MessageData) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if _, exists := s.messages[msg.ID]; !exists {
		return ErrNotFound
	}

	// Deep copy the message
	stored := &MessageData{
		ID:              msg.ID,
		Payload:         make([]byte, len(msg.Payload)),
		Headers:         make(map[string]string, len(msg.Headers)),
		Priority:        msg.Priority,
		Timestamp:       msg.Timestamp,
		TTL:             msg.TTL,
		DeliveryTime:    msg.DeliveryTime,
		DeduplicationID: msg.DeduplicationID,
		RetryCount:      msg.RetryCount,
		LastRetryAt:     msg.LastRetryAt,
	}
	copy(stored.Payload, msg.Payload)
	for k, v := range msg.Headers {
		stored.Headers[k] = v
	}

	s.messages[msg.ID] = stored
	return nil
}

// Exists checks if a message exists in storage.
func (s *MemoryStorage) Exists(id string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, ErrStorageClosed
	}

	_, exists := s.messages[id]
	return exists, nil
}

// GetByDeduplicationID retrieves a message by its deduplication ID.
func (s *MemoryStorage) GetByDeduplicationID(dedupID string) (*MessageData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	for _, msg := range s.messages {
		if msg.DeduplicationID == dedupID {
			// Return a copy
			result := &MessageData{
				ID:              msg.ID,
				Payload:         make([]byte, len(msg.Payload)),
				Headers:         make(map[string]string, len(msg.Headers)),
				Priority:        msg.Priority,
				Timestamp:       msg.Timestamp,
				TTL:             msg.TTL,
				DeliveryTime:    msg.DeliveryTime,
				DeduplicationID: msg.DeduplicationID,
				RetryCount:      msg.RetryCount,
				LastRetryAt:     msg.LastRetryAt,
			}
			copy(result.Payload, msg.Payload)
			for k, v := range msg.Headers {
				result.Headers[k] = v
			}
			return result, nil
		}
	}

	return nil, ErrNotFound
}

// GetExpired returns all messages that have exceeded their TTL.
func (s *MemoryStorage) GetExpired(now time.Time) ([]*MessageData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	var expired []*MessageData
	for _, msg := range s.messages {
		if msg.TTL > 0 && now.After(msg.Timestamp.Add(msg.TTL)) {
			result := &MessageData{
				ID:              msg.ID,
				Payload:         make([]byte, len(msg.Payload)),
				Headers:         make(map[string]string, len(msg.Headers)),
				Priority:        msg.Priority,
				Timestamp:       msg.Timestamp,
				TTL:             msg.TTL,
				DeliveryTime:    msg.DeliveryTime,
				DeduplicationID: msg.DeduplicationID,
				RetryCount:      msg.RetryCount,
				LastRetryAt:     msg.LastRetryAt,
			}
			copy(result.Payload, msg.Payload)
			for k, v := range msg.Headers {
				result.Headers[k] = v
			}
			expired = append(expired, result)
		}
	}

	return expired, nil
}

// GetReady returns all messages that are ready for delivery.
// A message is ready if its delivery time is before or equal to the given time.
func (s *MemoryStorage) GetReady(now time.Time) ([]*MessageData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	var ready []*MessageData
	for _, msg := range s.messages {
		if msg.DeliveryTime.IsZero() || !now.Before(msg.DeliveryTime) {
			result := &MessageData{
				ID:              msg.ID,
				Payload:         make([]byte, len(msg.Payload)),
				Headers:         make(map[string]string, len(msg.Headers)),
				Priority:        msg.Priority,
				Timestamp:       msg.Timestamp,
				TTL:             msg.TTL,
				DeliveryTime:    msg.DeliveryTime,
				DeduplicationID: msg.DeduplicationID,
				RetryCount:      msg.RetryCount,
				LastRetryAt:     msg.LastRetryAt,
			}
			copy(result.Payload, msg.Payload)
			for k, v := range msg.Headers {
				result.Headers[k] = v
			}
			ready = append(ready, result)
		}
	}

	return ready, nil
}
