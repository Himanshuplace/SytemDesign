// Package storage provides storage backends for the message queue.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DiskStorage is a persistent disk-based implementation of the Storage interface.
type DiskStorage struct {
	mu sync.RWMutex

	dataDir   string
	indexFile string
	index     map[string]*MessageData
	closed    bool

	// Configuration
	syncOnWrite bool
	maxSize     int
}

// DiskConfig holds configuration for DiskStorage.
type DiskConfig struct {
	// DataDir is the directory for storing message files
	DataDir string

	// SyncOnWrite ensures data is synced to disk on every write
	SyncOnWrite bool

	// MaxSize is the maximum number of messages (0 = unlimited)
	MaxSize int
}

// DefaultDiskConfig returns a default disk storage configuration.
func DefaultDiskConfig() DiskConfig {
	return DiskConfig{
		DataDir:     "./data/messages",
		SyncOnWrite: true,
		MaxSize:     100000,
	}
}

// NewDiskStorage creates a new disk-based storage with the given configuration.
func NewDiskStorage(cfg DiskConfig) (*DiskStorage, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data/messages"
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	d := &DiskStorage{
		dataDir:     cfg.DataDir,
		indexFile:   filepath.Join(cfg.DataDir, "index.json"),
		index:       make(map[string]*MessageData),
		syncOnWrite: cfg.SyncOnWrite,
		maxSize:     cfg.MaxSize,
	}

	// Load existing index
	if err := d.loadIndex(); err != nil {
		// If index doesn't exist, that's fine - we'll start fresh
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load index: %w", err)
		}
	}

	return d, nil
}

// Store saves a message to disk storage.
func (d *DiskStorage) Store(msg *MessageData) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	if _, exists := d.index[msg.ID]; exists {
		return ErrAlreadyExists
	}

	if d.maxSize > 0 && len(d.index) >= d.maxSize {
		return ErrStorageFull
	}

	// Deep copy the message
	stored := d.copyMessage(msg)

	// Write message to disk
	if err := d.writeMessage(stored); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Update index
	d.index[msg.ID] = stored

	// Persist index
	if err := d.saveIndex(); err != nil {
		// Rollback: remove the written message file
		d.deleteMessageFile(msg.ID)
		delete(d.index, msg.ID)
		return fmt.Errorf("failed to save index: %w", err)
	}

	return nil
}

// Get retrieves a message by ID.
func (d *DiskStorage) Get(id string) (*MessageData, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrStorageClosed
	}

	msg, exists := d.index[id]
	if !exists {
		return nil, ErrNotFound
	}

	// Return a copy to prevent external modifications
	return d.copyMessage(msg), nil
}

// Delete removes a message from disk storage.
func (d *DiskStorage) Delete(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	if _, exists := d.index[id]; !exists {
		return ErrNotFound
	}

	// Delete message file
	if err := d.deleteMessageFile(id); err != nil {
		return fmt.Errorf("failed to delete message file: %w", err)
	}

	// Remove from index
	delete(d.index, id)

	// Persist index
	if err := d.saveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	return nil
}

// List returns all message IDs in storage.
func (d *DiskStorage) List() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrStorageClosed
	}

	ids := make([]string, 0, len(d.index))
	for id := range d.index {
		ids = append(ids, id)
	}
	return ids, nil
}

// Count returns the number of messages in storage.
func (d *DiskStorage) Count() (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, ErrStorageClosed
	}

	return int64(len(d.index)), nil
}

// Clear removes all messages from storage.
func (d *DiskStorage) Clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	// Delete all message files
	for id := range d.index {
		d.deleteMessageFile(id)
	}

	// Clear index
	d.index = make(map[string]*MessageData)

	// Persist empty index
	return d.saveIndex()
}

// Close closes the storage and releases resources.
func (d *DiskStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	// Save final index state
	if err := d.saveIndex(); err != nil {
		return fmt.Errorf("failed to save index on close: %w", err)
	}

	d.closed = true
	d.index = nil

	return nil
}

// Update modifies an existing message in storage.
func (d *DiskStorage) Update(msg *MessageData) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	if _, exists := d.index[msg.ID]; !exists {
		return ErrNotFound
	}

	// Deep copy the message
	stored := d.copyMessage(msg)

	// Write updated message to disk
	if err := d.writeMessage(stored); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Update index
	d.index[msg.ID] = stored

	return nil
}

// Exists checks if a message exists in storage.
func (d *DiskStorage) Exists(id string) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return false, ErrStorageClosed
	}

	_, exists := d.index[id]
	return exists, nil
}

// GetByDeduplicationID retrieves a message by its deduplication ID.
func (d *DiskStorage) GetByDeduplicationID(dedupID string) (*MessageData, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrStorageClosed
	}

	for _, msg := range d.index {
		if msg.DeduplicationID == dedupID {
			return d.copyMessage(msg), nil
		}
	}

	return nil, ErrNotFound
}

// GetExpired returns all messages that have exceeded their TTL.
func (d *DiskStorage) GetExpired(now time.Time) ([]*MessageData, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrStorageClosed
	}

	var expired []*MessageData
	for _, msg := range d.index {
		if msg.TTL > 0 && now.After(msg.Timestamp.Add(msg.TTL)) {
			expired = append(expired, d.copyMessage(msg))
		}
	}

	return expired, nil
}

// GetReady returns all messages that are ready for delivery.
func (d *DiskStorage) GetReady(now time.Time) ([]*MessageData, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrStorageClosed
	}

	var ready []*MessageData
	for _, msg := range d.index {
		if msg.DeliveryTime.IsZero() || !now.Before(msg.DeliveryTime) {
			ready = append(ready, d.copyMessage(msg))
		}
	}

	return ready, nil
}

// Compact removes expired messages and optimizes storage.
func (d *DiskStorage) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrStorageClosed
	}

	now := time.Now()
	var toRemove []string

	for id, msg := range d.index {
		if msg.TTL > 0 && now.After(msg.Timestamp.Add(msg.TTL)) {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		d.deleteMessageFile(id)
		delete(d.index, id)
	}

	return d.saveIndex()
}

// writeMessage writes a message to a file.
func (d *DiskStorage) writeMessage(msg *MessageData) error {
	filename := d.messageFilename(msg.ID)

	data, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if d.syncOnWrite {
		flags |= os.O_SYNC
	}

	file, err := os.OpenFile(filename, flags, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if d.syncOnWrite {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

// readMessage reads a message from a file.
func (d *DiskStorage) readMessage(id string) (*MessageData, error) {
	filename := d.messageFilename(id)

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var msg MessageData
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &msg, nil
}

// deleteMessageFile deletes a message file.
func (d *DiskStorage) deleteMessageFile(id string) error {
	filename := d.messageFilename(id)
	return os.Remove(filename)
}

// messageFilename returns the filename for a message.
func (d *DiskStorage) messageFilename(id string) string {
	return filepath.Join(d.dataDir, id+".json")
}

// loadIndex loads the index from disk.
func (d *DiskStorage) loadIndex() error {
	data, err := os.ReadFile(d.indexFile)
	if err != nil {
		return err
	}

	var index map[string]*MessageData
	if err := json.Unmarshal(data, &index); err != nil {
		return fmt.Errorf("failed to unmarshal index: %w", err)
	}

	d.index = index
	if d.index == nil {
		d.index = make(map[string]*MessageData)
	}

	return nil
}

// saveIndex saves the index to disk.
func (d *DiskStorage) saveIndex() error {
	data, err := json.MarshalIndent(d.index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	// Write to temp file first
	tempFile := d.indexFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp index: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, d.indexFile); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename index file: %w", err)
	}

	return nil
}

// copyMessage creates a deep copy of a message.
func (d *DiskStorage) copyMessage(msg *MessageData) *MessageData {
	if msg == nil {
		return nil
	}

	copied := &MessageData{
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

	copy(copied.Payload, msg.Payload)
	for k, v := range msg.Headers {
		copied.Headers[k] = v
	}

	return copied
}

// Stats returns storage statistics.
func (d *DiskStorage) Stats() DiskStorageStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := DiskStorageStats{
		MessageCount: int64(len(d.index)),
		DataDir:      d.dataDir,
		IsClosed:     d.closed,
	}

	// Calculate total size
	filepath.Walk(d.dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			stats.TotalSize += info.Size()
		}
		return nil
	})

	return stats
}

// DiskStorageStats holds statistics for disk storage.
type DiskStorageStats struct {
	MessageCount int64
	TotalSize    int64
	DataDir      string
	IsClosed     bool
}
