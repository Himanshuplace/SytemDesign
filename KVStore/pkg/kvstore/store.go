package kvstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"kv-store/pkg/wal"
)

// Store is the main key-value store implementation.
// It provides a thread-safe, optionally persistent key-value store with
// support for TTL, batch operations, and snapshots.
type Store struct {
	opts    *Options
	data    *ShardedMap
	wal     *wal.WAL
	snap    *SnapshotManager
	metrics *Metrics

	mu          sync.RWMutex
	closed      atomic.Bool
	stopCh      chan struct{}
	doneCh      chan struct{}
	wg          sync.WaitGroup
	deleteCount atomic.Int64 // Track deletes for compaction
}

// Metrics tracks store statistics.
type Metrics struct {
	Gets           atomic.Uint64
	Sets           atomic.Uint64
	Deletes        atomic.Uint64
	Hits           atomic.Uint64
	Misses         atomic.Uint64
	Evictions      atomic.Uint64
	ExpiredCleanup atomic.Uint64
	BatchOps       atomic.Uint64
	Errors         atomic.Uint64
}

// New creates a new key-value store with the given options.
func New(opts ...Option) (*Store, error) {
	// Start with default options
	cfg := DefaultOptions()
	cfg.Apply(opts...)

	// Validate options
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	s := &Store{
		opts:    cfg,
		data:    NewShardedMap(cfg.ShardCount),
		metrics: &Metrics{},
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	// Initialize persistence if enabled
	if cfg.PersistenceMode != PersistenceNone {
		if err := s.initPersistence(); err != nil {
			return nil, fmt.Errorf("failed to initialize persistence: %w", err)
		}
	}

	// Start background workers
	s.startBackgroundWorkers()

	return s, nil
}

// initPersistence initializes WAL and/or snapshot managers based on configuration.
func (s *Store) initPersistence() error {
	var err error

	// Initialize WAL if needed
	if s.opts.PersistenceMode == PersistenceWAL || s.opts.PersistenceMode == PersistenceHybrid {
		walOpts := wal.DefaultOptions(s.opts.DataDir)
		walOpts.SyncOnWrite = s.opts.SyncInterval == 0
		walOpts.SyncInterval = s.opts.SyncInterval

		s.wal, err = wal.Open(walOpts)
		if err != nil {
			return fmt.Errorf("failed to open WAL: %w", err)
		}
	}

	// Initialize snapshot manager if needed
	if s.opts.PersistenceMode == PersistenceSnapshot || s.opts.PersistenceMode == PersistenceHybrid {
		snapOpts := DefaultSnapshotOptions(s.opts.DataDir)
		snapOpts.Compress = s.opts.EnableCompression
		snapOpts.MinInterval = s.opts.SnapshotInterval

		s.snap, err = NewSnapshotManager(snapOpts)
		if err != nil {
			if s.wal != nil {
				s.wal.Close()
			}
			return fmt.Errorf("failed to create snapshot manager: %w", err)
		}
	}

	// Restore data from persistence
	if err := s.restore(); err != nil {
		return fmt.Errorf("failed to restore data: %w", err)
	}

	return nil
}

// restore restores data from WAL and/or snapshots.
func (s *Store) restore() error {
	// First, try to restore from snapshot
	if s.snap != nil {
		entries, meta, err := s.snap.RestoreLatest()
		if err == nil {
			for _, entry := range entries {
				s.data.Set(entry)
			}
			fmt.Printf("Restored %d entries from snapshot %d\n", len(entries), meta.ID)
		} else if !errors.Is(err, ErrSnapshotNotFound) {
			return err
		}
	}

	// Then replay WAL to catch up
	if s.wal != nil {
		err := s.wal.Replay(func(record *wal.Record) error {
			switch record.Type {
			case wal.RecordTypePut:
				entry := NewEntry(string(record.Key), record.Value)
				if record.ExpiresAt > 0 {
					entry.ExpiresAt = time.Unix(0, record.ExpiresAt)
				}
				if !entry.IsExpired() {
					s.data.Set(entry)
				}
			case wal.RecordTypeDelete:
				s.data.Delete(string(record.Key))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("WAL replay failed: %w", err)
		}
	}

	return nil
}

// startBackgroundWorkers starts background maintenance goroutines.
func (s *Store) startBackgroundWorkers() {
	// Expired entry cleanup worker
	if s.opts.CleanupInterval > 0 {
		s.wg.Add(1)
		go s.cleanupWorker()
	}

	// Snapshot worker
	if s.snap != nil && s.opts.SnapshotInterval > 0 {
		s.wg.Add(1)
		go s.snapshotWorker()
	}
}

// cleanupWorker periodically removes expired entries.
func (s *Store) cleanupWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := s.data.CleanupExpiredWithCallback(s.opts.OnExpire)
			if count > 0 {
				s.metrics.ExpiredCleanup.Add(uint64(count))
			}
		case <-s.stopCh:
			return
		}
	}
}

// snapshotWorker periodically creates snapshots.
func (s *Store) snapshotWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.opts.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := s.snap.Create(s.data); err != nil {
				if s.opts.OnError != nil {
					s.opts.OnError(err)
				}
			}
		case <-s.stopCh:
			return
		}
	}
}

// Get retrieves the value for a key.
// Returns ErrKeyNotFound if the key does not exist.
// Returns ErrKeyExpired if the key has expired.
func (s *Store) Get(key string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	if key == "" {
		return nil, ErrEmptyKey
	}

	s.metrics.Gets.Add(1)

	entry, exists := s.data.Get(key)
	if !exists {
		s.metrics.Misses.Add(1)
		return nil, NewKeyError(key, ErrKeyNotFound)
	}

	if entry.IsExpired() {
		s.metrics.Misses.Add(1)
		// Lazy delete expired entry
		s.data.Delete(key)
		return nil, NewKeyError(key, ErrKeyExpired)
	}

	s.metrics.Hits.Add(1)

	// Return a copy of the value to prevent external mutations
	valueCopy := make([]byte, len(entry.Value))
	copy(valueCopy, entry.Value)
	return valueCopy, nil
}

// GetString retrieves the value for a key as a string.
func (s *Store) GetString(key string) (string, error) {
	value, err := s.Get(key)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// GetEntry retrieves the full entry for a key, including metadata.
func (s *Store) GetEntry(key string) (*Entry, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	if key == "" {
		return nil, ErrEmptyKey
	}

	entry, exists := s.data.Get(key)
	if !exists {
		return nil, NewKeyError(key, ErrKeyNotFound)
	}

	if entry.IsExpired() {
		s.data.Delete(key)
		return nil, NewKeyError(key, ErrKeyExpired)
	}

	return entry.Clone(), nil
}

// Set stores a key-value pair.
func (s *Store) Set(key string, value []byte) error {
	return s.SetWithTTL(key, value, s.opts.DefaultTTL)
}

// SetString stores a string value.
func (s *Store) SetString(key, value string) error {
	return s.Set(key, []byte(value))
}

// SetWithTTL stores a key-value pair with a specific TTL.
func (s *Store) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if err := s.validateKeyValue(key, value); err != nil {
		return err
	}

	s.metrics.Sets.Add(1)

	// Check capacity and evict if necessary
	if err := s.ensureCapacity(); err != nil {
		return err
	}

	// Create entry
	var entry *Entry
	if ttl > 0 {
		entry = NewEntryWithTTL(key, value, ttl)
	} else {
		entry = NewEntry(key, value)
	}

	// Write to WAL first (if enabled)
	if s.wal != nil {
		var expiresAt int64
		if !entry.ExpiresAt.IsZero() {
			expiresAt = entry.ExpiresAt.UnixNano()
		}
		if err := s.wal.WritePut([]byte(key), value, expiresAt); err != nil {
			s.metrics.Errors.Add(1)
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	// Store in memory
	old := s.data.Set(entry)

	// Call eviction callback if we're replacing an entry
	if old != nil && s.opts.OnEvict != nil {
		s.opts.OnEvict(old.Key, old.Value)
	}

	return nil
}

// SetNX sets a key-value pair only if the key does not exist.
// Returns true if the key was set, false if it already existed.
func (s *Store) SetNX(key string, value []byte) (bool, error) {
	return s.SetNXWithTTL(key, value, s.opts.DefaultTTL)
}

// SetNXWithTTL sets a key-value pair with TTL only if the key does not exist.
func (s *Store) SetNXWithTTL(key string, value []byte, ttl time.Duration) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}

	if s.Exists(key) {
		return false, nil
	}

	err := s.SetWithTTL(key, value, ttl)
	return err == nil, err
}

// Delete removes a key from the store.
// Returns true if the key existed and was deleted.
func (s *Store) Delete(key string) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}

	if key == "" {
		return false, ErrEmptyKey
	}

	s.metrics.Deletes.Add(1)

	// Write to WAL first (if enabled)
	if s.wal != nil {
		if err := s.wal.WriteDelete([]byte(key)); err != nil {
			s.metrics.Errors.Add(1)
			return false, fmt.Errorf("WAL write failed: %w", err)
		}
	}

	entry, deleted := s.data.Delete(key)
	if deleted {
		s.deleteCount.Add(1)

		// Check if we need compaction
		if s.wal != nil && s.deleteCount.Load() >= int64(s.opts.CompactionThreshold) {
			go s.compact()
		}
	}

	_ = entry // Could be used for callback
	return deleted, nil
}

// Exists checks if a key exists in the store (and is not expired).
func (s *Store) Exists(key string) bool {
	if s.closed.Load() {
		return false
	}

	if key == "" {
		return false
	}

	return s.data.Exists(key)
}

// TTL returns the remaining time-to-live for a key.
// Returns 0 if the key has no expiration or doesn't exist.
func (s *Store) TTL(key string) (time.Duration, error) {
	if s.closed.Load() {
		return 0, ErrStoreClosed
	}

	entry, exists := s.data.Get(key)
	if !exists {
		return 0, NewKeyError(key, ErrKeyNotFound)
	}

	if entry.IsExpired() {
		s.data.Delete(key)
		return 0, NewKeyError(key, ErrKeyExpired)
	}

	return entry.TTL(), nil
}

// Expire sets a new expiration time for a key.
func (s *Store) Expire(key string, ttl time.Duration) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	entry, exists := s.data.Get(key)
	if !exists {
		return NewKeyError(key, ErrKeyNotFound)
	}

	if entry.IsExpired() {
		s.data.Delete(key)
		return NewKeyError(key, ErrKeyExpired)
	}

	entry.SetTTL(ttl)
	return nil
}

// Persist removes the expiration from a key.
func (s *Store) Persist(key string) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	entry, exists := s.data.Get(key)
	if !exists {
		return NewKeyError(key, ErrKeyNotFound)
	}

	entry.ClearExpiration()
	return nil
}

// Keys returns all keys in the store.
func (s *Store) Keys() []string {
	if s.closed.Load() {
		return nil
	}
	return s.data.Keys()
}

// Len returns the number of entries in the store.
func (s *Store) Len() int {
	if s.closed.Load() {
		return 0
	}
	return s.data.Len()
}

// Size returns the total size of all entries in bytes.
func (s *Store) Size() int64 {
	if s.closed.Load() {
		return 0
	}
	return s.data.Size()
}

// Clear removes all entries from the store.
func (s *Store) Clear() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.data.Clear()

	// Clear WAL
	if s.wal != nil {
		if err := s.wal.Clear(); err != nil {
			return fmt.Errorf("failed to clear WAL: %w", err)
		}
	}

	s.deleteCount.Store(0)
	return nil
}

// ApplyBatch applies all operations in a batch atomically.
func (s *Store) ApplyBatch(batch *Batch) (*BatchResult, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	if batch == nil || batch.IsEmpty() {
		return &BatchResult{}, nil
	}

	start := time.Now()
	result := &BatchResult{}

	s.metrics.BatchOps.Add(1)

	// Validate batch
	validator := NewBatchValidator(s.opts.MaxKeySize, s.opts.MaxValueSize, 0, 0)
	if err := validator.Validate(batch); err != nil {
		return nil, err
	}

	ops := batch.Operations()

	// Write to WAL first (if enabled)
	if s.wal != nil {
		records := make([]*wal.Record, 0, len(ops))
		for _, op := range ops {
			var record *wal.Record
			switch op.Type {
			case OpPut:
				record = &wal.Record{
					Type:  wal.RecordTypePut,
					Key:   []byte(op.Key),
					Value: op.Value,
				}
			case OpPutWithTTL:
				expiresAt := time.Now().Add(op.TTL).UnixNano()
				record = &wal.Record{
					Type:      wal.RecordTypePut,
					Key:       []byte(op.Key),
					Value:     op.Value,
					ExpiresAt: expiresAt,
				}
			case OpDelete:
				record = &wal.Record{
					Type: wal.RecordTypeDelete,
					Key:  []byte(op.Key),
				}
			}
			records = append(records, record)
		}

		if err := s.wal.WriteBatch(records); err != nil {
			return nil, fmt.Errorf("WAL batch write failed: %w", err)
		}
	}

	// Apply operations to memory
	for _, op := range ops {
		var err error
		switch op.Type {
		case OpPut:
			entry := NewEntry(op.Key, op.Value)
			s.data.Set(entry)
		case OpPutWithTTL:
			entry := NewEntryWithTTL(op.Key, op.Value, op.TTL)
			s.data.Set(entry)
		case OpDelete:
			s.data.Delete(op.Key)
		default:
			err = fmt.Errorf("unknown operation type: %d", op.Type)
		}

		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, err)
		} else {
			result.Successful++
		}
	}

	batch.markCommitted()
	result.Duration = time.Since(start)

	return result, nil
}

// Iterator returns an iterator over all entries.
func (s *Store) Iterator(opts *IteratorOptions) Iterator {
	if s.closed.Load() {
		return &MapIterator{closed: true}
	}
	return NewMapIterator(s.data, opts)
}

// PrefixIterator returns an iterator over entries with a specific prefix.
func (s *Store) PrefixIterator(prefix string) *PrefixIterator {
	if s.closed.Load() {
		return &PrefixIterator{MapIterator: &MapIterator{closed: true}}
	}
	return NewPrefixIterator(s.data, prefix)
}

// RangeIterator returns an iterator over entries in a key range.
func (s *Store) RangeIterator(start, end string) *RangeIterator {
	if s.closed.Load() {
		return &RangeIterator{MapIterator: &MapIterator{closed: true}}
	}
	return NewRangeIterator(s.data, start, end)
}

// Snapshot creates a point-in-time snapshot.
func (s *Store) Snapshot() (*SnapshotMetadata, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	if s.snap == nil {
		return nil, errors.New("snapshots not enabled")
	}

	return s.snap.Create(s.data)
}

// RestoreSnapshot restores from a snapshot file.
func (s *Store) RestoreSnapshot(path string) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if s.snap == nil {
		return errors.New("snapshots not enabled")
	}

	entries, err := s.snap.Restore(path)
	if err != nil {
		return err
	}

	// Clear current data and restore
	s.data.Clear()
	for _, entry := range entries {
		s.data.Set(entry)
	}

	return nil
}

// compact triggers compaction (creates a snapshot and truncates WAL).
func (s *Store) compact() {
	if s.snap == nil {
		return
	}

	meta, err := s.snap.Create(s.data)
	if err != nil {
		if s.opts.OnError != nil {
			s.opts.OnError(err)
		}
		return
	}

	// Truncate WAL up to the snapshot point
	if s.wal != nil {
		if err := s.wal.Truncate(meta.WALSequence); err != nil {
			if s.opts.OnError != nil {
				s.opts.OnError(err)
			}
		}
	}

	s.deleteCount.Store(0)
}

// validateKeyValue validates a key-value pair against configured limits.
func (s *Store) validateKeyValue(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	if len(key) > s.opts.MaxKeySize {
		return NewValidationError("key", fmt.Sprintf("key size %d exceeds maximum %d", len(key), s.opts.MaxKeySize))
	}

	if len(value) > s.opts.MaxValueSize {
		return NewValidationError("value", fmt.Sprintf("value size %d exceeds maximum %d", len(value), s.opts.MaxValueSize))
	}

	return nil
}

// ensureCapacity checks if the store has capacity and evicts if necessary.
func (s *Store) ensureCapacity() error {
	currentLen := s.data.Len()
	if currentLen < s.opts.MaxEntries {
		return nil
	}

	// Need to evict
	switch s.opts.EvictionPolicy {
	case EvictionNone:
		return errors.New("store is full and eviction is disabled")

	case EvictionLRU:
		entry := s.data.GetLRU()
		if entry != nil {
			s.data.Delete(entry.Key)
			s.metrics.Evictions.Add(1)
			if s.opts.OnEvict != nil {
				s.opts.OnEvict(entry.Key, entry.Value)
			}
		}

	case EvictionLFU:
		entry := s.data.GetLFU()
		if entry != nil {
			s.data.Delete(entry.Key)
			s.metrics.Evictions.Add(1)
			if s.opts.OnEvict != nil {
				s.opts.OnEvict(entry.Key, entry.Value)
			}
		}

	case EvictionRandom:
		keys := s.data.Keys()
		if len(keys) > 0 {
			// Just take the first key (effectively random due to map iteration)
			s.data.Delete(keys[0])
			s.metrics.Evictions.Add(1)
		}

	case EvictionTTL:
		// Find entry closest to expiration
		var candidate *Entry
		s.data.ForEach(func(entry *Entry) bool {
			if entry.HasExpiration() {
				if candidate == nil || entry.TTL() < candidate.TTL() {
					candidate = entry
				}
			}
			return true
		})
		if candidate != nil {
			s.data.Delete(candidate.Key)
			s.metrics.Evictions.Add(1)
			if s.opts.OnEvict != nil {
				s.opts.OnEvict(candidate.Key, candidate.Value)
			}
		}
	}

	return nil
}

// Sync forces a sync of the WAL to disk.
func (s *Store) Sync() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if s.wal != nil {
		return s.wal.Sync()
	}
	return nil
}

// Stats returns statistics about the store.
type StoreStats struct {
	EntryCount     int
	TotalSize      int64
	ShardCount     int
	Gets           uint64
	Sets           uint64
	Deletes        uint64
	Hits           uint64
	Misses         uint64
	HitRate        float64
	Evictions      uint64
	ExpiredCleanup uint64
	Errors         uint64
}

// Stats returns current store statistics.
func (s *Store) Stats() StoreStats {
	hits := s.metrics.Hits.Load()
	misses := s.metrics.Misses.Load()
	total := hits + misses

	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return StoreStats{
		EntryCount:     s.data.Len(),
		TotalSize:      s.data.Size(),
		ShardCount:     s.data.ShardCount(),
		Gets:           s.metrics.Gets.Load(),
		Sets:           s.metrics.Sets.Load(),
		Deletes:        s.metrics.Deletes.Load(),
		Hits:           hits,
		Misses:         misses,
		HitRate:        hitRate,
		Evictions:      s.metrics.Evictions.Load(),
		ExpiredCleanup: s.metrics.ExpiredCleanup.Load(),
		Errors:         s.metrics.Errors.Load(),
	}
}

// Close closes the store and releases all resources.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Stop background workers
	close(s.stopCh)
	s.wg.Wait()

	var lastErr error

	// Create final snapshot if enabled
	if s.snap != nil {
		if _, err := s.snap.Create(s.data); err != nil {
			lastErr = err
		}
	}

	// Close WAL
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// IsClosed returns true if the store has been closed.
func (s *Store) IsClosed() bool {
	return s.closed.Load()
}

// Watch watches for changes to a key.
// Returns a channel that receives the new value when the key changes.
// The watch is cancelled when the context is done.
func (s *Store) Watch(ctx context.Context, key string) <-chan []byte {
	ch := make(chan []byte, 1)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(100 * time.Millisecond) // Poll interval
		defer ticker.Stop()

		var lastValue []byte

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				value, err := s.Get(key)
				if err != nil {
					continue
				}

				if !bytesEqual(value, lastValue) {
					lastValue = value
					select {
					case ch <- value:
					default:
						// Channel full, skip
					}
				}
			}
		}
	}()

	return ch
}

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
