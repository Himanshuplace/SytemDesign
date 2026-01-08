package kvstore

import (
	"hash/fnv"
	"sync"
	"time"
)

// Shard represents a single partition of the key-value store.
// Each shard has its own lock for fine-grained concurrency control.
type Shard struct {
	data    map[string]*Entry
	mu      sync.RWMutex
	size    int64 // Total size of all entries in bytes
	count   int   // Number of entries
	maxSize int64 // Maximum size in bytes (0 = unlimited)
}

// newShard creates a new shard with the specified initial capacity.
func newShard(initialCapacity int) *Shard {
	return &Shard{
		data: make(map[string]*Entry, initialCapacity),
	}
}

// Get retrieves an entry from the shard.
// Returns the entry and true if found, nil and false otherwise.
func (s *Shard) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	entry, exists := s.data[key]
	s.mu.RUnlock()

	if !exists {
		return nil, false
	}

	// Check expiration
	if entry.IsExpired() {
		return nil, false
	}

	// Update access metadata
	entry.Touch()

	return entry, true
}

// Set stores an entry in the shard.
// Returns the previous entry if it existed, nil otherwise.
func (s *Shard) Set(entry *Entry) *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := entry.Key
	old, exists := s.data[key]

	// Update size tracking
	if exists {
		s.size -= int64(old.Size())
	} else {
		s.count++
	}
	s.size += int64(entry.Size())

	s.data[key] = entry

	if exists {
		return old
	}
	return nil
}

// Delete removes an entry from the shard.
// Returns the deleted entry and true if it existed, nil and false otherwise.
func (s *Shard) Delete(key string) (*Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists {
		return nil, false
	}

	delete(s.data, key)
	s.size -= int64(entry.Size())
	s.count--

	return entry, true
}

// Exists checks if a key exists in the shard (and is not expired).
func (s *Shard) Exists(key string) bool {
	s.mu.RLock()
	entry, exists := s.data[key]
	s.mu.RUnlock()

	if !exists {
		return false
	}

	return !entry.IsExpired()
}

// Len returns the number of entries in the shard.
func (s *Shard) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Size returns the total size of entries in bytes.
func (s *Shard) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Keys returns all keys in the shard.
func (s *Shard) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Entries returns all non-expired entries in the shard.
func (s *Shard) Entries() []*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]*Entry, 0, len(s.data))
	for _, entry := range s.data {
		if !entry.IsExpired() {
			entries = append(entries, entry.Clone())
		}
	}
	return entries
}

// Clear removes all entries from the shard.
func (s *Shard) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*Entry)
	s.size = 0
	s.count = 0
}

// CleanupExpired removes all expired entries from the shard.
// Returns the number of entries removed and their keys.
func (s *Shard) CleanupExpired() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	for key, entry := range s.data {
		if entry.IsExpired() {
			expired = append(expired, key)
			s.size -= int64(entry.Size())
			s.count--
			delete(s.data, key)
		}
	}

	return len(expired), expired
}

// GetExpired returns all expired entries without removing them.
func (s *Shard) GetExpired() []*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var expired []*Entry
	for _, entry := range s.data {
		if entry.IsExpired() {
			expired = append(expired, entry.Clone())
		}
	}
	return expired
}

// ForEach iterates over all entries in the shard.
// The callback receives a copy of each entry.
// If the callback returns false, iteration stops.
func (s *Shard) ForEach(fn func(entry *Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, entry := range s.data {
		if !entry.IsExpired() {
			if !fn(entry.Clone()) {
				return
			}
		}
	}
}

// GetOldest returns the oldest entry by creation time.
func (s *Shard) GetOldest() *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var oldest *Entry
	var oldestTime time.Time

	for _, entry := range s.data {
		if entry.IsExpired() {
			continue
		}
		if oldest == nil || (entry.Metadata != nil && entry.Metadata.CreatedAt.Before(oldestTime)) {
			oldest = entry
			if entry.Metadata != nil {
				oldestTime = entry.Metadata.CreatedAt
			}
		}
	}

	if oldest != nil {
		return oldest.Clone()
	}
	return nil
}

// GetLRU returns the least recently used entry.
func (s *Shard) GetLRU() *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lru *Entry
	var lruTime time.Time

	for _, entry := range s.data {
		if entry.IsExpired() || entry.Metadata == nil {
			continue
		}
		if lru == nil || entry.Metadata.AccessedAt.Before(lruTime) {
			lru = entry
			lruTime = entry.Metadata.AccessedAt
		}
	}

	if lru != nil {
		return lru.Clone()
	}
	return nil
}

// GetLFU returns the least frequently used entry.
func (s *Shard) GetLFU() *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lfu *Entry
	var lfuCount uint64 = ^uint64(0) // Max uint64

	for _, entry := range s.data {
		if entry.IsExpired() || entry.Metadata == nil {
			continue
		}
		count := entry.Metadata.AccessCount.Load()
		if count < lfuCount {
			lfu = entry
			lfuCount = count
		}
	}

	if lfu != nil {
		return lfu.Clone()
	}
	return nil
}

// ShardedMap is a concurrent map implementation using sharding.
type ShardedMap struct {
	shards     []*Shard
	shardCount uint64
	shardMask  uint64 // For fast modulo using bitwise AND
}

// NewShardedMap creates a new sharded map with the specified number of shards.
// shardCount should be a power of 2 for optimal performance.
func NewShardedMap(shardCount int) *ShardedMap {
	if shardCount <= 0 {
		shardCount = 32
	}

	// Round up to nearest power of 2
	shardCount = roundUpToPowerOf2(shardCount)

	shards := make([]*Shard, shardCount)
	for i := range shards {
		shards[i] = newShard(64) // Initial capacity per shard
	}

	return &ShardedMap{
		shards:     shards,
		shardCount: uint64(shardCount),
		shardMask:  uint64(shardCount - 1),
	}
}

// roundUpToPowerOf2 rounds n up to the nearest power of 2.
func roundUpToPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// getShard returns the shard for a given key.
func (m *ShardedMap) getShard(key string) *Shard {
	hash := fnvHash(key)
	return m.shards[hash&m.shardMask]
}

// fnvHash computes FNV-1a hash of the key.
func fnvHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// Get retrieves an entry by key.
func (m *ShardedMap) Get(key string) (*Entry, bool) {
	return m.getShard(key).Get(key)
}

// Set stores an entry.
func (m *ShardedMap) Set(entry *Entry) *Entry {
	return m.getShard(entry.Key).Set(entry)
}

// Delete removes an entry by key.
func (m *ShardedMap) Delete(key string) (*Entry, bool) {
	return m.getShard(key).Delete(key)
}

// Exists checks if a key exists.
func (m *ShardedMap) Exists(key string) bool {
	return m.getShard(key).Exists(key)
}

// Len returns the total number of entries across all shards.
func (m *ShardedMap) Len() int {
	total := 0
	for _, shard := range m.shards {
		total += shard.Len()
	}
	return total
}

// Size returns the total size of all entries in bytes.
func (m *ShardedMap) Size() int64 {
	var total int64
	for _, shard := range m.shards {
		total += shard.Size()
	}
	return total
}

// Keys returns all keys across all shards.
func (m *ShardedMap) Keys() []string {
	var keys []string
	for _, shard := range m.shards {
		keys = append(keys, shard.Keys()...)
	}
	return keys
}

// Entries returns all entries across all shards.
func (m *ShardedMap) Entries() []*Entry {
	var entries []*Entry
	for _, shard := range m.shards {
		entries = append(entries, shard.Entries()...)
	}
	return entries
}

// Clear removes all entries from all shards.
func (m *ShardedMap) Clear() {
	for _, shard := range m.shards {
		shard.Clear()
	}
}

// CleanupExpired removes expired entries from all shards.
// Returns the total number of entries removed.
func (m *ShardedMap) CleanupExpired() int {
	total := 0
	for _, shard := range m.shards {
		count, _ := shard.CleanupExpired()
		total += count
	}
	return total
}

// CleanupExpiredWithCallback removes expired entries and calls the callback for each.
func (m *ShardedMap) CleanupExpiredWithCallback(callback func(key string, value []byte)) int {
	total := 0
	for _, shard := range m.shards {
		expired := shard.GetExpired()
		for _, entry := range expired {
			if _, deleted := shard.Delete(entry.Key); deleted {
				total++
				if callback != nil {
					callback(entry.Key, entry.Value)
				}
			}
		}
	}
	return total
}

// ForEach iterates over all entries in all shards.
func (m *ShardedMap) ForEach(fn func(entry *Entry) bool) {
	for _, shard := range m.shards {
		shouldContinue := true
		shard.ForEach(func(entry *Entry) bool {
			shouldContinue = fn(entry)
			return shouldContinue
		})
		if !shouldContinue {
			return
		}
	}
}

// GetLRU returns the least recently used entry across all shards.
func (m *ShardedMap) GetLRU() *Entry {
	var lru *Entry
	var lruTime time.Time

	for _, shard := range m.shards {
		candidate := shard.GetLRU()
		if candidate == nil || candidate.Metadata == nil {
			continue
		}
		if lru == nil || candidate.Metadata.AccessedAt.Before(lruTime) {
			lru = candidate
			lruTime = candidate.Metadata.AccessedAt
		}
	}

	return lru
}

// GetLFU returns the least frequently used entry across all shards.
func (m *ShardedMap) GetLFU() *Entry {
	var lfu *Entry
	var lfuCount uint64 = ^uint64(0)

	for _, shard := range m.shards {
		candidate := shard.GetLFU()
		if candidate == nil || candidate.Metadata == nil {
			continue
		}
		count := candidate.Metadata.AccessCount.Load()
		if count < lfuCount {
			lfu = candidate
			lfuCount = count
		}
	}

	return lfu
}

// ShardStats contains statistics for a single shard.
type ShardStats struct {
	Index int
	Count int
	Size  int64
}

// Stats returns statistics for each shard.
func (m *ShardedMap) Stats() []ShardStats {
	stats := make([]ShardStats, len(m.shards))
	for i, shard := range m.shards {
		stats[i] = ShardStats{
			Index: i,
			Count: shard.Len(),
			Size:  shard.Size(),
		}
	}
	return stats
}

// ShardCount returns the number of shards.
func (m *ShardedMap) ShardCount() int {
	return int(m.shardCount)
}
