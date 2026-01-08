package kvstore

import (
	"time"
)

// Default configuration values.
const (
	DefaultMaxKeySize          = 1024            // 1 KB
	DefaultMaxValueSize        = 1024 * 1024     // 1 MB
	DefaultMaxEntries          = 1_000_000       // 1 million entries
	DefaultSyncInterval        = 1 * time.Second // Sync WAL every second
	DefaultCleanupInterval     = 1 * time.Minute // Cleanup expired keys every minute
	DefaultSnapshotInterval    = 5 * time.Minute // Snapshot every 5 minutes
	DefaultCompactionThreshold = 1000            // Compact after 1000 deletes
)

// EvictionPolicy defines the policy for evicting entries when the store is full.
type EvictionPolicy int

const (
	// EvictionNone disables automatic eviction; Set will fail when full.
	EvictionNone EvictionPolicy = iota

	// EvictionLRU evicts the least recently used entry.
	EvictionLRU

	// EvictionLFU evicts the least frequently used entry.
	EvictionLFU

	// EvictionRandom evicts a random entry.
	EvictionRandom

	// EvictionTTL evicts entries closest to expiration first.
	EvictionTTL
)

// String returns the string representation of the eviction policy.
func (p EvictionPolicy) String() string {
	switch p {
	case EvictionNone:
		return "none"
	case EvictionLRU:
		return "lru"
	case EvictionLFU:
		return "lfu"
	case EvictionRandom:
		return "random"
	case EvictionTTL:
		return "ttl"
	default:
		return "unknown"
	}
}

// PersistenceMode defines how data is persisted to disk.
type PersistenceMode int

const (
	// PersistenceNone disables persistence (in-memory only).
	PersistenceNone PersistenceMode = iota

	// PersistenceWAL uses write-ahead logging for durability.
	PersistenceWAL

	// PersistenceSnapshot uses periodic snapshots only.
	PersistenceSnapshot

	// PersistenceHybrid uses both WAL and periodic snapshots.
	PersistenceHybrid
)

// String returns the string representation of the persistence mode.
func (m PersistenceMode) String() string {
	switch m {
	case PersistenceNone:
		return "none"
	case PersistenceWAL:
		return "wal"
	case PersistenceSnapshot:
		return "snapshot"
	case PersistenceHybrid:
		return "hybrid"
	default:
		return "unknown"
	}
}

// Options holds the configuration for the KV store.
type Options struct {
	// Storage limits
	MaxKeySize   int // Maximum size of a key in bytes
	MaxValueSize int // Maximum size of a value in bytes
	MaxEntries   int // Maximum number of entries in the store

	// Eviction
	EvictionPolicy EvictionPolicy

	// Persistence
	PersistenceMode     PersistenceMode
	DataDir             string        // Directory for data files
	SyncInterval        time.Duration // Interval for syncing WAL to disk
	SnapshotInterval    time.Duration // Interval for taking snapshots
	CompactionThreshold int           // Number of deletes before compaction

	// TTL and cleanup
	DefaultTTL      time.Duration // Default TTL for entries (0 means no expiration)
	CleanupInterval time.Duration // Interval for cleaning up expired entries

	// Performance tuning
	ShardCount        int  // Number of shards for concurrent access (power of 2)
	EnableCompression bool // Enable value compression
	EnableMetrics     bool // Enable metrics collection

	// Callbacks
	OnEvict  func(key string, value []byte) // Called when an entry is evicted
	OnExpire func(key string, value []byte) // Called when an entry expires
	OnError  func(err error)                // Called when an error occurs
}

// Option is a functional option for configuring the store.
type Option func(*Options)

// DefaultOptions returns the default configuration.
func DefaultOptions() *Options {
	return &Options{
		MaxKeySize:          DefaultMaxKeySize,
		MaxValueSize:        DefaultMaxValueSize,
		MaxEntries:          DefaultMaxEntries,
		EvictionPolicy:      EvictionLRU,
		PersistenceMode:     PersistenceNone,
		DataDir:             "./data",
		SyncInterval:        DefaultSyncInterval,
		SnapshotInterval:    DefaultSnapshotInterval,
		CompactionThreshold: DefaultCompactionThreshold,
		DefaultTTL:          0, // No expiration by default
		CleanupInterval:     DefaultCleanupInterval,
		ShardCount:          32,
		EnableCompression:   false,
		EnableMetrics:       false,
	}
}

// WithMaxKeySize sets the maximum key size.
func WithMaxKeySize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.MaxKeySize = size
		}
	}
}

// WithMaxValueSize sets the maximum value size.
func WithMaxValueSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.MaxValueSize = size
		}
	}
}

// WithMaxEntries sets the maximum number of entries.
func WithMaxEntries(count int) Option {
	return func(o *Options) {
		if count > 0 {
			o.MaxEntries = count
		}
	}
}

// WithEvictionPolicy sets the eviction policy.
func WithEvictionPolicy(policy EvictionPolicy) Option {
	return func(o *Options) {
		o.EvictionPolicy = policy
	}
}

// WithPersistence configures persistence mode and data directory.
func WithPersistence(mode PersistenceMode, dataDir string) Option {
	return func(o *Options) {
		o.PersistenceMode = mode
		if dataDir != "" {
			o.DataDir = dataDir
		}
	}
}

// WithSyncInterval sets the WAL sync interval.
func WithSyncInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.SyncInterval = interval
		}
	}
}

// WithSnapshotInterval sets the snapshot interval.
func WithSnapshotInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.SnapshotInterval = interval
		}
	}
}

// WithCompactionThreshold sets the compaction threshold.
func WithCompactionThreshold(threshold int) Option {
	return func(o *Options) {
		if threshold > 0 {
			o.CompactionThreshold = threshold
		}
	}
}

// WithDefaultTTL sets the default TTL for entries.
func WithDefaultTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.DefaultTTL = ttl
	}
}

// WithCleanupInterval sets the cleanup interval for expired entries.
func WithCleanupInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.CleanupInterval = interval
		}
	}
}

// WithShardCount sets the number of shards.
func WithShardCount(count int) Option {
	return func(o *Options) {
		// Ensure shard count is a power of 2
		if count > 0 {
			// Round up to nearest power of 2
			count--
			count |= count >> 1
			count |= count >> 2
			count |= count >> 4
			count |= count >> 8
			count |= count >> 16
			count++
			o.ShardCount = count
		}
	}
}

// WithCompression enables or disables value compression.
func WithCompression(enabled bool) Option {
	return func(o *Options) {
		o.EnableCompression = enabled
	}
}

// WithMetrics enables or disables metrics collection.
func WithMetrics(enabled bool) Option {
	return func(o *Options) {
		o.EnableMetrics = enabled
	}
}

// WithOnEvict sets the callback for evicted entries.
func WithOnEvict(fn func(key string, value []byte)) Option {
	return func(o *Options) {
		o.OnEvict = fn
	}
}

// WithOnExpire sets the callback for expired entries.
func WithOnExpire(fn func(key string, value []byte)) Option {
	return func(o *Options) {
		o.OnExpire = fn
	}
}

// WithOnError sets the callback for errors.
func WithOnError(fn func(err error)) Option {
	return func(o *Options) {
		o.OnError = fn
	}
}

// Validate checks if the options are valid.
func (o *Options) Validate() error {
	if o.MaxKeySize <= 0 {
		return NewValidationError("MaxKeySize", "must be positive")
	}
	if o.MaxValueSize <= 0 {
		return NewValidationError("MaxValueSize", "must be positive")
	}
	if o.MaxEntries <= 0 {
		return NewValidationError("MaxEntries", "must be positive")
	}
	if o.ShardCount <= 0 {
		return NewValidationError("ShardCount", "must be positive")
	}
	if o.PersistenceMode != PersistenceNone && o.DataDir == "" {
		return NewValidationError("DataDir", "required when persistence is enabled")
	}
	if o.SyncInterval < 0 {
		return NewValidationError("SyncInterval", "cannot be negative")
	}
	if o.SnapshotInterval < 0 {
		return NewValidationError("SnapshotInterval", "cannot be negative")
	}
	if o.CleanupInterval < 0 {
		return NewValidationError("CleanupInterval", "cannot be negative")
	}
	return nil
}

// Apply applies the given options to the base options.
func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}
