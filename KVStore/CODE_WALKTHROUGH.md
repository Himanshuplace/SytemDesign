# Code Walkthrough: Line-by-Line Explanation

This document walks through every file in the key-value store project, explaining each line of code in detail.

---

## Table of Contents

1. [errors.go - Custom Error Types](#1-errorsgo---custom-error-types)
2. [options.go - Configuration](#2-optionsgo---configuration)
3. [entry.go - Data Structure](#3-entrygo---data-structure)
4. [shard.go - Thread-Safe Storage](#4-shardgo---thread-safe-storage)
5. [wal.go - Write-Ahead Log](#5-walgo---write-ahead-log)
6. [store.go - Main Implementation](#6-storego---main-implementation)

---

## 1. errors.go - Custom Error Types

### File Purpose
This file defines all the error types used throughout the key-value store. Good error handling is essential for a robust system.

### Code Breakdown

```go
// Package kvstore provides a thread-safe, persistent key-value store.
package kvstore
```
**What this does:** Declares this file belongs to the `kvstore` package. All files in the same directory with the same package name are compiled together.

**Why:** Go uses packages to organize code. The comment above `package` is a **package documentation comment** - tools like `go doc` display it.

---

```go
import (
    "errors"
    "fmt"
)
```
**What this does:** Imports two standard library packages.

**Why we need them:**
- `errors` - Provides `errors.New()` to create simple errors and `errors.Is()` for comparison
- `fmt` - Provides `fmt.Sprintf()` for formatting strings in error messages

---

```go
// Sentinel errors for common conditions.
var (
    ErrKeyNotFound = errors.New("key not found")
    ErrKeyExpired  = errors.New("key expired")
    ErrStoreClosed = errors.New("store is closed")
    ErrEmptyKey    = errors.New("key cannot be empty")
    ErrNilValue    = errors.New("value cannot be nil")
    // ... more errors
)
```
**What this does:** Creates package-level error variables using `var`.

**Why `var` block:** Groups related declarations together, more readable than separate `var` statements.

**Why `errors.New()`:** Creates a unique error value. Each call to `errors.New()` creates a distinct error, even with the same message.

**Why "sentinel" errors:**
```go
// These are called "sentinel" because they guard/signal specific conditions
// Usage:
if err == ErrKeyNotFound {
    // Handle missing key
}

// Or better (works with wrapped errors):
if errors.Is(err, ErrKeyNotFound) {
    // Handle missing key
}
```

---

```go
// KeyError represents an error associated with a specific key.
type KeyError struct {
    Key string
    Err error
}
```
**What this does:** Defines a **struct type** with two fields.

**Why a struct for errors:** Sometimes you need more context than a simple message. This error tells you WHICH key failed.

**Why exported (capital K):** Names starting with uppercase are exported (public). Other packages can use `KeyError`.

---

```go
// Error implements the error interface.
func (e *KeyError) Error() string {
    return fmt.Sprintf("key %q: %v", e.Key, e.Err)
}
```
**What this does:** Adds a method `Error()` to `KeyError`.

**Syntax breakdown:**
- `func` - keyword to define a function
- `(e *KeyError)` - **receiver**: this method belongs to `*KeyError` type
- `Error() string` - method name and return type
- `fmt.Sprintf("key %q: %v", ...)` - formats a string:
  - `%q` - adds quotes around the key
  - `%v` - formats the error in default way

**Why this matters:** In Go, the `error` interface is defined as:
```go
type error interface {
    Error() string
}
```
By adding `Error() string` method, `KeyError` now satisfies the `error` interface. This means you can use `KeyError` anywhere an `error` is expected.

---

```go
// Unwrap returns the underlying error for errors.Is/As support.
func (e *KeyError) Unwrap() error {
    return e.Err
}
```
**What this does:** Returns the wrapped error inside `KeyError`.

**Why:** Enables error chain inspection with `errors.Is()` and `errors.As()`:
```go
// Create a wrapped error
keyErr := &KeyError{Key: "user:1", Err: ErrKeyNotFound}

// This works because of Unwrap():
errors.Is(keyErr, ErrKeyNotFound) // returns true!

// Without Unwrap(), this would return false
```

---

```go
// NewKeyError creates a new KeyError.
func NewKeyError(key string, err error) *KeyError {
    return &KeyError{Key: key, Err: err}
}
```
**What this does:** A **constructor function** that creates and returns a pointer to a new `KeyError`.

**Why return `*KeyError` (pointer):**
1. Allows the caller to modify the struct if needed
2. Avoids copying the struct (more efficient for larger structs)
3. Convention: error types are usually pointers

**Why a constructor:** Provides a clean API and allows validation/initialization logic if needed later.

---

```go
// IsNotFound checks if the error is a key not found error.
func IsNotFound(err error) bool {
    return errors.Is(err, ErrKeyNotFound)
}
```
**What this does:** Helper function that wraps `errors.Is()`.

**Why helper functions:**
```go
// Without helper (verbose):
if errors.Is(err, kvstore.ErrKeyNotFound) { ... }

// With helper (cleaner):
if kvstore.IsNotFound(err) { ... }
```

---

## 2. options.go - Configuration

### File Purpose
Implements the **Functional Options Pattern** - a clean way to configure complex objects with many optional settings.

### Code Breakdown

```go
const (
    DefaultMaxKeySize          = 1024            // 1 KB
    DefaultMaxValueSize        = 1024 * 1024     // 1 MB
    DefaultMaxEntries          = 1_000_000       // 1 million entries
    DefaultSyncInterval        = 1 * time.Second
    DefaultCleanupInterval     = 1 * time.Minute
    DefaultSnapshotInterval    = 5 * time.Minute
    DefaultCompactionThreshold = 1000
)
```
**What this does:** Defines constants with default values.

**Syntax notes:**
- `const` declares compile-time constants
- `1_000_000` - underscores in numbers improve readability (Go 1.13+)
- `1 * time.Second` - multiplying by `time.Duration` creates a duration

**Why constants:** 
- Single source of truth for defaults
- Can be referenced in documentation
- Compile-time checked

---

```go
type EvictionPolicy int

const (
    EvictionNone EvictionPolicy = iota  // 0
    EvictionLRU                          // 1
    EvictionLFU                          // 2
    EvictionRandom                       // 3
    EvictionTTL                          // 4
)
```
**What this does:** Creates an enumeration (enum) type.

**Syntax breakdown:**
- `type EvictionPolicy int` - creates a new type based on `int`
- `iota` - special constant generator, starts at 0 and increments

**How iota works:**
```go
const (
    A = iota  // 0
    B         // 1 (iota automatically used)
    C         // 2
)
```

**Why custom type instead of plain int:**
```go
// With custom type (GOOD):
func SetPolicy(p EvictionPolicy) { ... }
SetPolicy(EvictionLRU)  // Works
SetPolicy(42)           // Compile error! 42 is int, not EvictionPolicy

// With plain int (BAD):
func SetPolicy(p int) { ... }
SetPolicy(42)           // Compiles, but invalid value!
```

---

```go
func (p EvictionPolicy) String() string {
    switch p {
    case EvictionNone:
        return "none"
    case EvictionLRU:
        return "lru"
    // ... more cases
    default:
        return "unknown"
    }
}
```
**What this does:** Implements the `Stringer` interface.

**Why:** When you print an `EvictionPolicy`, Go automatically calls `String()`:
```go
policy := EvictionLRU
fmt.Println(policy)  // Prints "lru" instead of "1"
```

---

```go
type Options struct {
    // Storage limits
    MaxKeySize   int
    MaxValueSize int
    MaxEntries   int

    // Eviction
    EvictionPolicy EvictionPolicy

    // Persistence
    PersistenceMode     PersistenceMode
    DataDir             string
    SyncInterval        time.Duration
    // ... more fields
    
    // Callbacks
    OnEvict  func(key string, value []byte)
    OnExpire func(key string, value []byte)
    OnError  func(err error)
}
```
**What this does:** Defines a struct to hold all configuration options.

**Why struct fields are grouped:** Comments organize related options together - makes it easier to find what you need.

**Why function fields (callbacks):**
```go
// User can provide custom behavior:
store, _ := New(
    WithOnEvict(func(key string, value []byte) {
        log.Printf("Evicted: %s", key)
    }),
)
```

---

```go
// Option is a functional option for configuring the store.
type Option func(*Options)
```
**What this does:** Defines a **type alias** for a function that modifies Options.

**This is the key insight of the pattern!** An Option is just a function that takes an Options pointer and modifies it.

---

```go
// WithMaxKeySize sets the maximum key size.
func WithMaxKeySize(size int) Option {
    return func(o *Options) {
        if size > 0 {
            o.MaxKeySize = size
        }
    }
}
```
**What this does:** Creates an Option that sets MaxKeySize.

**Step by step:**
1. `WithMaxKeySize(1024)` is called with value 1024
2. It returns a function: `func(o *Options) { o.MaxKeySize = 1024 }`
3. This function is NOT executed yet - just returned
4. Later, when `Apply()` is called, this function runs

**Why the `if size > 0` check:** Validation! Ignores invalid values.

**Trace through the pattern:**
```go
// Step 1: User calls New with options
store, _ := New(
    WithMaxKeySize(512),      // Returns func(o) { o.MaxKeySize = 512 }
    WithEvictionPolicy(LRU),  // Returns func(o) { o.EvictionPolicy = LRU }
)

// Step 2: Inside New()
func New(opts ...Option) (*Store, error) {
    cfg := DefaultOptions()   // Start with defaults
    cfg.Apply(opts...)        // Apply all options
    // cfg now has MaxKeySize=512, EvictionPolicy=LRU, everything else default
}

// Step 3: Apply calls each option function
func (o *Options) Apply(opts ...Option) {
    for _, opt := range opts {
        opt(o)  // Call the function with Options pointer
    }
}
```

---

```go
func (o *Options) Validate() error {
    if o.MaxKeySize <= 0 {
        return NewValidationError("MaxKeySize", "must be positive")
    }
    if o.MaxValueSize <= 0 {
        return NewValidationError("MaxValueSize", "must be positive")
    }
    // ... more checks
    return nil
}
```
**What this does:** Checks if all options have valid values.

**Why return error instead of panic:** Go philosophy - return errors, don't panic. Caller can decide how to handle.

---

## 3. entry.go - Data Structure

### File Purpose
Defines the `Entry` struct - the fundamental unit of storage. Each key-value pair is stored as an Entry.

### Code Breakdown

```go
type Entry struct {
    Key       string
    Value     []byte
    ExpiresAt time.Time
    Metadata  *Metadata
}
```
**Why `[]byte` for Value:**
- Can store ANY binary data (JSON, protobuf, images, etc.)
- `string` is immutable in Go, `[]byte` is mutable (we can modify it)
- Easy conversion: `string(bytes)` or `[]byte(str)`

**Why `time.Time` for ExpiresAt:**
- We store the absolute expiration time, not the TTL duration
- Checking expiration: `time.Now().After(entry.ExpiresAt)`
- Zero value `time.Time{}` means "no expiration"

**Why `*Metadata` (pointer):**
- Optional - can be nil if we don't need metadata
- Avoids copying large struct when passing Entry around
- Single source of truth

---

```go
type Metadata struct {
    CreatedAt   time.Time
    UpdatedAt   time.Time
    AccessedAt  time.Time
    AccessCount atomic.Uint64
    Version     uint64
    Size        int
    Checksum    uint32
}
```
**Why atomic.Uint64 for AccessCount:**
```go
// PROBLEM: Regular increment is NOT thread-safe
count++  // Actually: read count, add 1, write count
         // Another goroutine could interfere between these steps

// SOLUTION: Atomic operations
count.Add(1)  // Single uninterruptible operation
```

**Why Checksum:** To verify data integrity:
```go
// When writing:
checksum := crc32.ChecksumIEEE(data)
// Store checksum with data

// When reading:
newChecksum := crc32.ChecksumIEEE(data)
if newChecksum != storedChecksum {
    // DATA IS CORRUPTED!
}
```

---

```go
func NewEntry(key string, value []byte) *Entry {
    now := time.Now()
    e := &Entry{
        Key:   key,
        Value: value,
        Metadata: &Metadata{
            CreatedAt:  now,
            UpdatedAt:  now,
            AccessedAt: now,
            Version:    1,
            Size:       len(key) + len(value),
        },
    }
    e.Metadata.Checksum = e.computeChecksum()
    return e
}
```
**What `&Entry{...}` does:**
1. Creates a new Entry struct with given values
2. `&` takes its address, returning `*Entry` (pointer to Entry)

**Why pointer return:** Allows modifications and avoids copying.

---

```go
func (e *Entry) IsExpired() bool {
    if e.ExpiresAt.IsZero() {
        return false
    }
    return time.Now().After(e.ExpiresAt)
}
```
**What `IsZero()` checks:** `time.Time{}` (zero value) - means no expiration was set.

**What `After()` does:** Returns true if `time.Now()` is after `ExpiresAt`.

---

```go
func (e *Entry) Clone() *Entry {
    if e == nil {
        return nil
    }

    clone := &Entry{
        Key:       e.Key,
        Value:     make([]byte, len(e.Value)),
        ExpiresAt: e.ExpiresAt,
    }
    copy(clone.Value, e.Value)

    if e.Metadata != nil {
        clone.Metadata = &Metadata{
            CreatedAt:  e.Metadata.CreatedAt,
            // ... copy all fields
        }
        clone.Metadata.AccessCount.Store(e.Metadata.AccessCount.Load())
    }

    return clone
}
```
**Why Clone exists:**
```go
// WITHOUT Clone (DANGEROUS):
entry := store.data["key"]
entry.Value[0] = 'X'  // Modifies the actual stored data!

// WITH Clone (SAFE):
entry := store.data["key"].Clone()
entry.Value[0] = 'X'  // Only modifies the copy
```

**Why `make([]byte, len(e.Value))` then `copy()`:**
```go
// This SHARES the underlying array:
clone.Value = e.Value  // Both point to same memory!

// This creates INDEPENDENT copy:
clone.Value = make([]byte, len(e.Value))  // New memory
copy(clone.Value, e.Value)                 // Copy content
```

---

```go
func (e *Entry) Encode() []byte {
    keyBytes := []byte(e.Key)
    keyLen := len(keyBytes)
    valueLen := len(e.Value)

    totalSize := 4 + keyLen + 4 + valueLen + 8 + 8 + 4
    buf := make([]byte, totalSize)

    offset := 0

    // Key length and key
    binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
    offset += 4
    copy(buf[offset:], keyBytes)
    offset += keyLen

    // ... continue for other fields
    
    return buf
}
```
**What binary.BigEndian.PutUint32 does:**
Writes a 4-byte integer in big-endian format:
```
Number: 1000 (0x000003E8)

Big-endian (what we use):    [00][00][03][E8]
Little-endian (alternative): [E8][03][00][00]
```

**Why track offset:**
We're building one continuous byte slice. `offset` tracks where to write next:
```
buf: [keyLen 4 bytes][key N bytes][valueLen 4 bytes][value M bytes]...
      ^offset=0      ^offset=4     ^offset=4+N       ^offset=8+N
```

**Why prefix lengths:**
When reading back, we need to know where each field ends:
```go
// Reading:
keyLen := binary.BigEndian.Uint32(buf[0:4])  // First 4 bytes = length
key := string(buf[4 : 4+keyLen])              // Next keyLen bytes = key
// Now we know where value starts!
```

---

## 4. shard.go - Thread-Safe Storage

### File Purpose
Implements sharded storage for high-concurrency access. Instead of one big lock, we have many small locks.

### Code Breakdown

```go
type Shard struct {
    data    map[string]*Entry
    mu      sync.RWMutex
    size    int64
    count   int
    maxSize int64
}
```
**What each field does:**
- `data` - The actual key-value map
- `mu` - Lock for thread-safe access
- `size` - Total bytes (for memory tracking)
- `count` - Number of entries
- `maxSize` - Optional limit

**Why sync.RWMutex:**
```go
// Mutex: Only ONE goroutine at a time
mu.Lock()   // Blocks everyone
// ... do stuff
mu.Unlock()

// RWMutex: Multiple READERS or ONE writer
mu.RLock()  // Allows other readers, blocks writers
// ... read stuff
mu.RUnlock()

mu.Lock()   // Blocks everyone (for writing)
// ... write stuff
mu.Unlock()
```

Since reads are usually more common than writes, RWMutex provides better performance.

---

```go
func (s *Shard) Get(key string) (*Entry, bool) {
    s.mu.RLock()
    entry, exists := s.data[key]
    s.mu.RUnlock()

    if !exists {
        return nil, false
    }

    if entry.IsExpired() {
        return nil, false
    }

    entry.Touch()
    return entry, true
}
```
**Why unlock before checking expiration:**
```go
// Keep lock time MINIMAL:
s.mu.RLock()
entry, exists := s.data[key]  // Quick map lookup
s.mu.RUnlock()                 // Release immediately!

// Now other goroutines can access the shard while we:
if entry.IsExpired() { ... }  // Check expiration (no lock needed)
entry.Touch()                  // Update metadata (atomic operations)
```

**The pattern: `value, ok := map[key]`**
```go
entry, exists := s.data[key]
// If key exists: entry = the value, exists = true
// If key missing: entry = nil, exists = false
```

---

```go
func (s *Shard) Set(entry *Entry) *Entry {
    s.mu.Lock()
    defer s.mu.Unlock()

    key := entry.Key
    old, exists := s.data[key]

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
```
**Why `defer s.mu.Unlock()`:**
```go
// WITHOUT defer (error-prone):
s.mu.Lock()
// ... lots of code ...
// What if we return early? Forgot to unlock!
// What if panic? Lock held forever!
s.mu.Unlock()

// WITH defer (safe):
s.mu.Lock()
defer s.mu.Unlock()  // GUARANTEED to run when function exits
// ... lots of code ...
// Can return anywhere, panic anywhere - unlock still happens
```

**Why track size:**
Memory management! If total size exceeds limit, we need to evict entries.

---

```go
type ShardedMap struct {
    shards     []*Shard
    shardCount uint64
    shardMask  uint64
}
```
**Why shardMask:**
Fast modulo operation when shardCount is power of 2:
```go
// Slow:
index := hash % 32

// Fast (when 32 is power of 2):
index := hash & 31  // 31 = 0b11111

// Why it works:
// hash % 32 gives remainder 0-31
// hash & 31 keeps only last 5 bits, also 0-31
```

---

```go
func (m *ShardedMap) getShard(key string) *Shard {
    hash := fnvHash(key)
    return m.shards[hash&m.shardMask]
}
```
**What this does:**
1. Hash the key to get a number
2. Use bitwise AND to get shard index
3. Return that shard

**Example:**
```
key = "user:1"
hash = fnvHash("user:1") = 12345678901234
shardMask = 31 (for 32 shards)
index = 12345678901234 & 31 = 18
return shards[18]
```

---

```go
func fnvHash(key string) uint64 {
    h := fnv.New64a()
    h.Write([]byte(key))
    return h.Sum64()
}
```
**What FNV is:** Fowler-Noll-Vo - a fast, simple hash function.

**Why FNV:**
- Fast to compute
- Good distribution (keys spread evenly)
- Deterministic (same input = same output)

---

## 5. wal.go - Write-Ahead Log

### File Purpose
Provides durability by logging operations to disk BEFORE applying them to memory.

### Code Breakdown

```go
const (
    RecordTypePut    byte = 1
    RecordTypeDelete byte = 2
    RecordTypeBatch  byte = 3
)
```
**Why byte type:** We only need a few values, and byte (1 byte) is more compact than int (8 bytes) in the file.

---

```go
type Record struct {
    Type      byte
    Key       []byte
    Value     []byte
    ExpiresAt int64
    Sequence  uint64
}
```
**What each field stores:**
- `Type` - What operation (PUT, DELETE)
- `Key` - The key being modified
- `Value` - New value (empty for DELETE)
- `ExpiresAt` - Expiration time (0 = never)
- `Sequence` - Order number (for ordering during recovery)

---

```go
const (
    DefaultMaxSegmentSize = 64 * 1024 * 1024  // 64MB
    HeaderSize = 9  // checksum(4) + length(4) + type(1)
)
```
**Why segment files:**
Instead of one huge file, we split into segments:
```
data/
├── wal-0000000000000001.log  (64MB, full)
├── wal-0000000000000002.log  (64MB, full)
└── wal-0000000000000003.log  (30MB, active)
```

Benefits:
- Can delete old segments after snapshot
- Easier to manage smaller files
- Faster recovery (only replay recent segments)

---

```go
type Segment struct {
    id      uint64
    path    string
    file    *os.File
    writer  *bufio.Writer
    size    int64
    maxSize int64
    mu      sync.Mutex
    closed  bool
}
```
**Why bufio.Writer:**
```go
// WITHOUT buffer (slow):
file.Write(data)  // Each write = system call = slow

// WITH buffer (fast):
writer.Write(data)  // Writes to memory buffer
// Only flushes to disk when buffer full or Flush() called
```

---

```go
func (s *Segment) Write(record *Record) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return ErrWALClosed
    }

    payload := record.Encode()
    recordSize := HeaderSize + len(payload)

    if s.size+int64(recordSize) > s.maxSize {
        return ErrSegmentFull
    }

    // Build: [checksum(4)][length(4)][type(1)][payload]
    buf := make([]byte, recordSize)
    
    binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)+1))
    buf[8] = record.Type
    copy(buf[9:], payload)
    
    checksum := crc32.ChecksumIEEE(buf[4:])
    binary.BigEndian.PutUint32(buf[0:4], checksum)

    n, err := s.writer.Write(buf)
    if err != nil {
        return err
    }

    s.size += int64(n)
    return nil
}
```
**Record format on disk:**
```
┌──────────┬────────┬──────┬─────────────────────────────────┐
│ Checksum │ Length │ Type │           Payload               │
│ 4 bytes  │4 bytes │1 byte│  [keyLen][key][valLen][val]... │
└──────────┴────────┴──────┴─────────────────────────────────┘
```

**Why checksum first:** So we can verify the rest of the record before processing it.

---

```go
func (w *WAL) Write(record *Record) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if w.closed {
        return ErrWALClosed
    }

    err := w.active.Write(record)
    if err == ErrSegmentFull {
        if err := w.rotate(); err != nil {
            return err
        }
        err = w.active.Write(record)
    }

    if err != nil {
        return err
    }

    if w.opts.SyncOnWrite {
        return w.active.Sync()
    }

    return nil
}
```
**What rotate() does:** Creates a new segment when current one is full:
```
Before rotate:
  active → segment-001.log (64MB, FULL)

After rotate:
  segments = [segment-001.log, segment-002.log]
  active → segment-002.log (0MB, empty)
```

---

## 6. store.go - Main Implementation

### File Purpose
The main Store struct that ties everything together.

### Code Breakdown

```go
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
    deleteCount atomic.Int64
}
```
**Why atomic.Bool for closed:**
```go
// Check-then-act race condition:
if !s.closed {        // Goroutine A reads: false
                      // Goroutine B reads: false
    s.closed = true   // Both see false, both proceed!
}

// Atomic avoids this:
if s.closed.Load() {  // Atomic read
    return ErrStoreClosed
}
```

**Why channels (stopCh, doneCh):**
Communication between goroutines:
```go
// Main goroutine:
close(s.stopCh)  // Signal "please stop"
<-s.doneCh       // Wait for "I stopped"

// Background goroutine:
select {
case <-s.stopCh:  // Received stop signal
    close(s.doneCh)  // Signal "I stopped"
    return
}
```

**Why WaitGroup:**
```go
// Track how many goroutines are running
s.wg.Add(1)      // "One more goroutine starting"
go func() {
    defer s.wg.Done()  // "This goroutine finished"
    // ... do work
}()

s.wg.Wait()  // Block until all goroutines call Done()
```

---

```go
func New(opts ...Option) (*Store, error) {
    cfg := DefaultOptions()
    cfg.Apply(opts...)

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

    if cfg.PersistenceMode != PersistenceNone {
        if err := s.initPersistence(); err != nil {
            return nil, err
        }
    }

    s.startBackgroundWorkers()

    return s, nil
}
```
**What `...Option` means:** Variadic parameter - accepts zero or more Option values:
```go
New()                           // No options
New(WithMaxKeySize(512))        // One option
New(WithMaxKeySize(512), WithEvictionPolicy(LRU))  // Multiple
```

**What `make(chan struct{})` creates:** An unbuffered channel of empty structs. Used for signaling only (no data transferred).

---

```go
func (s *Store) SetWithTTL(key string, value []byte, ttl time.Duration) error {
    // 1. Check closed
    if s.closed.Load() {
        return ErrStoreClosed
    }

    // 2. Validate
    if err := s.validateKeyValue(key, value); err != nil {
        return err
    }

    // 3. Metrics
    s.metrics.Sets.Add(1)

    // 4. Capacity check
    if err := s.ensureCapacity(); err != nil {
        return err
    }

    // 5. Create entry
    var entry *Entry
    if ttl > 0 {
        entry = NewEntryWithTTL(key, value, ttl)
    } else {
        entry = NewEntry(key, value)
    }

    // 6. WAL FIRST! (Critical for durability)
    if s.wal != nil {
        var expiresAt int64
        if !entry.ExpiresAt.IsZero() {
            expiresAt = entry.ExpiresAt.UnixNano()
        }
        if err := s.wal.WritePut([]byte(key), value, expiresAt); err != nil {
            return fmt.Errorf("WAL write failed: %w", err)
        }
    }

    // 7. THEN memory (only after WAL succeeds)
    s.data.Set(entry)

    return nil
}
```
**THE CRITICAL ORDER:**
```
CORRECT ORDER:
1. Write to WAL (disk) ← survives crash
2. Update memory       ← fast access

If crash after step 1: WAL has it, recovered on restart
If crash after step 2: All good

WRONG ORDER:
1. Update memory       ← lost on crash!
2. Write to WAL
If crash after step 1: Data LOST! Nothing in WAL
```

---

```go
func (s *Store) Close() error {
    // Atomic check-and-set
    if !s.closed.CompareAndSwap(false, true) {
        return nil  // Already closed
    }

    // Signal background workers to stop
    close(s.stopCh)
    
    // Wait for them to finish
    s.wg.Wait()

    // Final cleanup
    if s.snap != nil {
        s.snap.Create(s.data)  // Save final state
    }
    if s.wal != nil {
        s.wal.Close()
    }

    return nil
}
```
**Why CompareAndSwap:**
```go
// Atomic "check if false, then set to true"
// Returns true if we did the swap (were first)
// Returns false if already true (someone else closed)

if !s.closed.CompareAndSwap(false, true) {
    return nil  // Another goroutine already closing
}
// Only ONE goroutine reaches here
```

---

## Summary: Key Concepts to Remember

### 1. Thread Safety
- Use `sync.RWMutex` for read-heavy workloads
- Use `atomic` types for simple counters/flags
- Keep lock duration minimal
- Return copies, not references

### 2. Durability
- WAL: Write to disk BEFORE memory
- Snapshots: Periodic full backups
- Checksums: Verify data integrity

### 3. Go Patterns
- Functional Options for configuration
- Error wrapping with `%w`
- `defer` for cleanup
- Channels for signaling
- Context for cancellation

### 4. Design Trade-offs
- Sharding vs simplicity → chose sharding for performance
- Memory vs disk → chose memory-first with optional persistence
- Copy vs reference → chose copy for safety

---

## Exercises to Deepen Understanding

1. **Add a new option:** Create `WithReadTimeout(duration)` option
2. **Add a new error:** Create `ErrValueTooLarge` error type
3. **Modify eviction:** Implement a simple random eviction policy
4. **Add logging:** Add a logger option that logs all operations
5. **Run benchmarks:** `go test -bench=. ./pkg/kvstore/`

---

Congratulations! You now understand every component of a production-quality key-value store! 🎉