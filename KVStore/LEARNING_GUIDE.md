# Complete Learning Guide: Building a Key-Value Store in Go

## Table of Contents

1. [Introduction: What is a Key-Value Store?](#1-introduction-what-is-a-key-value-store)
2. [Why Build One From Scratch?](#2-why-build-one-from-scratch)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Deep Dive: Each Component Explained](#4-deep-dive-each-component-explained)
5. [Go Patterns and Idioms Used](#5-go-patterns-and-idioms-used)
6. [The Complete Flow: What Happens When You Call Set/Get](#6-the-complete-flow)
7. [Design Decisions and Trade-offs](#7-design-decisions-and-trade-offs)
8. [Summary](#8-summary)

---

## 1. Introduction: What is a Key-Value Store?

### The Simplest Explanation

A key-value store is like a dictionary or a phone book:
- You have a **key** (like a person's name)
- You have a **value** (like their phone number)
- You can **look up** the value using the key

```
Key         →  Value
─────────────────────────
"user:1"    →  "Alice"
"user:2"    →  "Bob"
"session:x" →  "logged_in"
```

### In Programming Terms

```go
// This is essentially what we're building:
store["user:1"] = "Alice"     // Set
name := store["user:1"]        // Get → "Alice"
delete(store, "user:1")        // Delete
```

### Why Not Just Use a Go Map?

Go has built-in maps, so why build a key-value store? Because maps alone lack:

| Feature | Go Map | Our KV Store |
|---------|--------|--------------|
| Thread-safety | ❌ Crashes with concurrent access | ✅ Safe |
| Persistence | ❌ Lost when program ends | ✅ Saved to disk |
| TTL (expiration) | ❌ No | ✅ Keys can auto-expire |
| Size limits | ❌ Grows forever | ✅ Configurable limits |
| Eviction | ❌ No | ✅ LRU, LFU policies |
| Metrics | ❌ No | ✅ Hits, misses, etc. |

---

## 2. Why Build One From Scratch?

### Learning Goals

Building a KV store teaches you:

1. **Concurrency** - How to make data structures thread-safe
2. **Persistence** - How databases survive restarts
3. **Data Structures** - Hash maps, linked lists, trees
4. **System Design** - Trade-offs between speed, durability, memory
5. **Go Idioms** - Interfaces, goroutines, channels, error handling

### Real-World Usage

Key-value stores are everywhere:
- **Redis** - Caching, sessions
- **Memcached** - Caching
- **etcd** - Configuration storage (used by Kubernetes)
- **LevelDB/RocksDB** - Embedded storage (used by Chrome, Bitcoin)

---

## 3. High-Level Architecture

### The Big Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                         YOUR APPLICATION                         │
│                    store.Set("key", "value")                    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                           STORE                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      API LAYER                            │  │
│  │   Get(), Set(), Delete(), Exists(), TTL(), Batch()       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                │                                 │
│         ┌──────────────────────┼──────────────────────┐         │
│         ▼                      ▼                      ▼         │
│  ┌────────────┐      ┌─────────────────┐      ┌────────────┐   │
│  │ VALIDATION │      │  SHARDED MAP    │      │   METRICS  │   │
│  │ Key size   │      │ (In-Memory)     │      │ Hits/Miss  │   │
│  │ Value size │      │ ┌─────┬─────┐   │      │ Stats      │   │
│  └────────────┘      │ │Shard│Shard│...│      └────────────┘   │
│                      │ └─────┴─────┘   │                        │
│                      └─────────────────┘                        │
│                                │                                 │
│         ┌──────────────────────┼──────────────────────┐         │
│         ▼                      ▼                      ▼         │
│  ┌────────────┐      ┌─────────────────┐      ┌────────────┐   │
│  │    WAL     │      │    SNAPSHOT     │      │  CLEANUP   │   │
│  │ Write-Ahead│      │   Manager       │      │  Worker    │   │
│  │    Log     │      │ (Periodic Save) │      │ (Expired)  │   │
│  └────────────┘      └─────────────────┘      └────────────┘   │
│         │                      │                                 │
│         ▼                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      DISK STORAGE                        │   │
│  │   wal-0001.log  wal-0002.log  snapshot-0001.snap        │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Overview

| Component | Purpose | File |
|-----------|---------|------|
| Store | Main entry point, coordinates everything | `store.go` |
| ShardedMap | Thread-safe in-memory storage | `shard.go` |
| Entry | Single key-value pair with metadata | `entry.go` |
| WAL | Durability - logs every write | `wal/wal.go` |
| Snapshot | Point-in-time backup | `snapshot.go` |
| Options | Configuration | `options.go` |
| Errors | Custom error types | `errors.go` |
| Iterator | Traversing data | `iterator.go` |
| Batch | Multiple operations at once | `batch.go` |

---

## 4. Deep Dive: Each Component Explained

### 4.1 Errors (`errors.go`)

#### Why Start With Errors?

Errors are the **foundation** of robust Go code. We define them first because every other component uses them.

#### The Code Explained

```go
// Package kvstore provides a thread-safe, persistent key-value store.
package kvstore

import (
    "errors"
    "fmt"
)
```

**Why this import?**
- `errors` - Go's standard error creation package
- `fmt` - For formatting error messages

```go
// Sentinel errors for common conditions.
var (
    ErrKeyNotFound = errors.New("key not found")
    ErrKeyExpired  = errors.New("key expired")
    ErrStoreClosed = errors.New("store is closed")
    // ... more errors
)
```

**What are "Sentinel Errors"?**

Sentinel errors are **pre-defined error values** that represent specific conditions. They're called "sentinel" because they act as guards/signals.

```go
// How they're used:
value, err := store.Get("missing-key")
if errors.Is(err, ErrKeyNotFound) {
    // Handle missing key specifically
}
```

**Why use sentinel errors instead of strings?**

```go
// BAD: String comparison is fragile
if err.Error() == "key not found" { ... }

// GOOD: Type-safe comparison
if errors.Is(err, ErrKeyNotFound) { ... }
```

#### Custom Error Types

```go
// KeyError represents an error associated with a specific key.
type KeyError struct {
    Key string
    Err error
}
```

**Why create a struct for errors?**

Because sometimes you need **context**. When a key operation fails, you want to know **which key** failed.

```go
// Error implements the error interface.
func (e *KeyError) Error() string {
    return fmt.Sprintf("key %q: %v", e.Key, e.Err)
}
```

**What is `Error() string`?**

In Go, any type that has an `Error() string` method implements the `error` interface:

```go
// This is Go's built-in error interface:
type error interface {
    Error() string
}
```

By adding `Error() string` to our struct, we make it usable as an error.

```go
// Unwrap returns the underlying error for errors.Is/As support.
func (e *KeyError) Unwrap() error {
    return e.Err
}
```

**Why `Unwrap()`?**

This enables **error wrapping chains**. You can wrap an error in another error and still check the original:

```go
// Create a wrapped error
err := &KeyError{Key: "user:1", Err: ErrKeyNotFound}

// This works because of Unwrap():
errors.Is(err, ErrKeyNotFound) // → true
```

#### Helper Functions

```go
func IsNotFound(err error) bool {
    return errors.Is(err, ErrKeyNotFound)
}
```

**Why create helper functions?**

They make code more readable:

```go
// Without helper:
if errors.Is(err, kvstore.ErrKeyNotFound) { ... }

// With helper:
if kvstore.IsNotFound(err) { ... }
```

---

### 4.2 Options (`options.go`)

#### The Functional Options Pattern

This is one of the most important Go patterns. Let's understand why it exists.

**The Problem: How to Configure Complex Objects?**

```go
// Approach 1: Many parameters (BAD)
func New(maxKeySize, maxValueSize, maxEntries int, 
         eviction EvictionPolicy, dataDir string, ...) *Store

// This is terrible because:
// - Order matters and is easy to get wrong
// - Adding new options breaks existing code
// - Most options have reasonable defaults
```

```go
// Approach 2: Config struct (OKAY)
type Config struct {
    MaxKeySize   int
    MaxValueSize int
    // ...
}
func New(cfg Config) *Store

// This works but:
// - Zero values might be invalid (is MaxKeySize=0 intentional?)
// - Hard to distinguish "not set" from "set to zero"
```

```go
// Approach 3: Functional Options (BEST)
func New(opts ...Option) *Store

store, _ := New(
    WithMaxKeySize(1024),
    WithMaxValueSize(1024*1024),
)
// - Only specify what you want to change
// - Self-documenting
// - Easy to extend
// - Order doesn't matter
```

#### The Code Explained

```go
// Default configuration values.
const (
    DefaultMaxKeySize   = 1024            // 1 KB
    DefaultMaxValueSize = 1024 * 1024     // 1 MB
    DefaultMaxEntries   = 1_000_000       // 1 million entries
    // ...
)
```

**Why define constants?**
- They're reusable
- They document what "default" means
- They can be referenced in tests and documentation

```go
// EvictionPolicy defines the policy for evicting entries when the store is full.
type EvictionPolicy int

const (
    EvictionNone EvictionPolicy = iota  // 0
    EvictionLRU                          // 1
    EvictionLFU                          // 2
    EvictionRandom                       // 3
    EvictionTTL                          // 4
)
```

**What is `iota`?**

`iota` is Go's auto-incrementing constant generator:
- First constant gets 0
- Second gets 1
- And so on...

It's perfect for enumerations.

```go
// String returns the string representation of the eviction policy.
func (p EvictionPolicy) String() string {
    switch p {
    case EvictionNone:
        return "none"
    // ...
    }
}
```

**Why implement `String()`?**

When you print an EvictionPolicy, Go automatically calls String():

```go
fmt.Println(EvictionLRU) // Prints: "lru" (not "1")
```

#### The Options Struct

```go
type Options struct {
    MaxKeySize   int
    MaxValueSize int
    MaxEntries   int
    // ... many more fields
}
```

This holds all the configuration. But how do we set individual fields elegantly?

#### The Option Type

```go
// Option is a functional option for configuring the store.
type Option func(*Options)
```

**This is the key insight!**

An `Option` is a **function that modifies Options**. Let's see how:

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

**Let's trace through this:**

1. `WithMaxKeySize(1024)` is called
2. It returns a function: `func(o *Options) { o.MaxKeySize = 1024 }`
3. This function is stored and called later

```go
// DefaultOptions returns the default configuration.
func DefaultOptions() *Options {
    return &Options{
        MaxKeySize:     DefaultMaxKeySize,
        MaxValueSize:   DefaultMaxValueSize,
        // ... all defaults
    }
}
```

```go
// Apply applies the given options to the base options.
func (o *Options) Apply(opts ...Option) {
    for _, opt := range opts {
        opt(o)  // Call each option function with the Options