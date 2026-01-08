// Package main demonstrates the usage of the key-value store.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kv-store/pkg/kvstore"
)

func main() {
	fmt.Println("=== Go Key-Value Store Demo ===")
	fmt.Println()

	// Create a temporary directory for persistence
	dataDir := "./kv-data"
	// defer os.RemoveAll(dataDir)

	// Create the store with various options
	store, err := kvstore.New(
		kvstore.WithMaxKeySize(1024),
		kvstore.WithMaxValueSize(1024*1024), // 1MB max value
		kvstore.WithMaxEntries(100000),
		kvstore.WithEvictionPolicy(kvstore.EvictionLRU),
		kvstore.WithPersistence(kvstore.PersistenceHybrid, dataDir),
		kvstore.WithSyncInterval(time.Second),
		kvstore.WithSnapshotInterval(5*time.Minute),
		kvstore.WithCleanupInterval(30*time.Second),
		kvstore.WithDefaultTTL(0), // No default TTL
		kvstore.WithShardCount(32),
		kvstore.WithMetrics(true),
		kvstore.WithOnEvict(func(key string, value []byte) {
			fmt.Printf("  [Evicted] Key: %s\n", key)
		}),
		kvstore.WithOnExpire(func(key string, value []byte) {
			fmt.Printf("  [Expired] Key: %s\n", key)
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Demonstrate basic operations
	demoBasicOperations(store)

	// Demonstrate TTL functionality
	demoTTL(store)

	// Demonstrate batch operations
	demoBatchOperations(store)

	// Demonstrate iterators
	demoIterators(store)

	// Demonstrate concurrent access
	demoConcurrentAccess(store)

	// Demonstrate watch functionality
	demoWatch(store)

	// Demonstrate snapshots
	demoSnapshots(store)

	// Print final statistics
	printStats(store)

	// Handle graceful shutdown
	fmt.Println("\n=== Press Ctrl+C to exit ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down gracefully...")
}

func demoBasicOperations(store *kvstore.Store) {
	fmt.Println("--- Basic Operations ---")

	// Set values
	if err := store.Set("user:1:name", []byte("Alice")); err != nil {
		log.Printf("Set failed: %v", err)
	}
	if err := store.SetString("user:1:email", "alice@example.com"); err != nil {
		log.Printf("SetString failed: %v", err)
	}
	if err := store.Set("user:1:age", []byte("30")); err != nil {
		log.Printf("Set failed: %v", err)
	}

	// Get values
	name, err := store.Get("user:1:name")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else {
		fmt.Printf("  Name: %s\n", name)
	}

	email, err := store.GetString("user:1:email")
	if err != nil {
		log.Printf("GetString failed: %v", err)
	} else {
		fmt.Printf("  Email: %s\n", email)
	}

	// Check existence
	fmt.Printf("  Key 'user:1:name' exists: %v\n", store.Exists("user:1:name"))
	fmt.Printf("  Key 'user:999:name' exists: %v\n", store.Exists("user:999:name"))

	// Get entry with metadata
	entry, err := store.GetEntry("user:1:name")
	if err == nil && entry.Metadata != nil {
		fmt.Printf("  Entry metadata - Created: %v, Version: %d\n",
			entry.Metadata.CreatedAt.Format(time.RFC3339),
			entry.Metadata.Version)
	}

	// SetNX (set if not exists)
	wasSet, _ := store.SetNX("user:1:name", []byte("Bob"))
	fmt.Printf("  SetNX on existing key: %v (should be false)\n", wasSet)

	wasSet, _ = store.SetNX("user:2:name", []byte("Bob"))
	fmt.Printf("  SetNX on new key: %v (should be true)\n", wasSet)

	// Delete
	deleted, _ := store.Delete("user:1:age")
	fmt.Printf("  Deleted 'user:1:age': %v\n", deleted)

	fmt.Printf("  Total entries: %d\n", store.Len())
	fmt.Printf("  Total size: %d bytes\n", store.Size())
	fmt.Println()
}

func demoTTL(store *kvstore.Store) {
	fmt.Println("--- TTL Operations ---")

	// Set with TTL
	store.SetWithTTL("session:abc123", []byte("user_data"), 5*time.Second)
	fmt.Printf("  Set 'session:abc123' with 5s TTL\n")

	// Check TTL
	ttl, err := store.TTL("session:abc123")
	if err == nil {
		fmt.Printf("  TTL remaining: %v\n", ttl.Round(time.Millisecond))
	}

	// Expire - set TTL on existing key
	store.Set("temp:data", []byte("temporary"))
	store.Expire("temp:data", 3*time.Second)
	fmt.Printf("  Set 'temp:data' then added 3s expiration\n")

	// Persist - remove TTL
	store.SetWithTTL("persistent:data", []byte("was_temporary"), time.Hour)
	store.Persist("persistent:data")
	ttl, _ = store.TTL("persistent:data")
	fmt.Printf("  'persistent:data' TTL after Persist: %v (0 = no expiration)\n", ttl)

	// Wait for expiration
	fmt.Printf("  Waiting 3 seconds for 'temp:data' to expire...\n")
	time.Sleep(3500 * time.Millisecond)

	if !store.Exists("temp:data") {
		fmt.Printf("  'temp:data' has expired!\n")
	}

	fmt.Println()
}

func demoBatchOperations(store *kvstore.Store) {
	fmt.Println("--- Batch Operations ---")

	// Using Batch directly
	batch := kvstore.NewBatch()
	for i := 0; i < 10000000; i++ {
		batch.Put(fmt.Sprintf("batch:key:%d", i), []byte(fmt.Sprintf("value-%d", i)))
	}t
	batch.Delete("batch:key:0") // Delete one of them

	result, err := store.ApplyBatch(batch)
	if err != nil {
		log.Printf("ApplyBatch failed: %v", err)
	} else {
		fmt.Printf("  Batch applied: %d successful, %d failed, duration: %v\n",
			result.Successful, result.Failed, result.Duration)
	}

	// Using BatchBuilder (fluent API)
	batch2, err := kvstore.NewBatchBuilder().
		PutString("product:1:name", "Widget").
		PutString("product:1:price", "9.99").
		PutWithTTL("product:1:promo", []byte("SALE20"), time.Hour).
		Build()

	if err != nil {
		log.Printf("BatchBuilder failed: %v", err)
	} else {
		store.ApplyBatch(batch2)
		fmt.Printf("  Product batch applied: %d operations\n", batch2.Len())
	}

	// Batch statistics
	stats := batch.Stats()
	fmt.Printf("  Batch stats: %d puts, %d deletes, %d total size\n",
		stats.PutCount, stats.DeleteCount, stats.TotalSize)

	fmt.Println()
}

func demoIterators(store *kvstore.Store) {
	fmt.Println("--- Iterators ---")

	// Add some test data
	for i := 0; i < 5; i++ {
		store.Set(fmt.Sprintf("iter:user:%d", i), []byte(fmt.Sprintf("User %d", i)))
		store.Set(fmt.Sprintf("iter:product:%d", i), []byte(fmt.Sprintf("Product %d", i)))
	}

	// Iterate all with limit
	fmt.Printf("  First 5 entries:\n")
	it := store.Iterator(&kvstore.IteratorOptions{Limit: 5, Sorted: true})
	for it.Next() {
		fmt.Printf("    %s = %s\n", it.Key(), it.Value())
	}
	it.Close()

	// Prefix iterator
	fmt.Printf("  Entries with prefix 'iter:user:':\n")
	prefixIt := store.PrefixIterator("iter:user:")
	for prefixIt.Next() {
		fmt.Printf("    %s = %s\n", prefixIt.Key(), prefixIt.Value())
	}
	prefixIt.Close()

	// Range iterator
	fmt.Printf("  Entries in range 'iter:product:1' to 'iter:product:4':\n")
	rangeIt := store.RangeIterator("iter:product:1", "iter:product:4")
	for rangeIt.Next() {
		fmt.Printf("    %s = %s\n", rangeIt.Key(), rangeIt.Value())
	}
	rangeIt.Close()

	// Collect keys helper
	allIt := store.PrefixIterator("iter:")
	keys := kvstore.CollectKeys(allIt)
	allIt.Close()
	fmt.Printf("  Total 'iter:' keys: %d\n", len(keys))

	fmt.Println()
}

func demoConcurrentAccess(store *kvstore.Store) {
	fmt.Println("--- Concurrent Access ---")

	const numGoroutines = 10
	const opsPerGoroutine = 100

	start := time.Now()

	done := make(chan bool, numGoroutines*2)

	// Concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent:%d:%d", id, j)
				store.Set(key, []byte(fmt.Sprintf("value-%d-%d", id, j)))
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent:%d:%d", id, j)
				store.Get(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines*2; i++ {
		<-done
	}

	duration := time.Since(start)
	totalOps := numGoroutines * opsPerGoroutine * 2 // reads + writes
	opsPerSec := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Completed %d operations in %v\n", totalOps, duration)
	fmt.Printf("  Throughput: %.0f ops/sec\n", opsPerSec)
	fmt.Println()
}

func demoWatch(store *kvstore.Store) {
	fmt.Println("--- Watch ---")

	key := "watch:counter"
	store.Set(key, []byte("0"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := store.Watch(ctx, key)

	// Update the key in background
	go func() {
		for i := 1; i <= 3; i++ {
			time.Sleep(300 * time.Millisecond)
			store.Set(key, []byte(fmt.Sprintf("%d", i)))
		}
	}()

	fmt.Printf("  Watching key '%s' for changes...\n", key)
	for {
		select {
		case value, ok := <-ch:
			if !ok {
				fmt.Printf("  Watch channel closed\n")
				goto done
			}
			fmt.Printf("  Value changed to: %s\n", value)
		case <-ctx.Done():
			fmt.Printf("  Watch timeout\n")
			goto done
		}
	}
done:
	fmt.Println()
}

func demoSnapshots(store *kvstore.Store) {
	fmt.Println("--- Snapshots ---")

	// Ensure we have some data
	for i := 0; i < 50; i++ {
		store.Set(fmt.Sprintf("snapshot:key:%d", i), []byte(fmt.Sprintf("data-%d", i)))
	}

	// Create a snapshot
	meta, err := store.Snapshot()
	if err != nil {
		log.Printf("Snapshot failed: %v", err)
		fmt.Println()
		return
	}

	fmt.Printf("  Snapshot created:\n")
	fmt.Printf("    ID: %d\n", meta.ID)
	fmt.Printf("    Entries: %d\n", meta.EntryCount)
	fmt.Printf("    Size: %d bytes\n", meta.Size)
	fmt.Printf("    Compressed: %v\n", meta.IsCompressed())
	fmt.Printf("    Path: %s\n", meta.Path)

	// Get count before clear
	countBefore := store.Len()

	// Clear and restore
	store.Clear()
	fmt.Printf("  Store cleared. Entries: %d\n", store.Len())

	err = store.RestoreSnapshot(meta.Path)
	if err != nil {
		log.Printf("RestoreSnapshot failed: %v", err)
	} else {
		fmt.Printf("  Snapshot restored. Entries: %d (was %d)\n", store.Len(), countBefore)
	}

	fmt.Println()
}

func printStats(store *kvstore.Store) {
	fmt.Println("--- Statistics ---")

	stats := store.Stats()
	fmt.Printf("  Entry Count: %d\n", stats.EntryCount)
	fmt.Printf("  Total Size: %d bytes\n", stats.TotalSize)
	fmt.Printf("  Shard Count: %d\n", stats.ShardCount)
	fmt.Printf("  Gets: %d\n", stats.Gets)
	fmt.Printf("  Sets: %d\n", stats.Sets)
	fmt.Printf("  Deletes: %d\n", stats.Deletes)
	fmt.Printf("  Hits: %d\n", stats.Hits)
	fmt.Printf("  Misses: %d\n", stats.Misses)
	fmt.Printf("  Hit Rate: %.2f%%\n", stats.HitRate)
	fmt.Printf("  Evictions: %d\n", stats.Evictions)
	fmt.Printf("  Expired Cleanup: %d\n", stats.ExpiredCleanup)
	fmt.Printf("  Errors: %d\n", stats.Errors)
}
