package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Helper function to create a test store with default options
func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	store, err := New(opts...)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

// Helper function to create a test store with persistence
func newTestStoreWithPersistence(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := New(
		WithPersistence(PersistenceHybrid, dir),
		WithSyncInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to create store with persistence: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

func TestNew(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		store, err := New()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer store.Close()

		if store == nil {
			t.Fatal("store should not be nil")
		}
	})

	t.Run("with custom options", func(t *testing.T) {
		store, err := New(
			WithMaxKeySize(512),
			WithMaxValueSize(1024),
			WithMaxEntries(100),
			WithEvictionPolicy(EvictionLRU),
			WithShardCount(16),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer store.Close()

		if store == nil {
			t.Fatal("store should not be nil")
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		_, err := New(WithMaxKeySize(-1))
		if err == nil {
			t.Fatal("expected error for invalid options")
		}
	})
}

func TestStore_SetGet(t *testing.T) {
	store := newTestStore(t)

	t.Run("set and get", func(t *testing.T) {
		key := "test-key"
		value := []byte("test-value")

		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		got, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !bytes.Equal(got, value) {
			t.Errorf("Get() = %q, want %q", got, value)
		}
	})

	t.Run("set string and get string", func(t *testing.T) {
		key := "string-key"
		value := "string-value"

		err := store.SetString(key, value)
		if err != nil {
			t.Fatalf("SetString failed: %v", err)
		}

		got, err := store.GetString(key)
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}

		if got != value {
			t.Errorf("GetString() = %q, want %q", got, value)
		}
	})

	t.Run("get non-existent key", func(t *testing.T) {
		_, err := store.Get("non-existent")
		if !IsNotFound(err) {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		err := store.Set("", []byte("value"))
		if err == nil {
			t.Error("expected error for empty key")
		}
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		key := "overwrite-key"
		value1 := []byte("value1")
		value2 := []byte("value2")

		store.Set(key, value1)
		store.Set(key, value2)

		got, _ := store.Get(key)
		if !bytes.Equal(got, value2) {
			t.Errorf("Get() = %q, want %q", got, value2)
		}
	})
}

func TestStore_SetWithTTL(t *testing.T) {
	store := newTestStore(t, WithCleanupInterval(50*time.Millisecond))

	t.Run("key expires", func(t *testing.T) {
		key := "expiring-key"
		value := []byte("expiring-value")

		err := store.SetWithTTL(key, value, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("SetWithTTL failed: %v", err)
		}

		// Should exist immediately
		if !store.Exists(key) {
			t.Error("key should exist immediately after set")
		}

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		// Should be expired now
		_, err = store.Get(key)
		if !IsExpired(err) && !IsNotFound(err) {
			t.Errorf("expected key to be expired or not found, got %v", err)
		}
	})

	t.Run("TTL returns correct duration", func(t *testing.T) {
		key := "ttl-key"
		ttl := 5 * time.Second

		store.SetWithTTL(key, []byte("value"), ttl)

		got, err := store.TTL(key)
		if err != nil {
			t.Fatalf("TTL failed: %v", err)
		}

		// TTL should be close to the set value (within 100ms)
		if got > ttl || got < ttl-100*time.Millisecond {
			t.Errorf("TTL() = %v, want approximately %v", got, ttl)
		}
	})
}

func TestStore_Delete(t *testing.T) {
	store := newTestStore(t)

	t.Run("delete existing key", func(t *testing.T) {
		key := "delete-key"
		store.Set(key, []byte("value"))

		deleted, err := store.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if !deleted {
			t.Error("Delete should return true for existing key")
		}

		if store.Exists(key) {
			t.Error("key should not exist after delete")
		}
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		deleted, err := store.Delete("non-existent")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if deleted {
			t.Error("Delete should return false for non-existent key")
		}
	})
}

func TestStore_Exists(t *testing.T) {
	store := newTestStore(t)

	t.Run("existing key", func(t *testing.T) {
		key := "exists-key"
		store.Set(key, []byte("value"))

		if !store.Exists(key) {
			t.Error("Exists should return true for existing key")
		}
	})

	t.Run("non-existent key", func(t *testing.T) {
		if store.Exists("non-existent") {
			t.Error("Exists should return false for non-existent key")
		}
	})

	t.Run("expired key", func(t *testing.T) {
		key := "expired-exists-key"
		store.SetWithTTL(key, []byte("value"), 50*time.Millisecond)
		time.Sleep(100 * time.Millisecond)

		if store.Exists(key) {
			t.Error("Exists should return false for expired key")
		}
	})
}

func TestStore_SetNX(t *testing.T) {
	store := newTestStore(t)

	t.Run("set if not exists - new key", func(t *testing.T) {
		key := "setnx-new"
		ok, err := store.SetNX(key, []byte("value"))
		if err != nil {
			t.Fatalf("SetNX failed: %v", err)
		}

		if !ok {
			t.Error("SetNX should return true for new key")
		}
	})

	t.Run("set if not exists - existing key", func(t *testing.T) {
		key := "setnx-existing"
		store.Set(key, []byte("value1"))

		ok, err := store.SetNX(key, []byte("value2"))
		if err != nil {
			t.Fatalf("SetNX failed: %v", err)
		}

		if ok {
			t.Error("SetNX should return false for existing key")
		}

		// Value should remain unchanged
		got, _ := store.Get(key)
		if !bytes.Equal(got, []byte("value1")) {
			t.Error("SetNX should not overwrite existing value")
		}
	})
}

func TestStore_Expire(t *testing.T) {
	store := newTestStore(t)

	t.Run("set expiration on existing key", func(t *testing.T) {
		key := "expire-key"
		store.Set(key, []byte("value"))

		err := store.Expire(key, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("Expire failed: %v", err)
		}

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		_, err = store.Get(key)
		if !IsExpired(err) && !IsNotFound(err) {
			t.Errorf("key should be expired, got %v", err)
		}
	})

	t.Run("expire non-existent key", func(t *testing.T) {
		err := store.Expire("non-existent", time.Second)
		if !IsNotFound(err) {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})
}

func TestStore_Persist(t *testing.T) {
	store := newTestStore(t)

	key := "persist-key"
	store.SetWithTTL(key, []byte("value"), time.Second)

	err := store.Persist(key)
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	ttl, _ := store.TTL(key)
	if ttl != 0 {
		t.Errorf("TTL should be 0 after Persist, got %v", ttl)
	}
}

func TestStore_LenAndSize(t *testing.T) {
	store := newTestStore(t)

	// Empty store
	if store.Len() != 0 {
		t.Errorf("Len() = %d, want 0", store.Len())
	}

	// Add some entries
	store.Set("key1", []byte("value1"))
	store.Set("key2", []byte("value2"))
	store.Set("key3", []byte("value3"))

	if store.Len() != 3 {
		t.Errorf("Len() = %d, want 3", store.Len())
	}

	// Size should be positive
	if store.Size() <= 0 {
		t.Errorf("Size() = %d, want > 0", store.Size())
	}

	// Delete one
	store.Delete("key2")
	if store.Len() != 2 {
		t.Errorf("Len() = %d, want 2 after delete", store.Len())
	}
}

func TestStore_Keys(t *testing.T) {
	store := newTestStore(t)

	expectedKeys := []string{"key1", "key2", "key3"}
	for _, k := range expectedKeys {
		store.Set(k, []byte("value"))
	}

	keys := store.Keys()
	if len(keys) != len(expectedKeys) {
		t.Errorf("Keys() returned %d keys, want %d", len(keys), len(expectedKeys))
	}

	// Check all expected keys are present
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, k := range expectedKeys {
		if !keySet[k] {
			t.Errorf("Keys() missing key %q", k)
		}
	}
}

func TestStore_Clear(t *testing.T) {
	store := newTestStore(t)

	store.Set("key1", []byte("value1"))
	store.Set("key2", []byte("value2"))

	err := store.Clear()
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	if store.Len() != 0 {
		t.Errorf("Len() = %d after Clear, want 0", store.Len())
	}
}

func TestStore_GetEntry(t *testing.T) {
	store := newTestStore(t)

	key := "entry-key"
	value := []byte("entry-value")
	store.SetWithTTL(key, value, time.Hour)

	entry, err := store.GetEntry(key)
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}

	if entry.Key != key {
		t.Errorf("entry.Key = %q, want %q", entry.Key, key)
	}

	if !bytes.Equal(entry.Value, value) {
		t.Errorf("entry.Value = %q, want %q", entry.Value, value)
	}

	if !entry.HasExpiration() {
		t.Error("entry should have expiration")
	}

	if entry.Metadata == nil {
		t.Error("entry.Metadata should not be nil")
	}
}

func TestStore_Batch(t *testing.T) {
	store := newTestStore(t)

	t.Run("batch operations", func(t *testing.T) {
		batch := NewBatch()
		batch.Put("batch-key1", []byte("value1"))
		batch.Put("batch-key2", []byte("value2"))
		batch.PutWithTTL("batch-key3", []byte("value3"), time.Hour)
		batch.Delete("non-existent-key")

		result, err := store.ApplyBatch(batch)
		if err != nil {
			t.Fatalf("ApplyBatch failed: %v", err)
		}

		if result.Successful != 4 {
			t.Errorf("Successful = %d, want 4", result.Successful)
		}

		if result.Failed != 0 {
			t.Errorf("Failed = %d, want 0", result.Failed)
		}

		// Verify the batch was applied
		if !store.Exists("batch-key1") {
			t.Error("batch-key1 should exist")
		}
		if !store.Exists("batch-key2") {
			t.Error("batch-key2 should exist")
		}
		if !store.Exists("batch-key3") {
			t.Error("batch-key3 should exist")
		}
	})

	t.Run("batch builder", func(t *testing.T) {
		batch, err := NewBatchBuilder().
			PutString("builder-key1", "value1").
			PutString("builder-key2", "value2").
			Delete("builder-key1").
			Build()

		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if batch.Len() != 3 {
			t.Errorf("batch.Len() = %d, want 3", batch.Len())
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		batch := NewBatch()
		result, err := store.ApplyBatch(batch)
		if err != nil {
			t.Fatalf("ApplyBatch failed for empty batch: %v", err)
		}

		if result.Successful != 0 {
			t.Errorf("Successful = %d, want 0", result.Successful)
		}
	})
}

func TestStore_Iterator(t *testing.T) {
	store := newTestStore(t)

	// Populate store
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("iter-key-%02d", i)
		store.Set(key, []byte(fmt.Sprintf("value-%d", i)))
	}

	t.Run("iterate all", func(t *testing.T) {
		it := store.Iterator(nil)
		defer it.Close()

		count := 0
		for it.Next() {
			count++
			if it.Key() == "" {
				t.Error("Key should not be empty")
			}
			if it.Value() == nil {
				t.Error("Value should not be nil")
			}
		}

		if err := it.Error(); err != nil {
			t.Fatalf("Iterator error: %v", err)
		}

		if count != 10 {
			t.Errorf("Iterator returned %d entries, want 10", count)
		}
	})

	t.Run("prefix iterator", func(t *testing.T) {
		// Add some entries with a different prefix
		store.Set("other-key-1", []byte("value"))
		store.Set("other-key-2", []byte("value"))

		it := store.PrefixIterator("iter-key-")
		defer it.Close()

		count := 0
		for it.Next() {
			count++
		}

		if count != 10 {
			t.Errorf("PrefixIterator returned %d entries, want 10", count)
		}
	})

	t.Run("range iterator", func(t *testing.T) {
		it := store.RangeIterator("iter-key-03", "iter-key-07")
		defer it.Close()

		count := 0
		for it.Next() {
			count++
		}

		// Should include 03, 04, 05, 06 (07 is exclusive)
		if count != 4 {
			t.Errorf("RangeIterator returned %d entries, want 4", count)
		}
	})

	t.Run("iterator with limit", func(t *testing.T) {
		opts := &IteratorOptions{
			Limit:  5,
			Sorted: true,
		}
		it := store.Iterator(opts)
		defer it.Close()

		count := 0
		for it.Next() {
			count++
		}

		if count != 5 {
			t.Errorf("Iterator with limit returned %d entries, want 5", count)
		}
	})
}

func TestStore_Concurrency(t *testing.T) {
	store := newTestStore(t, WithShardCount(32))

	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := store.Set(key, value); err != nil {
					t.Errorf("concurrent Set failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify count
	expectedCount := numGoroutines * opsPerGoroutine
	if store.Len() != expectedCount {
		t.Errorf("Len() = %d, want %d", store.Len(), expectedCount)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
				_, err := store.Get(key)
				if err != nil {
					t.Errorf("concurrent Get failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
				store.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	// All should be deleted
	if store.Len() != 0 {
		t.Errorf("Len() = %d after deletes, want 0", store.Len())
	}
}

func TestStore_Eviction(t *testing.T) {
	evictedKeys := make([]string, 0)
	var mu sync.Mutex

	store := newTestStore(t,
		WithMaxEntries(10),
		WithEvictionPolicy(EvictionLRU),
		WithOnEvict(func(key string, value []byte) {
			mu.Lock()
			evictedKeys = append(evictedKeys, key)
			mu.Unlock()
		}),
	)

	// Fill the store
	for i := 0; i < 10; i++ {
		store.Set(fmt.Sprintf("key-%02d", i), []byte("value"))
	}

	// Add one more - should trigger eviction
	err := store.Set("new-key", []byte("new-value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Store should still have max entries
	if store.Len() > 10 {
		t.Errorf("Len() = %d, want <= 10", store.Len())
	}

	// Check that eviction callback was called
	mu.Lock()
	if len(evictedKeys) == 0 {
		t.Error("eviction callback should have been called")
	}
	mu.Unlock()
}

func TestStore_Stats(t *testing.T) {
	store := newTestStore(t)

	// Perform some operations
	store.Set("key1", []byte("value1"))
	store.Set("key2", []byte("value2"))
	store.Get("key1")
	store.Get("key2")
	store.Get("non-existent") // Miss
	store.Delete("key1")

	stats := store.Stats()

	if stats.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", stats.EntryCount)
	}

	if stats.Sets != 2 {
		t.Errorf("Sets = %d, want 2", stats.Sets)
	}

	if stats.Gets != 3 {
		t.Errorf("Gets = %d, want 3", stats.Gets)
	}

	if stats.Hits != 2 {
		t.Errorf("Hits = %d, want 2", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Misses = %d, want 1", stats.Misses)
	}

	if stats.Deletes != 1 {
		t.Errorf("Deletes = %d, want 1", stats.Deletes)
	}
}

func TestStore_Close(t *testing.T) {
	store, _ := New()

	// Close should succeed
	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations on closed store should fail
	err = store.Set("key", []byte("value"))
	if err != ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}

	_, err = store.Get("key")
	if err != ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}

	// Double close should be safe
	err = store.Close()
	if err != nil {
		t.Errorf("double Close failed: %v", err)
	}
}

func TestStore_Watch(t *testing.T) {
	store := newTestStore(t)

	key := "watch-key"
	store.Set(key, []byte("initial"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ch := store.Watch(ctx, key)

	// Update the key
	go func() {
		time.Sleep(50 * time.Millisecond)
		store.Set(key, []byte("updated"))
	}()

	// Should receive the updated value
	select {
	case value := <-ch:
		if string(value) != "updated" {
			t.Errorf("Watch received %q, want %q", value, "updated")
		}
	case <-ctx.Done():
		t.Error("Watch timed out waiting for update")
	}
}

func TestStore_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create store with persistence
	store1, err := New(
		WithPersistence(PersistenceWAL, dir),
		WithSyncInterval(0), // Sync on every write
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Write some data
	store1.Set("persist-key1", []byte("value1"))
	store1.Set("persist-key2", []byte("value2"))
	store1.SetWithTTL("persist-key3", []byte("value3"), time.Hour)
	store1.Delete("persist-key2")

	// Close the store
	store1.Close()

	// Reopen and verify data was persisted
	store2, err := New(
		WithPersistence(PersistenceWAL, dir),
	)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	// Verify data
	val1, err := store2.Get("persist-key1")
	if err != nil {
		t.Fatalf("Get persist-key1 failed: %v", err)
	}
	if string(val1) != "value1" {
		t.Errorf("persist-key1 = %q, want %q", val1, "value1")
	}

	// Deleted key should not exist
	if store2.Exists("persist-key2") {
		t.Error("persist-key2 should not exist")
	}

	// TTL key should exist
	if !store2.Exists("persist-key3") {
		t.Error("persist-key3 should exist")
	}
}

func TestStore_Snapshot(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		WithPersistence(PersistenceSnapshot, dir),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Write some data
	for i := 0; i < 100; i++ {
		store.Set(fmt.Sprintf("snap-key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Create snapshot
	meta, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if meta.EntryCount != 100 {
		t.Errorf("Snapshot EntryCount = %d, want 100", meta.EntryCount)
	}

	if meta.Size <= 0 {
		t.Error("Snapshot size should be positive")
	}

	// Verify snapshot file exists
	if _, err := os.Stat(meta.Path); os.IsNotExist(err) {
		t.Error("Snapshot file does not exist")
	}

	// Clear store and restore
	store.Clear()
	if store.Len() != 0 {
		t.Error("store should be empty after clear")
	}

	err = store.RestoreSnapshot(meta.Path)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}

	if store.Len() != 100 {
		t.Errorf("Len() = %d after restore, want 100", store.Len())
	}

	store.Close()
}

// Benchmarks

func BenchmarkStore_Set(b *testing.B) {
	store, _ := New()
	defer store.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.Set(key, value)
	}
}

func BenchmarkStore_Get(b *testing.B) {
	store, _ := New()
	defer store.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		store.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		store.Get(key)
	}
}

func BenchmarkStore_SetGet(b *testing.B) {
	store, _ := New()
	defer store.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.Set(key, value)
		store.Get(key)
	}
}

func BenchmarkStore_ConcurrentSet(b *testing.B) {
	store, _ := New(WithShardCount(64))
	defer store.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			store.Set(key, value)
			i++
		}
	})
}

func BenchmarkStore_ConcurrentGet(b *testing.B) {
	store, _ := New(WithShardCount(64))
	defer store.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		store.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%10000)
			store.Get(key)
			i++
		}
	})
}

func BenchmarkStore_ConcurrentMixed(b *testing.B) {
	store, _ := New(WithShardCount(64))
	defer store.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		store.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	value := []byte("benchmark-value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%10000)
			if i%2 == 0 {
				store.Set(key, value)
			} else {
				store.Get(key)
			}
			i++
		}
	})
}

func BenchmarkStore_BatchWrite(b *testing.B) {
	store, _ := New()
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch()
		for j := 0; j < 100; j++ {
			batch.Put(fmt.Sprintf("batch-key-%d-%d", i, j), []byte("value"))
		}
		store.ApplyBatch(batch)
	}
}

func BenchmarkStore_Iterator(b *testing.B) {
	store, _ := New()
	defer store.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		store.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := store.Iterator(nil)
		for it.Next() {
			_ = it.Key()
			_ = it.Value()
		}
		it.Close()
	}
}

func BenchmarkStore_SetWithTTL(b *testing.B) {
	store, _ := New()
	defer store.Close()

	value := []byte("benchmark-value")
	ttl := time.Hour

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.SetWithTTL(key, value, ttl)
	}
}

func BenchmarkStore_WithPersistence(b *testing.B) {
	dir := b.TempDir()
	store, _ := New(
		WithPersistence(PersistenceWAL, dir),
		WithSyncInterval(time.Second), // Don't sync on every write
	)
	defer store.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.Set(key, value)
	}
}

func BenchmarkEntry_Encode(b *testing.B) {
	entry := NewEntryWithTTL("benchmark-key", []byte("benchmark-value-that-is-a-bit-longer"), time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Encode()
	}
}

func BenchmarkEntry_Decode(b *testing.B) {
	entry := NewEntryWithTTL("benchmark-key", []byte("benchmark-value-that-is-a-bit-longer"), time.Hour)
	encoded := entry.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeEntry(encoded)
	}
}
