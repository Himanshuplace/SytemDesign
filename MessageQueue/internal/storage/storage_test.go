// Package storage provides tests for the storage backends.
package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestMemoryStorage_Store tests storing messages in memory storage.
func TestMemoryStorage_Store(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	msg := &MessageData{
		ID:        "test-id-1",
		Payload:   []byte("test payload"),
		Headers:   map[string]string{"key": "value"},
		Priority:  5,
		Timestamp: time.Now(),
	}

	err := s.Store(msg)
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	// Verify storage
	count, _ := s.Count()
	if count != 1 {
		t.Errorf("Count() = %d, want 1", count)
	}
}

func TestMemoryStorage_Store_NilMessage(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	err := s.Store(nil)
	if err == nil {
		t.Error("Store(nil) should return error")
	}
}

func TestMemoryStorage_Store_Duplicate(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	msg := &MessageData{
		ID:      "test-id-1",
		Payload: []byte("test payload"),
	}

	s.Store(msg)
	err := s.Store(msg)
	if err != ErrAlreadyExists {
		t.Errorf("Store() error = %v, want %v", err, ErrAlreadyExists)
	}
}

func TestMemoryStorage_Store_Full(t *testing.T) {
	cfg := MemoryConfig{MaxSize: 2}
	s := NewMemoryStorage(cfg)
	defer s.Close()

	s.Store(&MessageData{ID: "1", Payload: []byte("1")})
	s.Store(&MessageData{ID: "2", Payload: []byte("2")})

	err := s.Store(&MessageData{ID: "3", Payload: []byte("3")})
	if err != ErrStorageFull {
		t.Errorf("Store() error = %v, want %v", err, ErrStorageFull)
	}
}

func TestMemoryStorage_Get(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	original := &MessageData{
		ID:       "test-id-1",
		Payload:  []byte("test payload"),
		Headers:  map[string]string{"key": "value"},
		Priority: 5,
	}

	s.Store(original)

	retrieved, err := s.Get("test-id-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if retrieved.ID != original.ID {
		t.Errorf("ID = %q, want %q", retrieved.ID, original.ID)
	}

	if string(retrieved.Payload) != string(original.Payload) {
		t.Errorf("Payload = %q, want %q", string(retrieved.Payload), string(original.Payload))
	}

	if retrieved.Headers["key"] != "value" {
		t.Errorf("Headers[key] = %q, want %q", retrieved.Headers["key"], "value")
	}
}

func TestMemoryStorage_Get_NotFound(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	_, err := s.Get("non-existent")
	if err != ErrNotFound {
		t.Errorf("Get() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStorage_Get_ReturnsDeepCopy(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	original := &MessageData{
		ID:      "test-id-1",
		Payload: []byte("original"),
		Headers: map[string]string{"key": "value"},
	}

	s.Store(original)

	retrieved, _ := s.Get("test-id-1")
	retrieved.Payload[0] = 'X'
	retrieved.Headers["key"] = "modified"

	// Verify original in storage is unchanged
	original2, _ := s.Get("test-id-1")
	if original2.Payload[0] == 'X' {
		t.Error("Modifying retrieved message should not affect storage")
	}
	if original2.Headers["key"] == "modified" {
		t.Error("Modifying retrieved headers should not affect storage")
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	msg := &MessageData{ID: "test-id-1", Payload: []byte("test")}
	s.Store(msg)

	err := s.Delete("test-id-1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, err = s.Get("test-id-1")
	if err != ErrNotFound {
		t.Errorf("Get() after Delete() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStorage_Delete_NotFound(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	err := s.Delete("non-existent")
	if err != ErrNotFound {
		t.Errorf("Delete() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStorage_List(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{ID: "id-1", Payload: []byte("1")})
	s.Store(&MessageData{ID: "id-2", Payload: []byte("2")})
	s.Store(&MessageData{ID: "id-3", Payload: []byte("3")})

	ids, err := s.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("List() returned %d IDs, want 3", len(ids))
	}

	idMap := make(map[string]bool)
	for _, id := range ids {
		idMap[id] = true
	}

	for _, expected := range []string{"id-1", "id-2", "id-3"} {
		if !idMap[expected] {
			t.Errorf("List() missing ID %q", expected)
		}
	}
}

func TestMemoryStorage_Count(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	count, _ := s.Count()
	if count != 0 {
		t.Errorf("Count() = %d, want 0", count)
	}

	s.Store(&MessageData{ID: "1", Payload: []byte("1")})
	s.Store(&MessageData{ID: "2", Payload: []byte("2")})

	count, _ = s.Count()
	if count != 2 {
		t.Errorf("Count() = %d, want 2", count)
	}
}

func TestMemoryStorage_Clear(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{ID: "1", Payload: []byte("1")})
	s.Store(&MessageData{ID: "2", Payload: []byte("2")})

	err := s.Clear()
	if err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	count, _ := s.Count()
	if count != 0 {
		t.Errorf("Count() after Clear() = %d, want 0", count)
	}
}

func TestMemoryStorage_Close(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	s.Store(&MessageData{ID: "1", Payload: []byte("1")})

	err := s.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// All operations should fail after close
	_, err = s.Get("1")
	if err != ErrStorageClosed {
		t.Errorf("Get() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	err = s.Store(&MessageData{ID: "2", Payload: []byte("2")})
	if err != ErrStorageClosed {
		t.Errorf("Store() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	err = s.Delete("1")
	if err != ErrStorageClosed {
		t.Errorf("Delete() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	_, err = s.List()
	if err != ErrStorageClosed {
		t.Errorf("List() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	_, err = s.Count()
	if err != ErrStorageClosed {
		t.Errorf("Count() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	err = s.Clear()
	if err != ErrStorageClosed {
		t.Errorf("Clear() after Close() error = %v, want %v", err, ErrStorageClosed)
	}

	// Double close should return error
	err = s.Close()
	if err != ErrStorageClosed {
		t.Errorf("Second Close() error = %v, want %v", err, ErrStorageClosed)
	}
}

func TestMemoryStorage_Update(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	original := &MessageData{
		ID:       "test-id-1",
		Payload:  []byte("original"),
		Priority: 1,
	}
	s.Store(original)

	updated := &MessageData{
		ID:       "test-id-1",
		Payload:  []byte("updated"),
		Priority: 10,
	}

	err := s.Update(updated)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	retrieved, _ := s.Get("test-id-1")
	if string(retrieved.Payload) != "updated" {
		t.Errorf("Payload = %q, want %q", string(retrieved.Payload), "updated")
	}
	if retrieved.Priority != 10 {
		t.Errorf("Priority = %d, want 10", retrieved.Priority)
	}
}

func TestMemoryStorage_Update_NotFound(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	err := s.Update(&MessageData{ID: "non-existent", Payload: []byte("test")})
	if err != ErrNotFound {
		t.Errorf("Update() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStorage_Exists(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{ID: "existing", Payload: []byte("test")})

	exists, _ := s.Exists("existing")
	if !exists {
		t.Error("Exists() = false, want true")
	}

	exists, _ = s.Exists("non-existent")
	if exists {
		t.Error("Exists() = true, want false")
	}
}

func TestMemoryStorage_GetByDeduplicationID(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{
		ID:              "msg-1",
		Payload:         []byte("test"),
		DeduplicationID: "dedup-123",
	})
	s.Store(&MessageData{
		ID:              "msg-2",
		Payload:         []byte("test2"),
		DeduplicationID: "dedup-456",
	})

	msg, err := s.GetByDeduplicationID("dedup-123")
	if err != nil {
		t.Fatalf("GetByDeduplicationID() error = %v", err)
	}
	if msg.ID != "msg-1" {
		t.Errorf("ID = %q, want %q", msg.ID, "msg-1")
	}

	_, err = s.GetByDeduplicationID("non-existent")
	if err != ErrNotFound {
		t.Errorf("GetByDeduplicationID() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStorage_GetExpired(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	now := time.Now()

	// Message that is expired
	s.Store(&MessageData{
		ID:        "expired",
		Payload:   []byte("test"),
		Timestamp: now.Add(-2 * time.Hour),
		TTL:       time.Hour,
	})

	// Message that is not expired
	s.Store(&MessageData{
		ID:        "not-expired",
		Payload:   []byte("test"),
		Timestamp: now,
		TTL:       time.Hour,
	})

	// Message without TTL (never expires)
	s.Store(&MessageData{
		ID:        "no-ttl",
		Payload:   []byte("test"),
		Timestamp: now.Add(-10 * time.Hour),
		TTL:       0,
	})

	expired, err := s.GetExpired(now)
	if err != nil {
		t.Fatalf("GetExpired() error = %v", err)
	}

	if len(expired) != 1 {
		t.Errorf("GetExpired() returned %d messages, want 1", len(expired))
	}

	if expired[0].ID != "expired" {
		t.Errorf("Expired message ID = %q, want %q", expired[0].ID, "expired")
	}
}

func TestMemoryStorage_GetReady(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	now := time.Now()

	// Ready message (delivery time in past)
	s.Store(&MessageData{
		ID:           "ready",
		Payload:      []byte("test"),
		DeliveryTime: now.Add(-time.Hour),
	})

	// Not ready (delivery time in future)
	s.Store(&MessageData{
		ID:           "not-ready",
		Payload:      []byte("test"),
		DeliveryTime: now.Add(time.Hour),
	})

	// No delivery time (ready immediately)
	s.Store(&MessageData{
		ID:      "no-delivery-time",
		Payload: []byte("test"),
	})

	ready, err := s.GetReady(now)
	if err != nil {
		t.Fatalf("GetReady() error = %v", err)
	}

	if len(ready) != 2 {
		t.Errorf("GetReady() returned %d messages, want 2", len(ready))
	}

	ids := make(map[string]bool)
	for _, msg := range ready {
		ids[msg.ID] = true
	}

	if !ids["ready"] {
		t.Error("Expected 'ready' message in results")
	}
	if !ids["no-delivery-time"] {
		t.Error("Expected 'no-delivery-time' message in results")
	}
}

func TestMemoryStorage_Concurrency(t *testing.T) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent stores
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Store(&MessageData{
				ID:      "msg-" + string(rune('A'+i%26)) + string(rune('0'+i%10)),
				Payload: []byte("test"),
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.List()
			s.Count()
		}()
	}

	wg.Wait()

	// Verify no data corruption
	count, _ := s.Count()
	if count == 0 {
		t.Error("Expected some messages after concurrent operations")
	}
}

// Disk Storage Tests

func TestDiskStorage_BasicOperations(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "disk_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := DiskConfig{
		DataDir:     filepath.Join(tempDir, "messages"),
		SyncOnWrite: true,
		MaxSize:     100,
	}

	s, err := NewDiskStorage(cfg)
	if err != nil {
		t.Fatalf("NewDiskStorage() error = %v", err)
	}
	defer s.Close()

	// Store
	msg := &MessageData{
		ID:       "test-id-1",
		Payload:  []byte("test payload"),
		Headers:  map[string]string{"key": "value"},
		Priority: 5,
	}

	err = s.Store(msg)
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	// Get
	retrieved, err := s.Get("test-id-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if retrieved.ID != msg.ID {
		t.Errorf("ID = %q, want %q", retrieved.ID, msg.ID)
	}

	// Count
	count, _ := s.Count()
	if count != 1 {
		t.Errorf("Count() = %d, want 1", count)
	}

	// Delete
	err = s.Delete("test-id-1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	count, _ = s.Count()
	if count != 0 {
		t.Errorf("Count() after Delete() = %d, want 0", count)
	}
}

func TestDiskStorage_Persistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_storage_persist_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "messages")

	// Create storage and store message
	cfg := DiskConfig{DataDir: dataDir, SyncOnWrite: true}
	s1, _ := NewDiskStorage(cfg)

	msg := &MessageData{
		ID:      "persistent-msg",
		Payload: []byte("persistent payload"),
	}
	s1.Store(msg)
	s1.Close()

	// Reopen and verify data persisted
	s2, err := NewDiskStorage(cfg)
	if err != nil {
		t.Fatalf("NewDiskStorage() error = %v", err)
	}
	defer s2.Close()

	retrieved, err := s2.Get("persistent-msg")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if string(retrieved.Payload) != "persistent payload" {
		t.Errorf("Payload = %q, want %q", string(retrieved.Payload), "persistent payload")
	}
}

func TestDiskStorage_Stats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_storage_stats_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := DiskConfig{DataDir: filepath.Join(tempDir, "messages")}
	s, _ := NewDiskStorage(cfg)
	defer s.Close()

	s.Store(&MessageData{ID: "1", Payload: []byte("test1")})
	s.Store(&MessageData{ID: "2", Payload: []byte("test2")})

	stats := s.Stats()

	if stats.MessageCount != 2 {
		t.Errorf("MessageCount = %d, want 2", stats.MessageCount)
	}

	if stats.TotalSize == 0 {
		t.Error("TotalSize should be > 0")
	}

	if stats.IsClosed {
		t.Error("IsClosed should be false")
	}
}

// Benchmarks

func BenchmarkMemoryStorage_Store(b *testing.B) {
	s := NewMemoryStorage(MemoryConfig{MaxSize: b.N + 1})
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Store(&MessageData{
			ID:      "msg-" + string(rune(i)),
			Payload: []byte("benchmark payload"),
		})
	}
}

func BenchmarkMemoryStorage_Get(b *testing.B) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{ID: "test", Payload: []byte("benchmark payload")})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("test")
	}
}

func BenchmarkMemoryStorage_ConcurrentAccess(b *testing.B) {
	s := NewMemoryStorage(DefaultMemoryConfig())
	defer s.Close()

	s.Store(&MessageData{ID: "test", Payload: []byte("benchmark payload")})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("test")
		}
	})
}
