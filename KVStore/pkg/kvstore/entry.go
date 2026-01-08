package kvstore

import (
	"encoding/binary"
	"hash/crc32"
	"sync/atomic"
	"time"
)

// Entry represents a key-value pair with metadata.
type Entry struct {
	Key       string
	Value     []byte
	ExpiresAt time.Time // Zero value means no expiration
	Metadata  *Metadata
}

// Metadata holds additional information about an entry.
type Metadata struct {
	CreatedAt   time.Time     // When the entry was created
	UpdatedAt   time.Time     // When the entry was last updated
	AccessedAt  time.Time     // When the entry was last accessed
	AccessCount atomic.Uint64 // Number of times accessed (for LFU)
	Version     uint64        // Version number for optimistic concurrency
	Size        int           // Size of key + value in bytes
	Checksum    uint32        // CRC32 checksum for integrity verification
}

// NewEntry creates a new entry with the given key and value.
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

// NewEntryWithTTL creates a new entry with a time-to-live duration.
func NewEntryWithTTL(key string, value []byte, ttl time.Duration) *Entry {
	e := NewEntry(key, value)
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl)
	}
	return e
}

// NewEntryWithExpiration creates a new entry with a specific expiration time.
func NewEntryWithExpiration(key string, value []byte, expiresAt time.Time) *Entry {
	e := NewEntry(key, value)
	e.ExpiresAt = expiresAt
	return e
}

// IsExpired checks if the entry has expired.
func (e *Entry) IsExpired() bool {
	if e.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// TTL returns the remaining time-to-live for the entry.
// Returns 0 if the entry has no expiration or has already expired.
func (e *Entry) TTL() time.Duration {
	if e.ExpiresAt.IsZero() {
		return 0
	}
	ttl := time.Until(e.ExpiresAt)
	if ttl < 0 {
		return 0
	}
	return ttl
}

// Touch updates the access time and increments the access counter.
func (e *Entry) Touch() {
	if e.Metadata != nil {
		e.Metadata.AccessedAt = time.Now()
		e.Metadata.AccessCount.Add(1)
	}
}

// Update updates the entry's value and metadata.
func (e *Entry) Update(value []byte) {
	e.Value = value
	if e.Metadata != nil {
		e.Metadata.UpdatedAt = time.Now()
		e.Metadata.Version++
		e.Metadata.Size = len(e.Key) + len(value)
		e.Metadata.Checksum = e.computeChecksum()
	}
}

// UpdateWithTTL updates the entry's value and sets a new TTL.
func (e *Entry) UpdateWithTTL(value []byte, ttl time.Duration) {
	e.Update(value)
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl)
	} else {
		e.ExpiresAt = time.Time{} // Clear expiration
	}
}

// SetTTL sets a new TTL for the entry.
func (e *Entry) SetTTL(ttl time.Duration) {
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl)
	} else {
		e.ExpiresAt = time.Time{}
	}
}

// SetExpiration sets a specific expiration time.
func (e *Entry) SetExpiration(expiresAt time.Time) {
	e.ExpiresAt = expiresAt
}

// ClearExpiration removes the expiration from the entry.
func (e *Entry) ClearExpiration() {
	e.ExpiresAt = time.Time{}
}

// HasExpiration returns true if the entry has an expiration time set.
func (e *Entry) HasExpiration() bool {
	return !e.ExpiresAt.IsZero()
}

// Clone creates a deep copy of the entry.
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
			UpdatedAt:  e.Metadata.UpdatedAt,
			AccessedAt: e.Metadata.AccessedAt,
			Version:    e.Metadata.Version,
			Size:       e.Metadata.Size,
			Checksum:   e.Metadata.Checksum,
		}
		clone.Metadata.AccessCount.Store(e.Metadata.AccessCount.Load())
	}

	return clone
}

// Size returns the total size of the entry in bytes (key + value).
func (e *Entry) Size() int {
	return len(e.Key) + len(e.Value)
}

// computeChecksum computes the CRC32 checksum of the entry.
func (e *Entry) computeChecksum() uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(e.Key))
	h.Write(e.Value)
	return h.Sum32()
}

// VerifyChecksum verifies the entry's data integrity.
func (e *Entry) VerifyChecksum() bool {
	if e.Metadata == nil {
		return true // No checksum to verify
	}
	return e.computeChecksum() == e.Metadata.Checksum
}

// Encode serializes the entry to a byte slice for persistence.
// Format: [keyLen(4)][key][valueLen(4)][value][expiresAt(8)][version(8)][checksum(4)]
func (e *Entry) Encode() []byte {
	keyBytes := []byte(e.Key)
	keyLen := len(keyBytes)
	valueLen := len(e.Value)

	// Calculate total size
	totalSize := 4 + keyLen + 4 + valueLen + 8 + 8 + 4
	buf := make([]byte, totalSize)

	offset := 0

	// Key length and key
	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += keyLen

	// Value length and value
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4
	copy(buf[offset:], e.Value)
	offset += valueLen

	// Expiration timestamp (Unix nano)
	var expiresNano int64
	if !e.ExpiresAt.IsZero() {
		expiresNano = e.ExpiresAt.UnixNano()
	}
	binary.BigEndian.PutUint64(buf[offset:], uint64(expiresNano))
	offset += 8

	// Version
	var version uint64
	if e.Metadata != nil {
		version = e.Metadata.Version
	}
	binary.BigEndian.PutUint64(buf[offset:], version)
	offset += 8

	// Checksum of the encoded data (excluding the checksum field itself)
	checksum := crc32.ChecksumIEEE(buf[:offset])
	binary.BigEndian.PutUint32(buf[offset:], checksum)

	return buf
}

// DecodeEntry deserializes an entry from a byte slice.
func DecodeEntry(data []byte) (*Entry, error) {
	if len(data) < 28 { // Minimum: 4+0+4+0+8+8+4 = 28 bytes
		return nil, ErrCorruptedData
	}

	offset := 0

	// Key length and key
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+keyLen > len(data)-24 { // Need at least 24 more bytes after key
		return nil, ErrCorruptedData
	}
	key := string(data[offset : offset+keyLen])
	offset += keyLen

	// Value length and value
	valueLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+valueLen > len(data)-20 { // Need at least 20 more bytes after value
		return nil, ErrCorruptedData
	}
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+valueLen])
	offset += valueLen

	// Expiration timestamp
	expiresNano := int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Version
	version := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Verify checksum
	storedChecksum := binary.BigEndian.Uint32(data[offset:])
	computedChecksum := crc32.ChecksumIEEE(data[:offset])
	if storedChecksum != computedChecksum {
		return nil, ErrCorruptedData
	}

	// Build entry
	now := time.Now()
	entry := &Entry{
		Key:   key,
		Value: value,
		Metadata: &Metadata{
			CreatedAt:  now,
			UpdatedAt:  now,
			AccessedAt: now,
			Version:    version,
			Size:       len(key) + valueLen,
		},
	}

	if expiresNano > 0 {
		entry.ExpiresAt = time.Unix(0, expiresNano)
	}

	entry.Metadata.Checksum = entry.computeChecksum()

	return entry, nil
}

// EntryStats provides statistics about an entry.
type EntryStats struct {
	Key         string
	Size        int
	TTL         time.Duration
	HasExpiry   bool
	Version     uint64
	AccessCount uint64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	AccessedAt  time.Time
}

// Stats returns statistics about the entry.
func (e *Entry) Stats() EntryStats {
	stats := EntryStats{
		Key:       e.Key,
		Size:      e.Size(),
		TTL:       e.TTL(),
		HasExpiry: e.HasExpiration(),
	}

	if e.Metadata != nil {
		stats.Version = e.Metadata.Version
		stats.AccessCount = e.Metadata.AccessCount.Load()
		stats.CreatedAt = e.Metadata.CreatedAt
		stats.UpdatedAt = e.Metadata.UpdatedAt
		stats.AccessedAt = e.Metadata.AccessedAt
	}

	return stats
}

// Compare compares two entries for equality.
func (e *Entry) Compare(other *Entry) bool {
	if e == nil || other == nil {
		return e == other
	}

	if e.Key != other.Key {
		return false
	}

	if len(e.Value) != len(other.Value) {
		return false
	}

	for i := range e.Value {
		if e.Value[i] != other.Value[i] {
			return false
		}
	}

	return true
}
