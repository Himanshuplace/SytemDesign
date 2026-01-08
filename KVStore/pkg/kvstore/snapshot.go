package kvstore

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Snapshot-related errors.
var (
	ErrSnapshotNotFound   = errors.New("snapshot not found")
	ErrSnapshotCorrupted  = errors.New("snapshot corrupted")
	ErrSnapshotInvalid    = errors.New("invalid snapshot format")
	ErrSnapshotReadFailed = errors.New("failed to read snapshot")
)

// Snapshot file constants.
const (
	SnapshotFilePrefix  = "snapshot-"
	SnapshotFileSuffix  = ".snap"
	SnapshotMagicNumber = 0x4B56534E // "KVSN" in hex
	SnapshotVersion     = 1
	SnapshotHeaderSize  = 32 // Magic(4) + Version(4) + Timestamp(8) + EntryCount(8) + Checksum(4) + Flags(4)
	SnapshotBufferSize  = 256 * 1024
)

// SnapshotFlags defines snapshot options.
type SnapshotFlags uint32

const (
	// SnapshotFlagCompressed indicates the snapshot data is gzip compressed.
	SnapshotFlagCompressed SnapshotFlags = 1 << iota

	// SnapshotFlagIncremental indicates this is an incremental snapshot.
	SnapshotFlagIncremental

	// SnapshotFlagEncrypted indicates the snapshot is encrypted.
	SnapshotFlagEncrypted
)

// SnapshotMetadata contains information about a snapshot.
type SnapshotMetadata struct {
	ID          uint64        // Unique snapshot ID
	Timestamp   time.Time     // When the snapshot was created
	EntryCount  uint64        // Number of entries in the snapshot
	Size        int64         // Size of the snapshot file in bytes
	Checksum    uint32        // CRC32 checksum of the snapshot data
	Flags       SnapshotFlags // Snapshot flags
	Path        string        // File path of the snapshot
	WALSequence uint64        // WAL sequence number at time of snapshot
	Version     uint32        // Snapshot format version
}

// IsCompressed returns true if the snapshot is compressed.
func (m *SnapshotMetadata) IsCompressed() bool {
	return m.Flags&SnapshotFlagCompressed != 0
}

// IsIncremental returns true if this is an incremental snapshot.
func (m *SnapshotMetadata) IsIncremental() bool {
	return m.Flags&SnapshotFlagIncremental != 0
}

// SnapshotHeader represents the binary header of a snapshot file.
type SnapshotHeader struct {
	Magic      uint32
	Version    uint32
	Timestamp  int64
	EntryCount uint64
	Checksum   uint32
	Flags      uint32
}

// Encode encodes the header to bytes.
func (h *SnapshotHeader) Encode() []byte {
	buf := make([]byte, SnapshotHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.Version)
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(buf[16:24], h.EntryCount)
	binary.BigEndian.PutUint32(buf[24:28], h.Checksum)
	binary.BigEndian.PutUint32(buf[28:32], h.Flags)
	return buf
}

// DecodeSnapshotHeader decodes a header from bytes.
func DecodeSnapshotHeader(data []byte) (*SnapshotHeader, error) {
	if len(data) < SnapshotHeaderSize {
		return nil, ErrSnapshotInvalid
	}

	header := &SnapshotHeader{
		Magic:      binary.BigEndian.Uint32(data[0:4]),
		Version:    binary.BigEndian.Uint32(data[4:8]),
		Timestamp:  int64(binary.BigEndian.Uint64(data[8:16])),
		EntryCount: binary.BigEndian.Uint64(data[16:24]),
		Checksum:   binary.BigEndian.Uint32(data[24:28]),
		Flags:      binary.BigEndian.Uint32(data[28:32]),
	}

	if header.Magic != SnapshotMagicNumber {
		return nil, ErrSnapshotInvalid
	}

	return header, nil
}

// SnapshotOptions configures snapshot behavior.
type SnapshotOptions struct {
	// Directory where snapshots are stored.
	Dir string

	// Compress snapshots using gzip.
	Compress bool

	// Maximum number of snapshots to retain.
	MaxSnapshots int

	// Minimum interval between snapshots.
	MinInterval time.Duration

	// Buffer size for reading/writing snapshots.
	BufferSize int
}

// DefaultSnapshotOptions returns default snapshot options.
func DefaultSnapshotOptions(dir string) *SnapshotOptions {
	return &SnapshotOptions{
		Dir:          dir,
		Compress:     true,
		MaxSnapshots: 5,
		MinInterval:  5 * time.Minute,
		BufferSize:   SnapshotBufferSize,
	}
}

// SnapshotManager handles snapshot creation and restoration.
type SnapshotManager struct {
	opts       *SnapshotOptions
	mu         sync.RWMutex
	inProgress atomic.Bool
	lastTime   time.Time
	sequence   atomic.Uint64
}

// NewSnapshotManager creates a new snapshot manager.
func NewSnapshotManager(opts *SnapshotOptions) (*SnapshotManager, error) {
	if opts == nil {
		return nil, errors.New("snapshot options cannot be nil")
	}

	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	sm := &SnapshotManager{
		opts: opts,
	}

	// Initialize sequence from existing snapshots
	snapshots, err := sm.List()
	if err == nil && len(snapshots) > 0 {
		sm.sequence.Store(snapshots[len(snapshots)-1].ID)
	}

	return sm, nil
}

// Create creates a new snapshot from the given data.
func (sm *SnapshotManager) Create(data *ShardedMap) (*SnapshotMetadata, error) {
	// Check if snapshot is already in progress
	if !sm.inProgress.CompareAndSwap(false, true) {
		return nil, ErrSnapshotInProgress
	}
	defer sm.inProgress.Store(false)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check minimum interval
	if time.Since(sm.lastTime) < sm.opts.MinInterval && !sm.lastTime.IsZero() {
		return nil, fmt.Errorf("snapshot interval not reached, wait %v", sm.opts.MinInterval-time.Since(sm.lastTime))
	}

	// Generate snapshot ID and path
	id := sm.sequence.Add(1)
	timestamp := time.Now()
	filename := fmt.Sprintf("%s%016d-%d%s", SnapshotFilePrefix, id, timestamp.Unix(), SnapshotFileSuffix)
	path := filepath.Join(sm.opts.Dir, filename)

	// Create snapshot file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Collect all entries
	entries := data.Entries()

	// Create header
	header := &SnapshotHeader{
		Magic:      SnapshotMagicNumber,
		Version:    SnapshotVersion,
		Timestamp:  timestamp.UnixNano(),
		EntryCount: uint64(len(entries)),
		Flags:      0,
	}

	if sm.opts.Compress {
		header.Flags |= uint32(SnapshotFlagCompressed)
	}

	// Write header placeholder (we'll update checksum later)
	if _, err := file.Write(header.Encode()); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("failed to write snapshot header: %w", err)
	}

	// Set up writer (with optional compression)
	var writer io.Writer
	var gzipWriter *gzip.Writer
	bufWriter := bufio.NewWriterSize(file, sm.opts.BufferSize)

	if sm.opts.Compress {
		gzipWriter, err = gzip.NewWriterLevel(bufWriter, gzip.BestSpeed)
		if err != nil {
			os.Remove(path)
			return nil, fmt.Errorf("failed to create gzip writer: %w", err)
		}
		writer = gzipWriter
	} else {
		writer = bufWriter
	}

	// Write entries and compute checksum
	hasher := crc32.NewIEEE()
	multiWriter := io.MultiWriter(writer, hasher)

	for _, entry := range entries {
		encoded := entry.Encode()

		// Write entry length
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(encoded)))
		if _, err := multiWriter.Write(lenBuf); err != nil {
			os.Remove(path)
			return nil, fmt.Errorf("failed to write entry length: %w", err)
		}

		// Write entry data
		if _, err := multiWriter.Write(encoded); err != nil {
			os.Remove(path)
			return nil, fmt.Errorf("failed to write entry data: %w", err)
		}
	}

	// Close gzip writer if used
	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			os.Remove(path)
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}
	}

	// Flush buffer
	if err := bufWriter.Flush(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("failed to flush buffer: %w", err)
	}

	// Update header with checksum
	header.Checksum = hasher.Sum32()
	if _, err := file.WriteAt(header.Encode(), 0); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("failed to update snapshot header: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("failed to stat snapshot file: %w", err)
	}

	sm.lastTime = timestamp

	// Clean up old snapshots
	if sm.opts.MaxSnapshots > 0 {
		go sm.cleanup()
	}

	return &SnapshotMetadata{
		ID:         id,
		Timestamp:  timestamp,
		EntryCount: uint64(len(entries)),
		Size:       stat.Size(),
		Checksum:   header.Checksum,
		Flags:      SnapshotFlags(header.Flags),
		Path:       path,
		Version:    SnapshotVersion,
	}, nil
}

// Restore restores data from a snapshot file.
func (sm *SnapshotManager) Restore(path string) ([]*Entry, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	// Read header
	headerBuf := make([]byte, SnapshotHeaderSize)
	if _, err := io.ReadFull(file, headerBuf); err != nil {
		return nil, fmt.Errorf("failed to read snapshot header: %w", err)
	}

	header, err := DecodeSnapshotHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	// Set up reader (with optional decompression)
	var reader io.Reader
	bufReader := bufio.NewReaderSize(file, sm.opts.BufferSize)

	if header.Flags&uint32(SnapshotFlagCompressed) != 0 {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufReader
	}

	// Read entries and verify checksum
	hasher := crc32.NewIEEE()
	teeReader := io.TeeReader(reader, hasher)

	entries := make([]*Entry, 0, header.EntryCount)
	lenBuf := make([]byte, 4)

	for i := uint64(0); i < header.EntryCount; i++ {
		// Read entry length
		if _, err := io.ReadFull(teeReader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read entry length: %w", err)
		}
		entryLen := binary.BigEndian.Uint32(lenBuf)

		// Read entry data
		entryBuf := make([]byte, entryLen)
		if _, err := io.ReadFull(teeReader, entryBuf); err != nil {
			return nil, fmt.Errorf("failed to read entry data: %w", err)
		}

		// Decode entry
		entry, err := DecodeEntry(entryBuf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode entry: %w", err)
		}

		// Skip expired entries
		if !entry.IsExpired() {
			entries = append(entries, entry)
		}
	}

	// Verify checksum
	if hasher.Sum32() != header.Checksum {
		return nil, ErrSnapshotCorrupted
	}

	return entries, nil
}

// RestoreLatest restores from the most recent snapshot.
func (sm *SnapshotManager) RestoreLatest() ([]*Entry, *SnapshotMetadata, error) {
	snapshots, err := sm.List()
	if err != nil {
		return nil, nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil, ErrSnapshotNotFound
	}

	// Get the latest snapshot
	latest := snapshots[len(snapshots)-1]

	entries, err := sm.Restore(latest.Path)
	if err != nil {
		return nil, nil, err
	}

	return entries, latest, nil
}

// List returns all available snapshots sorted by timestamp.
func (sm *SnapshotManager) List() ([]*SnapshotMetadata, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	pattern := filepath.Join(sm.opts.Dir, SnapshotFilePrefix+"*"+SnapshotFileSuffix)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	snapshots := make([]*SnapshotMetadata, 0, len(files))

	for _, path := range files {
		meta, err := sm.readMetadata(path)
		if err != nil {
			continue // Skip corrupted snapshots
		}
		snapshots = append(snapshots, meta)
	}

	// Sort by ID (which corresponds to creation order)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].ID < snapshots[j].ID
	})

	return snapshots, nil
}

// readMetadata reads only the metadata from a snapshot file.
func (sm *SnapshotManager) readMetadata(path string) (*SnapshotMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read header
	headerBuf := make([]byte, SnapshotHeaderSize)
	if _, err := io.ReadFull(file, headerBuf); err != nil {
		return nil, err
	}

	header, err := DecodeSnapshotHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Extract ID from filename
	filename := filepath.Base(path)
	idStr := strings.TrimPrefix(filename, SnapshotFilePrefix)
	idStr = strings.Split(idStr, "-")[0]
	id, _ := strconv.ParseUint(idStr, 10, 64)

	return &SnapshotMetadata{
		ID:         id,
		Timestamp:  time.Unix(0, header.Timestamp),
		EntryCount: header.EntryCount,
		Size:       stat.Size(),
		Checksum:   header.Checksum,
		Flags:      SnapshotFlags(header.Flags),
		Path:       path,
		Version:    header.Version,
	}, nil
}

// Delete removes a snapshot by ID.
func (sm *SnapshotManager) Delete(id uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	pattern := filepath.Join(sm.opts.Dir, fmt.Sprintf("%s%016d-*%s", SnapshotFilePrefix, id, SnapshotFileSuffix))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return ErrSnapshotNotFound
	}

	for _, path := range files {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to delete snapshot: %w", err)
		}
	}

	return nil
}

// cleanup removes old snapshots exceeding MaxSnapshots.
func (sm *SnapshotManager) cleanup() {
	snapshots, err := sm.List()
	if err != nil {
		return
	}

	// Remove oldest snapshots if we have too many
	for len(snapshots) > sm.opts.MaxSnapshots {
		oldest := snapshots[0]
		_ = sm.Delete(oldest.ID)
		snapshots = snapshots[1:]
	}
}

// Verify verifies the integrity of a snapshot.
func (sm *SnapshotManager) Verify(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	// Read header
	headerBuf := make([]byte, SnapshotHeaderSize)
	if _, err := io.ReadFull(file, headerBuf); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	header, err := DecodeSnapshotHeader(headerBuf)
	if err != nil {
		return err
	}

	// Set up reader
	var reader io.Reader
	bufReader := bufio.NewReaderSize(file, sm.opts.BufferSize)

	if header.Flags&uint32(SnapshotFlagCompressed) != 0 {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			return ErrSnapshotCorrupted
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufReader
	}

	// Read all data and compute checksum
	hasher := crc32.NewIEEE()
	if _, err := io.Copy(hasher, reader); err != nil {
		return fmt.Errorf("failed to read snapshot data: %w", err)
	}

	if hasher.Sum32() != header.Checksum {
		return ErrSnapshotCorrupted
	}

	return nil
}

// InProgress returns true if a snapshot is currently being created.
func (sm *SnapshotManager) InProgress() bool {
	return sm.inProgress.Load()
}

// LastSnapshotTime returns the time of the last snapshot.
func (sm *SnapshotManager) LastSnapshotTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastTime
}

// SnapshotStats provides statistics about snapshots.
type SnapshotStats struct {
	TotalSnapshots int
	TotalSize      int64
	OldestSnapshot time.Time
	NewestSnapshot time.Time
	AverageSize    int64
}

// Stats returns statistics about all snapshots.
func (sm *SnapshotManager) Stats() (*SnapshotStats, error) {
	snapshots, err := sm.List()
	if err != nil {
		return nil, err
	}

	stats := &SnapshotStats{
		TotalSnapshots: len(snapshots),
	}

	if len(snapshots) == 0 {
		return stats, nil
	}

	for _, snap := range snapshots {
		stats.TotalSize += snap.Size
	}

	stats.OldestSnapshot = snapshots[0].Timestamp
	stats.NewestSnapshot = snapshots[len(snapshots)-1].Timestamp
	stats.AverageSize = stats.TotalSize / int64(len(snapshots))

	return stats, nil
}

// Clear removes all snapshots.
func (sm *SnapshotManager) Clear() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	pattern := filepath.Join(sm.opts.Dir, SnapshotFilePrefix+"*"+SnapshotFileSuffix)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, path := range files {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", path, err)
		}
	}

	sm.sequence.Store(0)
	sm.lastTime = time.Time{}

	return nil
}
