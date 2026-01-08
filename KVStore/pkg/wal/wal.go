// Package wal provides a Write-Ahead Log implementation for durability.
package wal

import (
	"bufio"
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
	"time"
)

// Record types for the WAL.
const (
	RecordTypePut    byte = 1
	RecordTypeDelete byte = 2
	RecordTypeBatch  byte = 3
)

// Common errors for WAL operations.
var (
	ErrWALClosed       = errors.New("wal: log is closed")
	ErrCorruptedRecord = errors.New("wal: corrupted record")
	ErrInvalidRecord   = errors.New("wal: invalid record")
	ErrSegmentFull     = errors.New("wal: segment full")
	ErrRecoveryFailed  = errors.New("wal: recovery failed")
)

const (
	// DefaultMaxSegmentSize is the default maximum size of a WAL segment (64MB).
	DefaultMaxSegmentSize = 64 * 1024 * 1024

	// DefaultBufferSize is the default buffer size for writes (64KB).
	DefaultBufferSize = 64 * 1024

	// SegmentFilePrefix is the prefix for WAL segment files.
	SegmentFilePrefix = "wal-"

	// SegmentFileSuffix is the suffix for WAL segment files.
	SegmentFileSuffix = ".log"

	// HeaderSize is the size of the record header in bytes.
	// Format: [checksum(4)][length(4)][type(1)] = 9 bytes
	HeaderSize = 9
)

// Record represents a single WAL record.
type Record struct {
	Type      byte
	Key       []byte
	Value     []byte
	ExpiresAt int64 // Unix nano timestamp, 0 means no expiration
	Sequence  uint64
}

// Encode serializes the record to bytes.
// Format: [keyLen(4)][key][valueLen(4)][value][expiresAt(8)]
func (r *Record) Encode() []byte {
	keyLen := len(r.Key)
	valueLen := len(r.Value)
	size := 4 + keyLen + 4 + valueLen + 8

	buf := make([]byte, size)
	offset := 0

	// Key length and key
	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4
	copy(buf[offset:], r.Key)
	offset += keyLen

	// Value length and value
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4
	copy(buf[offset:], r.Value)
	offset += valueLen

	// Expiration timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(r.ExpiresAt))

	return buf
}

// DecodeRecord deserializes a record from bytes.
func DecodeRecord(data []byte, recordType byte) (*Record, error) {
	if len(data) < 16 { // Minimum: 4 + 0 + 4 + 0 + 8
		return nil, ErrInvalidRecord
	}

	offset := 0

	// Key length and key
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+keyLen > len(data)-12 {
		return nil, ErrInvalidRecord
	}
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+keyLen])
	offset += keyLen

	// Value length and value
	valueLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+valueLen > len(data)-8 {
		return nil, ErrInvalidRecord
	}
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+valueLen])
	offset += valueLen

	// Expiration timestamp
	expiresAt := int64(binary.BigEndian.Uint64(data[offset:]))

	return &Record{
		Type:      recordType,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}, nil
}

// Options configures the WAL behavior.
type Options struct {
	Dir            string        // Directory for WAL files
	MaxSegmentSize int64         // Maximum size of a single segment file
	BufferSize     int           // Write buffer size
	SyncOnWrite    bool          // Sync to disk on every write
	SyncInterval   time.Duration // Interval for periodic sync (if SyncOnWrite is false)
}

// DefaultOptions returns default WAL options.
func DefaultOptions(dir string) *Options {
	return &Options{
		Dir:            dir,
		MaxSegmentSize: DefaultMaxSegmentSize,
		BufferSize:     DefaultBufferSize,
		SyncOnWrite:    false,
		SyncInterval:   time.Second,
	}
}

// Segment represents a single WAL segment file.
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

// newSegment creates a new segment.
func newSegment(dir string, id uint64, maxSize int64, bufferSize int) (*Segment, error) {
	filename := fmt.Sprintf("%s%016d%s", SegmentFilePrefix, id, SegmentFileSuffix)
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %w", err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment: %w", err)
	}

	return &Segment{
		id:      id,
		path:    path,
		file:    file,
		writer:  bufio.NewWriterSize(file, bufferSize),
		size:    stat.Size(),
		maxSize: maxSize,
	}, nil
}

// openSegment opens an existing segment for reading.
func openSegment(path string, maxSize int64, bufferSize int) (*Segment, error) {
	// Extract ID from filename
	filename := filepath.Base(path)
	idStr := strings.TrimPrefix(filename, SegmentFilePrefix)
	idStr = strings.TrimSuffix(idStr, SegmentFileSuffix)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid segment filename: %s", filename)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment: %w", err)
	}

	return &Segment{
		id:      id,
		path:    path,
		file:    file,
		writer:  bufio.NewWriterSize(file, bufferSize),
		size:    stat.Size(),
		maxSize: maxSize,
	}, nil
}

// Write writes a record to the segment.
func (s *Segment) Write(record *Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrWALClosed
	}

	// Encode the record payload
	payload := record.Encode()

	// Calculate the total record size
	recordSize := HeaderSize + len(payload)

	// Check if segment has space
	if s.size+int64(recordSize) > s.maxSize {
		return ErrSegmentFull
	}

	// Build the full record with header
	// Format: [checksum(4)][length(4)][type(1)][payload]
	buf := make([]byte, recordSize)

	// Length (excluding checksum and length fields)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)+1)) // +1 for type

	// Type
	buf[8] = record.Type

	// Payload
	copy(buf[9:], payload)

	// Checksum (covers length, type, and payload)
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	// Write to buffer
	n, err := s.writer.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	s.size += int64(n)
	return nil
}

// Sync flushes the buffer and syncs to disk.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrWALClosed
	}

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// Close closes the segment.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	if err := s.writer.Flush(); err != nil {
		s.file.Close()
		return fmt.Errorf("failed to flush on close: %w", err)
	}

	return s.file.Close()
}

// Size returns the current size of the segment.
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// ID returns the segment ID.
func (s *Segment) ID() uint64 {
	return s.id
}

// Path returns the segment file path.
func (s *Segment) Path() string {
	return s.path
}

// IsFull returns true if the segment is at or near capacity.
func (s *Segment) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size >= s.maxSize
}

// WAL represents the Write-Ahead Log.
type WAL struct {
	opts     *Options
	segments []*Segment
	active   *Segment
	sequence uint64
	mu       sync.RWMutex
	closed   bool
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// Open opens or creates a WAL in the specified directory.
func Open(opts *Options) (*WAL, error) {
	if opts == nil {
		return nil, errors.New("wal: options cannot be nil")
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		opts:     opts,
		segments: make([]*Segment, 0),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		return nil, err
	}

	// Create new active segment if needed
	if wal.active == nil {
		if err := wal.rotate(); err != nil {
			return nil, err
		}
	}

	// Start background sync if configured
	if !opts.SyncOnWrite && opts.SyncInterval > 0 {
		go wal.backgroundSync()
	}

	return wal, nil
}

// loadSegments loads existing segment files.
func (w *WAL) loadSegments() error {
	pattern := filepath.Join(w.opts.Dir, SegmentFilePrefix+"*"+SegmentFileSuffix)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	// Sort files by name (which includes the segment ID)
	sort.Strings(files)

	for _, path := range files {
		seg, err := openSegment(path, w.opts.MaxSegmentSize, w.opts.BufferSize)
		if err != nil {
			return err
		}
		w.segments = append(w.segments, seg)
	}

	// Set the active segment to the last one
	if len(w.segments) > 0 {
		w.active = w.segments[len(w.segments)-1]
		w.sequence = w.active.ID()
	}

	return nil
}

// rotate creates a new segment and makes it active.
func (w *WAL) rotate() error {
	w.sequence++

	seg, err := newSegment(w.opts.Dir, w.sequence, w.opts.MaxSegmentSize, w.opts.BufferSize)
	if err != nil {
		return err
	}

	// Close old active segment (but keep it in the list)
	if w.active != nil {
		if err := w.active.Sync(); err != nil {
			return err
		}
	}

	w.segments = append(w.segments, seg)
	w.active = seg

	return nil
}

// backgroundSync periodically syncs the WAL to disk.
func (w *WAL) backgroundSync() {
	ticker := time.NewTicker(w.opts.SyncInterval)
	defer ticker.Stop()
	defer close(w.doneCh)

	for {
		select {
		case <-ticker.C:
			w.mu.RLock()
			if !w.closed && w.active != nil {
				_ = w.active.Sync()
			}
			w.mu.RUnlock()
		case <-w.stopCh:
			return
		}
	}
}

// Write writes a record to the WAL.
func (w *WAL) Write(record *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	// Try to write to active segment
	err := w.active.Write(record)
	if err == ErrSegmentFull {
		// Rotate to a new segment
		if err := w.rotate(); err != nil {
			return err
		}
		err = w.active.Write(record)
	}

	if err != nil {
		return err
	}

	// Sync if configured
	if w.opts.SyncOnWrite {
		return w.active.Sync()
	}

	return nil
}

// WritePut writes a put operation to the WAL.
func (w *WAL) WritePut(key, value []byte, expiresAt int64) error {
	return w.Write(&Record{
		Type:      RecordTypePut,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	})
}

// WriteDelete writes a delete operation to the WAL.
func (w *WAL) WriteDelete(key []byte) error {
	return w.Write(&Record{
		Type: RecordTypeDelete,
		Key:  key,
	})
}

// WriteBatch writes multiple records atomically.
func (w *WAL) WriteBatch(records []*Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	for _, record := range records {
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
	}

	if w.opts.SyncOnWrite {
		return w.active.Sync()
	}

	return nil
}

// Sync syncs the WAL to disk.
func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return ErrWALClosed
	}

	return w.active.Sync()
}

// Reader provides sequential read access to WAL records.
type Reader struct {
	segments []*Segment
	segIdx   int
	reader   *bufio.Reader
	file     *os.File
}

// NewReader creates a reader for all WAL segments.
func (w *WAL) NewReader() (*Reader, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Sync active segment first
	if w.active != nil {
		if err := w.active.Sync(); err != nil {
			return nil, err
		}
	}

	// Make a copy of segments slice
	segments := make([]*Segment, len(w.segments))
	copy(segments, w.segments)

	return &Reader{
		segments: segments,
		segIdx:   -1,
	}, nil
}

// Next reads the next record from the WAL.
// Returns io.EOF when all records have been read.
func (r *Reader) Next() (*Record, error) {
	for {
		// Initialize reader for current segment if needed
		if r.reader == nil {
			r.segIdx++
			if r.segIdx >= len(r.segments) {
				return nil, io.EOF
			}

			file, err := os.Open(r.segments[r.segIdx].Path())
			if err != nil {
				return nil, err
			}
			if r.file != nil {
				r.file.Close()
			}
			r.file = file
			r.reader = bufio.NewReader(file)
		}

		// Read header
		header := make([]byte, HeaderSize)
		_, err := io.ReadFull(r.reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Move to next segment
			r.reader = nil
			continue
		}
		if err != nil {
			return nil, err
		}

		// Parse header
		storedChecksum := binary.BigEndian.Uint32(header[0:4])
		length := binary.BigEndian.Uint32(header[4:8])
		recordType := header[8]

		// Read payload
		payloadLen := int(length) - 1 // -1 for type byte
		if payloadLen < 0 {
			return nil, ErrCorruptedRecord
		}

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			_, err = io.ReadFull(r.reader, payload)
			if err != nil {
				return nil, ErrCorruptedRecord
			}
		}

		// Verify checksum
		checksumData := make([]byte, 4+1+payloadLen)
		binary.BigEndian.PutUint32(checksumData[0:4], length)
		checksumData[4] = recordType
		copy(checksumData[5:], payload)
		computedChecksum := crc32.ChecksumIEEE(checksumData)

		if storedChecksum != computedChecksum {
			return nil, ErrCorruptedRecord
		}

		// Decode record
		record, err := DecodeRecord(payload, recordType)
		if err != nil {
			return nil, err
		}

		return record, nil
	}
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Replay replays all records through the provided handler function.
// The handler is called for each record in order.
func (w *WAL) Replay(handler func(*Record) error) error {
	reader, err := w.NewReader()
	if err != nil {
		return err
	}
	defer reader.Close()

	for {
		record, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("replay error: %w", err)
		}

		if err := handler(record); err != nil {
			return err
		}
	}
}

// Truncate removes all segments before the specified segment ID.
// This is typically called after a successful snapshot.
func (w *WAL) Truncate(beforeID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	var remaining []*Segment
	for _, seg := range w.segments {
		if seg.ID() >= beforeID {
			remaining = append(remaining, seg)
		} else {
			// Close and remove the segment file
			seg.Close()
			os.Remove(seg.Path())
		}
	}

	w.segments = remaining
	return nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	// Stop background sync
	close(w.stopCh)
	if !w.opts.SyncOnWrite && w.opts.SyncInterval > 0 {
		<-w.doneCh
	}

	// Close all segments
	var lastErr error
	for _, seg := range w.segments {
		if err := seg.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Stats returns statistics about the WAL.
type Stats struct {
	SegmentCount int
	TotalSize    int64
	ActiveSize   int64
	Dir          string
}

// Stats returns current WAL statistics.
func (w *WAL) Stats() Stats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	stats := Stats{
		SegmentCount: len(w.segments),
		Dir:          w.opts.Dir,
	}

	for _, seg := range w.segments {
		stats.TotalSize += seg.Size()
	}

	if w.active != nil {
		stats.ActiveSize = w.active.Size()
	}

	return stats
}

// Clear removes all WAL files and resets the state.
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	// Close and remove all segments
	for _, seg := range w.segments {
		seg.Close()
		os.Remove(seg.Path())
	}

	w.segments = nil
	w.active = nil
	w.sequence = 0

	// Create new active segment
	return w.rotate()
}
