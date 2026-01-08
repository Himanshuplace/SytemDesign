// Package encoding provides utilities for serializing and deserializing data.
package encoding

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

// Common errors for encoding operations.
var (
	ErrInvalidData      = errors.New("invalid data format")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrDecompression    = errors.New("decompression failed")
	ErrUnsupportedType  = errors.New("unsupported type")
)

// Encoder defines the interface for encoding data.
type Encoder interface {
	Encode(v any) ([]byte, error)
}

// Decoder defines the interface for decoding data.
type Decoder interface {
	Decode(data []byte, v any) error
}

// Codec combines encoding and decoding capabilities.
type Codec interface {
	Encoder
	Decoder
}

// BinaryCodec provides binary encoding/decoding for primitive types.
type BinaryCodec struct {
	order binary.ByteOrder
}

// NewBinaryCodec creates a new BinaryCodec with the specified byte order.
func NewBinaryCodec(order binary.ByteOrder) *BinaryCodec {
	if order == nil {
		order = binary.BigEndian
	}
	return &BinaryCodec{order: order}
}

// DefaultBinaryCodec returns a BinaryCodec with big-endian byte order.
func DefaultBinaryCodec() *BinaryCodec {
	return NewBinaryCodec(binary.BigEndian)
}

// Encode encodes a value to bytes using binary encoding.
func (c *BinaryCodec) Encode(v any) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, c.order, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes bytes into a value using binary encoding.
func (c *BinaryCodec) Decode(data []byte, v any) error {
	return binary.Read(bytes.NewReader(data), c.order, v)
}

// JSONCodec provides JSON encoding/decoding.
type JSONCodec struct {
	indent bool
}

// NewJSONCodec creates a new JSONCodec.
func NewJSONCodec(indent bool) *JSONCodec {
	return &JSONCodec{indent: indent}
}

// Encode encodes a value to JSON bytes.
func (c *JSONCodec) Encode(v any) ([]byte, error) {
	if c.indent {
		return json.MarshalIndent(v, "", "  ")
	}
	return json.Marshal(v)
}

// Decode decodes JSON bytes into a value.
func (c *JSONCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// GobCodec provides gob encoding/decoding.
type GobCodec struct{}

// NewGobCodec creates a new GobCodec.
func NewGobCodec() *GobCodec {
	return &GobCodec{}
}

// Encode encodes a value using gob encoding.
func (c *GobCodec) Encode(v any) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes gob bytes into a value.
func (c *GobCodec) Decode(data []byte, v any) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(v)
}

// Compressor provides compression/decompression functionality.
type Compressor struct {
	level int
	pool  sync.Pool
}

// NewCompressor creates a new Compressor with the specified compression level.
// Level should be between gzip.BestSpeed (1) and gzip.BestCompression (9),
// or gzip.DefaultCompression (-1).
func NewCompressor(level int) *Compressor {
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}
	c := &Compressor{level: level}
	c.pool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(nil, level)
			return w
		},
	}
	return c
}

// DefaultCompressor returns a Compressor with default compression level.
func DefaultCompressor() *Compressor {
	return NewCompressor(gzip.DefaultCompression)
}

// Compress compresses the input data using gzip.
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := c.pool.Get().(*gzip.Writer)
	defer c.pool.Put(w)

	w.Reset(buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress decompresses gzip data.
func (c *Compressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, ErrDecompression
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		return nil, ErrDecompression
	}

	return result, nil
}

// ChecksumWriter wraps data with a CRC32 checksum.
type ChecksumWriter struct{}

// NewChecksumWriter creates a new ChecksumWriter.
func NewChecksumWriter() *ChecksumWriter {
	return &ChecksumWriter{}
}

// Wrap adds a CRC32 checksum to the data.
// Format: [data][checksum(4 bytes)]
func (w *ChecksumWriter) Wrap(data []byte) []byte {
	checksum := crc32.ChecksumIEEE(data)
	result := make([]byte, len(data)+4)
	copy(result, data)
	binary.BigEndian.PutUint32(result[len(data):], checksum)
	return result
}

// Unwrap verifies and removes the CRC32 checksum from data.
func (w *ChecksumWriter) Unwrap(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, ErrInvalidData
	}

	payload := data[:len(data)-4]
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	computedChecksum := crc32.ChecksumIEEE(payload)

	if storedChecksum != computedChecksum {
		return nil, ErrChecksumMismatch
	}

	return payload, nil
}

// Verify checks if the data has a valid checksum without modifying it.
func (w *ChecksumWriter) Verify(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	payload := data[:len(data)-4]
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	computedChecksum := crc32.ChecksumIEEE(payload)

	return storedChecksum == computedChecksum
}

// EncodeString encodes a string with length prefix.
// Format: [length(4 bytes)][string bytes]
func EncodeString(s string) []byte {
	b := []byte(s)
	result := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(result, uint32(len(b)))
	copy(result[4:], b)
	return result
}

// DecodeString decodes a length-prefixed string.
func DecodeString(data []byte) (string, int, error) {
	if len(data) < 4 {
		return "", 0, ErrInvalidData
	}

	length := binary.BigEndian.Uint32(data)
	if len(data) < int(4+length) {
		return "", 0, ErrInvalidData
	}

	return string(data[4 : 4+length]), int(4 + length), nil
}

// EncodeBytes encodes a byte slice with length prefix.
// Format: [length(4 bytes)][bytes]
func EncodeBytes(b []byte) []byte {
	result := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(result, uint32(len(b)))
	copy(result[4:], b)
	return result
}

// DecodeBytes decodes a length-prefixed byte slice.
func DecodeBytes(data []byte) ([]byte, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInvalidData
	}

	length := binary.BigEndian.Uint32(data)
	if len(data) < int(4+length) {
		return nil, 0, ErrInvalidData
	}

	result := make([]byte, length)
	copy(result, data[4:4+length])

	return result, int(4 + length), nil
}

// EncodeUint64 encodes a uint64 as big-endian bytes.
func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// DecodeUint64 decodes big-endian bytes to uint64.
func DecodeUint64(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, ErrInvalidData
	}
	return binary.BigEndian.Uint64(data), nil
}

// EncodeUint32 encodes a uint32 as big-endian bytes.
func EncodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

// DecodeUint32 decodes big-endian bytes to uint32.
func DecodeUint32(data []byte) (uint32, error) {
	if len(data) < 4 {
		return 0, ErrInvalidData
	}
	return binary.BigEndian.Uint32(data), nil
}

// EncodeInt64 encodes an int64 as big-endian bytes.
func EncodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

// DecodeInt64 decodes big-endian bytes to int64.
func DecodeInt64(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, ErrInvalidData
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// EncodeVarint encodes an int64 using variable-length encoding.
func EncodeVarint(v int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	return buf[:n]
}

// DecodeVarint decodes a varint from data.
func DecodeVarint(data []byte) (int64, int, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, 0, ErrInvalidData
	}
	return v, n, nil
}

// EncodeUvarint encodes a uint64 using variable-length encoding.
func EncodeUvarint(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

// DecodeUvarint decodes a uvarint from data.
func DecodeUvarint(data []byte) (uint64, int, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, ErrInvalidData
	}
	return v, n, nil
}

// EncodedSize returns the total encoded size for a string or byte slice.
func EncodedSize(data []byte) int {
	return 4 + len(data)
}

// EncodedStringSize returns the total encoded size for a string.
func EncodedStringSize(s string) int {
	return 4 + len(s)
}
