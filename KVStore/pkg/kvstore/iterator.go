package kvstore

import (
	"sort"
	"strings"
	"sync"
)

// Iterator provides sequential access to key-value pairs.
type Iterator interface {
	// Next advances the iterator to the next entry.
	// Returns true if there is a next entry, false if iteration is complete.
	Next() bool

	// Entry returns the current entry.
	// Returns nil if the iterator is not positioned at a valid entry.
	Entry() *Entry

	// Key returns the current entry's key.
	// Returns empty string if not positioned at a valid entry.
	Key() string

	// Value returns the current entry's value.
	// Returns nil if not positioned at a valid entry.
	Value() []byte

	// Error returns any error that occurred during iteration.
	Error() error

	// Close releases resources associated with the iterator.
	Close()

	// Reset resets the iterator to the beginning.
	Reset()
}

// IteratorOptions configures iterator behavior.
type IteratorOptions struct {
	// Prefix filters entries to only those with keys starting with this prefix.
	Prefix string

	// Start is the key to start iteration from (inclusive).
	Start string

	// End is the key to end iteration at (exclusive).
	End string

	// Reverse iterates in reverse order.
	Reverse bool

	// Limit limits the number of entries returned (0 = no limit).
	Limit int

	// IncludeExpired includes expired entries in iteration.
	IncludeExpired bool

	// Sorted sorts entries by key.
	Sorted bool
}

// DefaultIteratorOptions returns default iterator options.
func DefaultIteratorOptions() *IteratorOptions {
	return &IteratorOptions{
		Sorted: true,
	}
}

// MapIterator iterates over entries in a ShardedMap.
type MapIterator struct {
	entries []*Entry
	index   int
	opts    *IteratorOptions
	err     error
	closed  bool
	mu      sync.Mutex
}

// NewMapIterator creates a new iterator for a ShardedMap.
func NewMapIterator(m *ShardedMap, opts *IteratorOptions) *MapIterator {
	if opts == nil {
		opts = DefaultIteratorOptions()
	}

	// Collect all entries
	var entries []*Entry
	m.ForEach(func(entry *Entry) bool {
		// Apply prefix filter
		if opts.Prefix != "" && !strings.HasPrefix(entry.Key, opts.Prefix) {
			return true
		}

		// Apply range filter
		if opts.Start != "" && entry.Key < opts.Start {
			return true
		}
		if opts.End != "" && entry.Key >= opts.End {
			return true
		}

		// Check expiration
		if !opts.IncludeExpired && entry.IsExpired() {
			return true
		}

		entries = append(entries, entry.Clone())
		return true
	})

	// Sort if requested
	if opts.Sorted {
		if opts.Reverse {
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].Key > entries[j].Key
			})
		} else {
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].Key < entries[j].Key
			})
		}
	}

	// Apply limit
	if opts.Limit > 0 && len(entries) > opts.Limit {
		entries = entries[:opts.Limit]
	}

	return &MapIterator{
		entries: entries,
		index:   -1,
		opts:    opts,
	}
}

// Next advances the iterator to the next entry.
func (it *MapIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	it.index++
	return it.index < len(it.entries)
}

// Entry returns the current entry.
func (it *MapIterator) Entry() *Entry {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.index < 0 || it.index >= len(it.entries) {
		return nil
	}
	return it.entries[it.index]
}

// Key returns the current entry's key.
func (it *MapIterator) Key() string {
	entry := it.Entry()
	if entry == nil {
		return ""
	}
	return entry.Key
}

// Value returns the current entry's value.
func (it *MapIterator) Value() []byte {
	entry := it.Entry()
	if entry == nil {
		return nil
	}
	return entry.Value
}

// Error returns any error that occurred during iteration.
func (it *MapIterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

// Close releases resources associated with the iterator.
func (it *MapIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.entries = nil
}

// Reset resets the iterator to the beginning.
func (it *MapIterator) Reset() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.closed {
		it.index = -1
	}
}

// Count returns the total number of entries in the iterator.
func (it *MapIterator) Count() int {
	it.mu.Lock()
	defer it.mu.Unlock()

	return len(it.entries)
}

// Skip advances the iterator by n entries.
// Returns the number of entries actually skipped.
func (it *MapIterator) Skip(n int) int {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || n <= 0 {
		return 0
	}

	remaining := len(it.entries) - it.index - 1
	if n > remaining {
		n = remaining
	}

	it.index += n
	return n
}

// Remaining returns the number of entries remaining in the iterator.
func (it *MapIterator) Remaining() int {
	it.mu.Lock()
	defer it.mu.Unlock()

	remaining := len(it.entries) - it.index - 1
	if remaining < 0 {
		return 0
	}
	return remaining
}

// PrefixIterator provides iteration over entries with a specific prefix.
type PrefixIterator struct {
	*MapIterator
	prefix string
}

// NewPrefixIterator creates a new iterator for entries with the given prefix.
func NewPrefixIterator(m *ShardedMap, prefix string) *PrefixIterator {
	opts := DefaultIteratorOptions()
	opts.Prefix = prefix

	return &PrefixIterator{
		MapIterator: NewMapIterator(m, opts),
		prefix:      prefix,
	}
}

// Prefix returns the prefix being iterated.
func (it *PrefixIterator) Prefix() string {
	return it.prefix
}

// RangeIterator provides iteration over a range of keys.
type RangeIterator struct {
	*MapIterator
	start string
	end   string
}

// NewRangeIterator creates a new iterator for entries in the given key range.
// start is inclusive, end is exclusive.
func NewRangeIterator(m *ShardedMap, start, end string) *RangeIterator {
	opts := DefaultIteratorOptions()
	opts.Start = start
	opts.End = end

	return &RangeIterator{
		MapIterator: NewMapIterator(m, opts),
		start:       start,
		end:         end,
	}
}

// Range returns the range being iterated.
func (it *RangeIterator) Range() (string, string) {
	return it.start, it.end
}

// FilterIterator provides iteration with a custom filter function.
type FilterIterator struct {
	source Iterator
	filter func(*Entry) bool
	next   *Entry
	err    error
	closed bool
	mu     sync.Mutex
}

// NewFilterIterator creates a new iterator that filters entries using the provided function.
func NewFilterIterator(source Iterator, filter func(*Entry) bool) *FilterIterator {
	return &FilterIterator{
		source: source,
		filter: filter,
	}
}

// Next advances the iterator to the next entry that passes the filter.
func (it *FilterIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	for it.source.Next() {
		entry := it.source.Entry()
		if entry != nil && it.filter(entry) {
			it.next = entry
			return true
		}
	}

	it.err = it.source.Error()
	it.next = nil
	return false
}

// Entry returns the current entry.
func (it *FilterIterator) Entry() *Entry {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.next
}

// Key returns the current entry's key.
func (it *FilterIterator) Key() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.next == nil {
		return ""
	}
	return it.next.Key
}

// Value returns the current entry's value.
func (it *FilterIterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.next == nil {
		return nil
	}
	return it.next.Value
}

// Error returns any error that occurred during iteration.
func (it *FilterIterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

// Close releases resources associated with the iterator.
func (it *FilterIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.source.Close()
}

// Reset resets the iterator to the beginning.
func (it *FilterIterator) Reset() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.next = nil
	it.source.Reset()
}

// TransformIterator applies a transformation to each entry.
type TransformIterator struct {
	source    Iterator
	transform func(*Entry) *Entry
	current   *Entry
	err       error
	closed    bool
	mu        sync.Mutex
}

// NewTransformIterator creates a new iterator that transforms entries.
func NewTransformIterator(source Iterator, transform func(*Entry) *Entry) *TransformIterator {
	return &TransformIterator{
		source:    source,
		transform: transform,
	}
}

// Next advances the iterator to the next entry.
func (it *TransformIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	if it.source.Next() {
		entry := it.source.Entry()
		if entry != nil {
			it.current = it.transform(entry)
		}
		return true
	}

	it.err = it.source.Error()
	it.current = nil
	return false
}

// Entry returns the current transformed entry.
func (it *TransformIterator) Entry() *Entry {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.current
}

// Key returns the current entry's key.
func (it *TransformIterator) Key() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.current == nil {
		return ""
	}
	return it.current.Key
}

// Value returns the current entry's value.
func (it *TransformIterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.current == nil {
		return nil
	}
	return it.current.Value
}

// Error returns any error that occurred during iteration.
func (it *TransformIterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

// Close releases resources associated with the iterator.
func (it *TransformIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.source.Close()
}

// Reset resets the iterator to the beginning.
func (it *TransformIterator) Reset() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.current = nil
	it.source.Reset()
}

// CollectKeys collects all keys from an iterator.
func CollectKeys(it Iterator) []string {
	var keys []string
	for it.Next() {
		keys = append(keys, it.Key())
	}
	return keys
}

// CollectEntries collects all entries from an iterator.
func CollectEntries(it Iterator) []*Entry {
	var entries []*Entry
	for it.Next() {
		entry := it.Entry()
		if entry != nil {
			entries = append(entries, entry)
		}
	}
	return entries
}

// CollectValues collects all values from an iterator.
func CollectValues(it Iterator) [][]byte {
	var values [][]byte
	for it.Next() {
		values = append(values, it.Value())
	}
	return values
}

// ForEachEntry iterates over all entries and calls the provided function.
// If the function returns false, iteration stops.
func ForEachEntry(it Iterator, fn func(*Entry) bool) error {
	for it.Next() {
		entry := it.Entry()
		if entry != nil {
			if !fn(entry) {
				break
			}
		}
	}
	return it.Error()
}
