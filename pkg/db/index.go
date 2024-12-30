package db

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// IndexEntry represents a single index entry
type IndexEntry struct {
	Key   interface{}
	Value interface{}
}

// MemoryIndex is a simple in-memory index implementation
type MemoryIndex struct {
	entries []IndexEntry
	mu      sync.RWMutex
}

// NewMemoryIndex creates a new memory index
func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		entries: make([]IndexEntry, 0),
	}
}

// Add adds a new entry to the index
func (idx *MemoryIndex) Add(key, value interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries = append(idx.entries, IndexEntry{Key: key, Value: value})
	sort.Slice(idx.entries, func(i, j int) bool {
		return compareValues(idx.entries[i].Key, idx.entries[j].Key) < 0
	})
	return nil
}

// Remove removes an entry from the index
func (idx *MemoryIndex) Remove(key interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for i, entry := range idx.entries {
		if entry.Key == key {
			idx.entries = append(idx.entries[:i], idx.entries[i+1:]...)
			return nil
		}
	}
	return nil
}

// Find finds entries in the index that match the given key
func (idx *MemoryIndex) Find(key interface{}) ([]interface{}, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []interface{}
	for _, entry := range idx.entries {
		if entry.Key == key {
			results = append(results, entry.Value)
		}
	}
	return results, nil
}

// Range finds entries in the index within the given range
func (idx *MemoryIndex) Range(start, end interface{}) ([]interface{}, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []interface{}
	for _, entry := range idx.entries {
		if compareValues(entry.Key, start) >= 0 && compareValues(entry.Key, end) <= 0 {
			results = append(results, entry.Value)
		}
	}
	return results, nil
}

// Clear removes all entries from the index
func (idx *MemoryIndex) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries = make([]IndexEntry, 0)
	return nil
}

// IndexManager manages indexes for a table
type IndexManager struct {
	indexes map[string]*struct {
		index   *MemoryIndex
		columns []string
	}
	mu sync.RWMutex
}

// NewIndexManager creates a new index manager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*struct {
			index   *MemoryIndex
			columns []string
		}),
	}
}

// CreateIndex creates a new index for the specified column
func (im *IndexManager) CreateIndex(name string, columns []string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	im.indexes[name] = &struct {
		index   *MemoryIndex
		columns []string
	}{
		index:   NewMemoryIndex(),
		columns: columns,
	}
	return nil
}

// DropIndex drops the index for the specified column
func (im *IndexManager) DropIndex(name string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; !exists {
		return fmt.Errorf("index %s not found", name)
	}

	delete(im.indexes, name)
	return nil
}

// GetIndex returns the index for the specified column
func (im *IndexManager) GetIndex(name string) (*MemoryIndex, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx, exists := im.indexes[name]
	if !exists {
		return nil, fmt.Errorf("index %s not found", name)
	}

	return idx.index, nil
}

// HasIndex checks if an index exists for the specified column
func (im *IndexManager) HasIndex(name string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()

	_, exists := im.indexes[name]
	return exists
}

// IndexRecord indexes a record
func (im *IndexManager) IndexRecord(record map[string]interface{}) error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for name, idx := range im.indexes {
		// For multi-column indexes, create a composite key
		var key interface{}
		if len(idx.columns) == 1 {
			key = record[idx.columns[0]]
		} else {
			keys := make([]interface{}, len(idx.columns))
			for i, col := range idx.columns {
				keys[i] = record[col]
			}
			key = keys
		}

		if err := idx.index.Add(key, record); err != nil {
			return fmt.Errorf("failed to index record for index %s: %w", name, err)
		}
	}
	return nil
}

// RemoveRecord removes a record from all indexes
func (im *IndexManager) RemoveRecord(record map[string]interface{}) error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for name, idx := range im.indexes {
		// For multi-column indexes, create a composite key
		var key interface{}
		if len(idx.columns) == 1 {
			key = record[idx.columns[0]]
		} else {
			keys := make([]interface{}, len(idx.columns))
			for i, col := range idx.columns {
				keys[i] = record[col]
			}
			key = keys
		}

		if err := idx.index.Remove(key); err != nil {
			return fmt.Errorf("failed to remove record from index %s: %w", name, err)
		}
	}
	return nil
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	switch v1 := a.(type) {
	case int:
		v2, ok := b.(int)
		if !ok {
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case float64:
		v2, ok := b.(float64)
		if !ok {
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2, ok := b.(string)
		if !ok {
			return 0
		}
		return strings.Compare(v1, v2)
	default:
		return 0
	}
}
