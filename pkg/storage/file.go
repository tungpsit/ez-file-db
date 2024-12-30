package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage handles the file-based storage operations
type FileStorage struct {
	basePath    string
	maxFileSize int64
	mu          sync.RWMutex
}

// Record represents a single data record
type Record struct {
	ID      interface{}            `json:"id"`
	Data    map[string]interface{} `json:"data"`
	Version int64                  `json:"version"`
}

// NewFileStorage creates a new FileStorage instance
func NewFileStorage(basePath string, maxFileSize int64) (*FileStorage, error) {
	return &FileStorage{
		basePath:    basePath,
		maxFileSize: maxFileSize,
	}, nil
}

// Write writes a record to storage
func (fs *FileStorage) Write(tableName string, record *Record) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	filePath := fs.getFilePath(tableName, record.ID)
	dir := filepath.Dir(filePath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	// Check if current file size would exceed max size
	if fs.shouldRotateFile(filePath) {
		filePath = fs.getNextFilePath(tableName, record.ID)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

// Read reads a record from storage
func (fs *FileStorage) Read(tableName string, id interface{}) (*Record, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	filePath := fs.getFilePath(tableName, id)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal record: %w", err)
	}

	return &record, nil
}

// Delete removes a record from storage
func (fs *FileStorage) Delete(tableName string, id interface{}) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	filePath := fs.getFilePath(tableName, id)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

// getFilePath returns the file path for a record
func (fs *FileStorage) getFilePath(tableName string, id interface{}) string {
	return filepath.Join(fs.basePath, tableName, fmt.Sprintf("%v.json", id))
}

// shouldRotateFile checks if the current file should be rotated
func (fs *FileStorage) shouldRotateFile(filePath string) bool {
	info, err := os.Stat(filePath)
	if err != nil {
		return false
	}
	return info.Size() >= fs.maxFileSize
}

// getNextFilePath returns the next available file path for rotation
func (fs *FileStorage) getNextFilePath(tableName string, id interface{}) string {
	base := filepath.Join(fs.basePath, tableName)
	pattern := fmt.Sprintf("%v_*.json", id)
	matches, _ := filepath.Glob(filepath.Join(base, pattern))
	return filepath.Join(base, fmt.Sprintf("%v_%d.json", id, len(matches)))
}

// Scan performs a sequential scan of records in a table
func (fs *FileStorage) Scan(tableName string, fn func(*Record) error) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	dir := filepath.Join(fs.basePath, tableName)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var record Record
			if err := decoder.Decode(&record); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to decode record: %w", err)
			}

			if err := fn(&record); err != nil {
				return err
			}
		}

		return nil
	})
}
