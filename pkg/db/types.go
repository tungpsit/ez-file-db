package db

import (
	"time"
)

// DataType represents the supported data types in the database
type DataType int

const (
	Int DataType = iota
	Float
	String
	Boolean
	DateTime
	Blob
)

// Column represents a table column definition
type Column struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
	Default    interface{}
}

// IndexInfo represents index configuration
type IndexInfo struct {
	Name    string    `json:"name"`
	Type    IndexType `json:"type"`
	Columns []string  `json:"columns"`
	Unique  bool      `json:"unique"`
}

// Table represents a database table structure
type Table struct {
	Name        string      `json:"name"`
	Columns     []Column    `json:"columns"`
	PrimaryKey  string      `json:"primary_key"`
	Indexes     []IndexInfo `json:"indexes"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	MaxFileSize int64       `json:"max_file_size"`
}

// Index represents a table index
type Index struct {
	Name    string
	Columns []string
	Type    IndexType
}

// IndexType represents the type of index
type IndexType int

const (
	BTree IndexType = iota
	Hash
)

// Config represents the database configuration
type Config struct {
	DataDir          string
	MaxFileSize      int64  // Maximum size of each data file in bytes
	CacheSize        int    // Maximum number of records to cache
	CompressionLevel int    // Compression level (0-9, 0 = disabled)
	EnableEncryption bool   // Enable encryption at rest
	EncryptionKey    string // Encryption key (if encryption is enabled)
	MaxConnections   int    // Maximum number of concurrent connections
}

// DefaultConfig returns the default database configuration
func DefaultConfig() Config {
	return Config{
		DataDir:          "./data",
		MaxFileSize:      100 * 1024 * 1024, // 100MB
		CacheSize:        1000,
		CompressionLevel: 0,
		EnableEncryption: false,
		MaxConnections:   100,
	}
}

// CreateIndexOptions represents options for creating an index
type CreateIndexOptions struct {
	Name    string
	Type    IndexType
	Columns []string
	Unique  bool
}
