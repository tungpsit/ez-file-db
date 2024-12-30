package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tungpsit/ez-file-db/pkg/storage"
)

var (
	ErrDatabaseExists   = errors.New("database already exists")
	ErrDatabaseNotFound = errors.New("database not found")
	ErrTableExists      = errors.New("table already exists")
	ErrTableNotFound    = errors.New("table not found")
	ErrInvalidDataType  = errors.New("invalid data type")
	ErrInvalidOperation = errors.New("invalid operation")
)

const (
	schemaTableName = "_schema"
)

// Database represents the interface for database operations
type Database interface {
	// Database Management
	Drop() error
	Close() error

	// Table Operations
	CreateTable(name string, columns []Column) error
	DropTable(name string) error
	GetTable(name string) (*Table, error)
	ListTables() ([]string, error)

	// Index Operations
	CreateIndex(table string, options CreateIndexOptions) error
	DropIndex(table, indexName string) error
	ListIndexes(table string) ([]IndexInfo, error)

	// Data Operations
	Insert(table string, data map[string]interface{}) error
	Update(table string, data map[string]interface{}, where map[string]interface{}) error
	Delete(table string, where map[string]interface{}) error
	Query(table string, columns []string, where map[string]interface{}, limit, offset int) ([]map[string]interface{}, error)
}

// database implements the Database interface
type database struct {
	name    string
	config  Config
	tables  map[string]*Table
	storage *storage.FileStorage
	indexes map[string]*IndexManager
	mu      sync.RWMutex
}

// New creates a new database instance or opens an existing one
func New(name string, config Config) (Database, error) {
	db := &database{
		name:    name,
		config:  config,
		tables:  make(map[string]*Table),
		indexes: make(map[string]*IndexManager),
	}

	// Create data directory if it doesn't exist
	dbPath := filepath.Join(config.DataDir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize storage
	storage, err := storage.NewFileStorage(dbPath, config.MaxFileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}
	db.storage = storage

	// Check if database exists
	schemaPath := filepath.Join(dbPath, schemaTableName)
	exists := true
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		exists = false
	}

	if !exists {
		// Initialize new database
		if err := db.initializeDatabase(); err != nil {
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else {
		// Load existing database
		if err := db.loadTables(); err != nil {
			return nil, fmt.Errorf("failed to load tables: %w", err)
		}
	}

	return db, nil
}

// initializeDatabase initializes a new database with schema table
func (db *database) initializeDatabase() error {
	// Create schema table directory
	schemaPath := filepath.Join(db.config.DataDir, db.name, schemaTableName)
	if err := os.MkdirAll(schemaPath, 0755); err != nil {
		return fmt.Errorf("failed to create schema directory: %w", err)
	}

	// Create schema table
	now := time.Now()
	schemaTable := &Table{
		Name: schemaTableName,
		Columns: []Column{
			{Name: "name", Type: String, PrimaryKey: true},
			{Name: "schema", Type: String},
		},
		MaxFileSize: db.config.MaxFileSize,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	db.tables[schemaTableName] = schemaTable

	// Create schema record for the schema table itself
	schemaData, err := json.Marshal(tableSchema{
		Name:        schemaTableName,
		Columns:     schemaTable.Columns,
		PrimaryKey:  "name",
		CreatedAt:   now,
		UpdatedAt:   now,
		MaxFileSize: db.config.MaxFileSize,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal schema table schema: %w", err)
	}

	record := &storage.Record{
		ID: schemaTableName,
		Data: map[string]interface{}{
			"name":   schemaTableName,
			"schema": string(schemaData),
		},
		Version: time.Now().UnixNano(),
	}

	if err := db.storage.Write(schemaTableName, record); err != nil {
		return fmt.Errorf("failed to create schema table: %w", err)
	}

	return nil
}

// Drop implements Database.Drop
func (db *database) Drop() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbPath := filepath.Join(db.config.DataDir, db.name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return ErrDatabaseNotFound
	}

	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}

	return nil
}

// Close implements Database.Close
func (db *database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Implement cleanup logic here
	return nil
}

// CreateTable implements Database.CreateTable
func (db *database) CreateTable(name string, columns []Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return ErrTableExists
	}

	now := time.Now()
	table := &Table{
		Name:        name,
		Columns:     columns,
		MaxFileSize: db.config.MaxFileSize,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Validate columns and set primary key
	for _, col := range columns {
		if col.PrimaryKey {
			table.PrimaryKey = col.Name
			break
		}
	}

	if table.PrimaryKey == "" {
		return fmt.Errorf("table must have a primary key")
	}

	// Create index manager for the table
	indexManager := NewIndexManager()
	// Create index for primary key
	if err := indexManager.CreateIndex("pk_"+table.PrimaryKey, []string{table.PrimaryKey}); err != nil {
		return fmt.Errorf("failed to create primary key index: %w", err)
	}

	// Create indexes for unique columns
	for _, col := range columns {
		if col.Unique && col.Name != table.PrimaryKey {
			if err := indexManager.CreateIndex("idx_"+col.Name, []string{col.Name}); err != nil {
				return fmt.Errorf("failed to create unique index for column %s: %w", col.Name, err)
			}
		}
	}

	db.indexes[name] = indexManager

	// Persist table schema
	schema := tableSchema{
		Name:        name,
		Columns:     columns,
		PrimaryKey:  table.PrimaryKey,
		CreatedAt:   table.CreatedAt,
		UpdatedAt:   table.UpdatedAt,
		MaxFileSize: table.MaxFileSize,
	}

	schemaData, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	record := &storage.Record{
		ID: name,
		Data: map[string]interface{}{
			"name":   name,
			"schema": string(schemaData),
		},
		Version: time.Now().UnixNano(),
	}

	if err := db.storage.Write(schemaTableName, record); err != nil {
		return fmt.Errorf("failed to persist schema: %w", err)
	}

	db.tables[name] = table
	return nil
}

// GetTable implements Database.GetTable
func (db *database) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[name]
	if !exists {
		return nil, ErrTableNotFound
	}

	return table, nil
}

// ListTables implements Database.ListTables
func (db *database) ListTables() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// Insert implements Database.Insert
func (db *database) Insert(tableName string, data map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Validate data against schema
	if err := validateData(table, data); err != nil {
		return err
	}

	// Get primary key value
	id, ok := data[table.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key %s is required", table.PrimaryKey)
	}

	// Check unique constraints
	indexManager := db.indexes[tableName]
	for _, col := range table.Columns {
		if col.Unique {
			if value, exists := data[col.Name]; exists {
				var indexName string
				if col.PrimaryKey {
					indexName = "pk_" + col.Name
				} else {
					indexName = "idx_" + col.Name
				}
				if index, err := indexManager.GetIndex(indexName); err == nil {
					if results, err := index.Find(value); err == nil && len(results) > 0 {
						return fmt.Errorf("unique constraint violation for column %s", col.Name)
					}
				}
			}
		}
	}

	// Create record
	record := &storage.Record{
		ID:      id,
		Data:    data,
		Version: time.Now().UnixNano(),
	}

	// Write to storage
	if err := db.storage.Write(tableName, record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Update indexes
	if err := indexManager.IndexRecord(data); err != nil {
		// Rollback storage write on index error
		_ = db.storage.Delete(tableName, id)
		return fmt.Errorf("failed to update indexes: %w", err)
	}

	return nil
}

// Update implements Database.Update
func (db *database) Update(tableName string, data map[string]interface{}, where map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Validate update data against schema
	if err := validateData(table, data); err != nil {
		return err
	}

	// Get primary key value from where clause
	id, ok := where[table.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key %s is required in where clause", table.PrimaryKey)
	}

	// Read existing record
	record, err := db.storage.Read(tableName, id)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}
	if record == nil {
		return fmt.Errorf("record not found")
	}

	// Check unique constraints for updated values
	indexManager := db.indexes[tableName]
	for _, col := range table.Columns {
		if col.Unique {
			if value, exists := data[col.Name]; exists && value != record.Data[col.Name] {
				if index, err := indexManager.GetIndex(col.Name); err == nil {
					if results, err := index.Find(value); err == nil && len(results) > 0 {
						return fmt.Errorf("unique constraint violation for column %s", col.Name)
					}
				}
			}
		}
	}

	// Remove old index entries
	if err := indexManager.RemoveRecord(record.Data); err != nil {
		return fmt.Errorf("failed to remove old index entries: %w", err)
	}

	// Update record data
	for k, v := range data {
		record.Data[k] = v
	}
	record.Version = time.Now().UnixNano()

	// Write updated record
	if err := db.storage.Write(tableName, record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Update indexes with new values
	if err := indexManager.IndexRecord(record.Data); err != nil {
		return fmt.Errorf("failed to update indexes: %w", err)
	}

	return nil
}

// Delete implements Database.Delete
func (db *database) Delete(tableName string, where map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Get primary key value from where clause
	id, ok := where[table.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key %s is required in where clause", table.PrimaryKey)
	}

	// Read existing record to update indexes
	record, err := db.storage.Read(tableName, id)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}
	if record == nil {
		return nil // Record doesn't exist, nothing to delete
	}

	// Remove index entries
	indexManager := db.indexes[tableName]
	if err := indexManager.RemoveRecord(record.Data); err != nil {
		return fmt.Errorf("failed to remove index entries: %w", err)
	}

	// Delete from storage
	if err := db.storage.Delete(tableName, id); err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

// Query implements Database.Query
func (db *database) Query(tableName string, columns []string, where map[string]interface{}, limit, offset int) ([]map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[tableName]
	if !exists {
		return nil, ErrTableNotFound
	}

	// Validate requested columns
	if err := validateColumns(table, columns); err != nil {
		return nil, err
	}

	indexManager := db.indexes[tableName]
	var results []map[string]interface{}

	// Try to use index for primary key lookup
	if id, ok := where[table.PrimaryKey]; ok {
		if index, err := indexManager.GetIndex(table.PrimaryKey); err == nil {
			if records, err := index.Find(id); err == nil && len(records) > 0 {
				for _, record := range records {
					if data, ok := record.(map[string]interface{}); ok {
						result := projectColumns(data, columns)
						results = append(results, result)
					}
				}
				return applyLimitOffset(results, limit, offset), nil
			}
		}
	}

	// Try to use other indexes
	for column := range where {
		if indexManager.HasIndex(column) {
			if index, err := indexManager.GetIndex(column); err == nil {
				if records, err := index.Find(where[column]); err == nil {
					for _, record := range records {
						if data, ok := record.(map[string]interface{}); ok {
							if matchesWhere(data, where) {
								result := projectColumns(data, columns)
								results = append(results, result)
							}
						}
					}
					return applyLimitOffset(results, limit, offset), nil
				}
			}
		}
	}

	// Fall back to full table scan
	var currentOffset int
	var count int

	err := db.storage.Scan(tableName, func(record *storage.Record) error {
		if matchesWhere(record.Data, where) {
			if currentOffset < offset {
				currentOffset++
				return nil
			}

			if limit > 0 && count >= limit {
				return nil
			}

			result := projectColumns(record.Data, columns)
			results = append(results, result)
			count++
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan records: %w", err)
	}

	return results, nil
}

// projectColumns creates a new map with only the requested columns
func projectColumns(data map[string]interface{}, columns []string) map[string]interface{} {
	if len(columns) == 0 {
		result := make(map[string]interface{})
		for k, v := range data {
			result[k] = v
		}
		return result
	}

	result := make(map[string]interface{})
	for _, col := range columns {
		if value, exists := data[col]; exists {
			result[col] = value
		}
	}
	return result
}

// matchesWhere checks if a record matches the where conditions
func matchesWhere(data map[string]interface{}, where map[string]interface{}) bool {
	for k, v := range where {
		if value, exists := data[k]; !exists || value != v {
			return false
		}
	}
	return true
}

// applyLimitOffset applies limit and offset to results
func applyLimitOffset(results []map[string]interface{}, limit, offset int) []map[string]interface{} {
	if offset >= len(results) {
		return []map[string]interface{}{}
	}

	end := len(results)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	return results[offset:end]
}

// validateData validates data against table schema
func validateData(table *Table, data map[string]interface{}) error {
	for _, col := range table.Columns {
		value, exists := data[col.Name]
		if !exists {
			if col.NotNull {
				return fmt.Errorf("column %s is required", col.Name)
			}
			continue
		}

		if err := validateDataType(col.Type, value); err != nil {
			return fmt.Errorf("invalid data type for column %s: %w", col.Name, err)
		}
	}
	return nil
}

// validateColumns validates requested columns against table schema
func validateColumns(table *Table, columns []string) error {
	if len(columns) == 0 {
		return nil
	}

	columnMap := make(map[string]bool)
	for _, col := range table.Columns {
		columnMap[col.Name] = true
	}

	for _, col := range columns {
		if !columnMap[col] {
			return fmt.Errorf("column %s not found in table %s", col, table.Name)
		}
	}
	return nil
}

// validateDataType validates a value against a DataType
func validateDataType(dt DataType, value interface{}) error {
	switch dt {
	case Int:
		switch value.(type) {
		case int, int32, int64:
			return nil
		}
	case Float:
		switch value.(type) {
		case float32, float64:
			return nil
		}
	case String:
		if _, ok := value.(string); ok {
			return nil
		}
	case Boolean:
		if _, ok := value.(bool); ok {
			return nil
		}
	case DateTime:
		switch value.(type) {
		case time.Time:
			return nil
		}
	default:
		return ErrInvalidDataType
	}
	return fmt.Errorf("invalid type for %v", dt)
}

// DropTable implements Database.DropTable
func (db *database) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; !exists {
		return ErrTableNotFound
	}

	// Delete schema record
	if err := db.storage.Delete(schemaTableName, name); err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	delete(db.tables, name)
	return nil
}

// loadTables loads all table schemas from storage
func (db *database) loadTables() error {
	return db.storage.Scan(schemaTableName, func(record *storage.Record) error {
		schemaStr, ok := record.Data["schema"].(string)
		if !ok {
			return fmt.Errorf("invalid schema data for table %v", record.ID)
		}

		var schema tableSchema
		if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
			return fmt.Errorf("failed to unmarshal schema: %w", err)
		}

		table := &Table{
			Name:        schema.Name,
			Columns:     schema.Columns,
			PrimaryKey:  schema.PrimaryKey,
			CreatedAt:   schema.CreatedAt,
			UpdatedAt:   schema.UpdatedAt,
			MaxFileSize: schema.MaxFileSize,
		}

		db.tables[schema.Name] = table
		return nil
	})
}

// CreateIndex implements Database.CreateIndex
func (db *database) CreateIndex(table string, options CreateIndexOptions) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	t, exists := db.tables[table]
	if !exists {
		return ErrTableNotFound
	}

	// Validate columns
	columnMap := make(map[string]bool)
	for _, col := range t.Columns {
		columnMap[col.Name] = true
	}
	for _, col := range options.Columns {
		if !columnMap[col] {
			return fmt.Errorf("column %s not found in table %s", col, table)
		}
	}

	// Check if index already exists
	for _, idx := range t.Indexes {
		if idx.Name == options.Name {
			return fmt.Errorf("index %s already exists", options.Name)
		}
	}

	// Create index in memory
	indexManager := db.indexes[table]
	if err := indexManager.CreateIndex(options.Name, options.Columns); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Add index info to table
	t.Indexes = append(t.Indexes, IndexInfo(options))

	// Update table schema
	if err := db.updateTableSchema(t); err != nil {
		// Rollback index creation
		indexManager.DropIndex(options.Name)
		return fmt.Errorf("failed to update table schema: %w", err)
	}

	// Build index data
	err := db.storage.Scan(table, func(record *storage.Record) error {
		return indexManager.IndexRecord(record.Data)
	})
	if err != nil {
		// Rollback index creation
		indexManager.DropIndex(options.Name)
		return fmt.Errorf("failed to build index: %w", err)
	}

	return nil
}

// DropIndex implements Database.DropIndex
func (db *database) DropIndex(table, indexName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	t, exists := db.tables[table]
	if !exists {
		return ErrTableNotFound
	}

	// Find and remove index info
	found := false
	for i, idx := range t.Indexes {
		if idx.Name == indexName {
			t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("index %s not found", indexName)
	}

	// Drop index from memory
	indexManager := db.indexes[table]
	if err := indexManager.DropIndex(indexName); err != nil {
		return fmt.Errorf("failed to drop index: %w", err)
	}

	// Update table schema
	if err := db.updateTableSchema(t); err != nil {
		return fmt.Errorf("failed to update table schema: %w", err)
	}

	return nil
}

// ListIndexes implements Database.ListIndexes
func (db *database) ListIndexes(table string) ([]IndexInfo, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	t, exists := db.tables[table]
	if !exists {
		return nil, ErrTableNotFound
	}

	indexes := make([]IndexInfo, len(t.Indexes))
	copy(indexes, t.Indexes)
	return indexes, nil
}

// updateTableSchema updates the persisted table schema
func (db *database) updateTableSchema(table *Table) error {
	schema := tableSchema{
		Name:        table.Name,
		Columns:     table.Columns,
		PrimaryKey:  table.PrimaryKey,
		CreatedAt:   table.CreatedAt,
		UpdatedAt:   time.Now(),
		MaxFileSize: table.MaxFileSize,
	}

	schemaData, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	record := &storage.Record{
		ID: table.Name,
		Data: map[string]interface{}{
			"name":   table.Name,
			"schema": string(schemaData),
		},
		Version: time.Now().UnixNano(),
	}

	if err := db.storage.Write(schemaTableName, record); err != nil {
		return fmt.Errorf("failed to persist schema: %w", err)
	}

	return nil
}

// Additional method implementations will be added for other Database interface methods

// tableSchema represents the persisted table schema
type tableSchema struct {
	Name        string    `json:"name"`
	Columns     []Column  `json:"columns"`
	PrimaryKey  string    `json:"primary_key"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	MaxFileSize int64     `json:"max_file_size"`
}
