package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	// Setup test database
	config := Config{
		DataDir:          "./testdata",
		MaxFileSize:      1024 * 1024, // 1MB
		CacheSize:        100,
		CompressionLevel: 0,
	}

	// Clean up test directory after tests
	defer os.RemoveAll(config.DataDir)

	t.Run("Database Creation", func(t *testing.T) {
		db, err := New("test_db", config)
		assert.NoError(t, err)
		assert.NotNil(t, db)

		// Try creating the same database again
		db2, err := New("test_db", config)
		assert.NoError(t, err)
		assert.NotNil(t, db2)
	})

	t.Run("Table Operations", func(t *testing.T) {
		db, err := New("test_db", config)
		assert.NoError(t, err)

		// Create table
		err = db.CreateTable("users", []Column{
			{Name: "id", Type: Int, PrimaryKey: true},
			{Name: "name", Type: String},
			{Name: "age", Type: Int},
		})
		assert.NoError(t, err)

		// Try creating the same table again
		err = db.CreateTable("users", []Column{})
		assert.Error(t, err)
		assert.Equal(t, ErrTableExists, err)

		// Get table
		table, err := db.GetTable("users")
		assert.NoError(t, err)
		assert.Equal(t, "users", table.Name)
		assert.Equal(t, 3, len(table.Columns))

		// List tables
		tables, err := db.ListTables()
		assert.NoError(t, err)
		assert.Contains(t, tables, "users")
	})

	t.Run("Data Operations", func(t *testing.T) {
		db, err := New("test_db", config)
		assert.NoError(t, err)

		// Insert data
		userData := map[string]interface{}{
			"id":   1,
			"name": "John Doe",
			"age":  30,
		}
		err = db.Insert("users", userData)
		assert.NoError(t, err)

		// Query data
		results, err := db.Query("users", []string{"id", "name", "age"},
			map[string]interface{}{"id": 1}, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, userData["name"], results[0]["name"])

		// Update data
		updateData := map[string]interface{}{
			"age": 31,
		}
		err = db.Update("users", updateData, map[string]interface{}{"id": 1})
		assert.NoError(t, err)

		// Query updated data
		results, err = db.Query("users", []string{"age"},
			map[string]interface{}{"id": 1}, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, 31, results[0]["age"])

		// Delete data
		err = db.Delete("users", map[string]interface{}{"id": 1})
		assert.NoError(t, err)

		// Query deleted data
		results, err = db.Query("users", []string{"id"},
			map[string]interface{}{"id": 1}, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results))
	})
}
