package db

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkDatabase(b *testing.B) {
	// Setup test database
	config := Config{
		DataDir:          "./benchdata",
		MaxFileSize:      1024 * 1024, // 1MB
		CacheSize:        1000,
		CompressionLevel: 0,
	}

	// Clean up test directory after benchmarks
	defer os.RemoveAll(config.DataDir)

	database, err := New("bench_db", config)
	if err != nil {
		b.Fatal(err)
	}

	// Create test table
	err = database.CreateTable("users", []Column{
		{Name: "id", Type: Int, PrimaryKey: true},
		{Name: "name", Type: String},
		{Name: "age", Type: Int},
		{Name: "email", Type: String},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Create index on age
	err = database.CreateIndex("users", CreateIndexOptions{
		Name:    "idx_age",
		Type:    BTree,
		Columns: []string{"age"},
		Unique:  false,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := database.Insert("users", map[string]interface{}{
				"id":    i,
				"name":  fmt.Sprintf("User%d", i),
				"age":   i % 100,
				"email": fmt.Sprintf("user%d@example.com", i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Insert some data for query benchmarks
	for i := 0; i < 1000; i++ {
		err := database.Insert("users", map[string]interface{}{
			"id":    i + b.N,
			"name":  fmt.Sprintf("User%d", i),
			"age":   i % 100,
			"email": fmt.Sprintf("user%d@example.com", i),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Query_ByPrimaryKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			id := i % 1000
			_, err := database.Query(
				"users",
				[]string{"id", "name", "age"},
				map[string]interface{}{"id": id},
				1,
				0,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query_ByIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			age := i % 100
			_, err := database.Query(
				"users",
				[]string{"id", "name", "age"},
				map[string]interface{}{"age": age},
				10,
				0,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Update", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			id := i % 1000
			err := database.Update(
				"users",
				map[string]interface{}{"age": i % 100},
				map[string]interface{}{"id": id},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			id := i % 1000
			err := database.Delete(
				"users",
				map[string]interface{}{"id": id},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDatabaseParallel(b *testing.B) {
	// Setup test database
	config := Config{
		DataDir:          "./benchdata_parallel",
		MaxFileSize:      1024 * 1024, // 1MB
		CacheSize:        1000,
		CompressionLevel: 0,
	}

	// Clean up test directory after benchmarks
	defer os.RemoveAll(config.DataDir)

	database, err := New("bench_db", config)
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	// Create test table
	err = database.CreateTable("users", []Column{
		{Name: "id", Type: Int, PrimaryKey: true},
		{Name: "name", Type: String},
		{Name: "age", Type: Int},
		{Name: "email", Type: String},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Create index on age
	err = database.CreateIndex("users", CreateIndexOptions{
		Name:    "idx_age",
		Type:    BTree,
		Columns: []string{"age"},
		Unique:  false,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Insert_Parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				err := database.Insert("users", map[string]interface{}{
					"id":    i,
					"name":  fmt.Sprintf("User%d", i),
					"age":   i % 100,
					"email": fmt.Sprintf("user%d@example.com", i),
				})
				if err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})

	// Insert some data for query benchmarks
	for i := 0; i < 1000; i++ {
		err := database.Insert("users", map[string]interface{}{
			"id":    i + 10000,
			"name":  fmt.Sprintf("User%d", i),
			"age":   i % 100,
			"email": fmt.Sprintf("user%d@example.com", i+10000),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Query_Parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				age := i % 100
				_, err := database.Query(
					"users",
					[]string{"id", "name", "age"},
					map[string]interface{}{"age": age},
					10,
					0,
				)
				if err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})
}
