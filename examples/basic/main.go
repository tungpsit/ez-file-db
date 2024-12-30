package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/tungpsit/ez-file-db/pkg/db"
)

func main() {
	// Create a new database instance
	config := db.DefaultConfig()

	// remove the example_db directory
	os.RemoveAll(filepath.Join(config.DataDir, "example_db"))

	database, err := db.New("example_db", config)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Create a users table
	err = database.CreateTable("users", []db.Column{
		{Name: "id", Type: db.Int, PrimaryKey: true},
		{Name: "name", Type: db.String},
		{Name: "age", Type: db.Int},
		{Name: "email", Type: db.String, Unique: true},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create an index on the age column
	err = database.CreateIndex("users", db.CreateIndexOptions{
		Name:    "idx_age",
		Type:    db.BTree,
		Columns: []string{"age"},
		Unique:  false,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Insert some data
	users := []map[string]interface{}{
		{
			"id":    1,
			"name":  "John Doe",
			"age":   30,
			"email": "john@example.com",
		},
		{
			"id":    2,
			"name":  "Jane Smith",
			"age":   25,
			"email": "jane@example.com",
		},
		{
			"id":    3,
			"name":  "Bob Johnson",
			"age":   30,
			"email": "bob@example.com",
		},
	}

	for _, user := range users {
		if err := database.Insert("users", user); err != nil {
			log.Fatal(err)
		}
	}

	// Query data using the age index
	fmt.Println("\nQuerying users with age = 30:")
	result, err := database.Query(
		"users",
		[]string{"id", "name", "age"},
		map[string]interface{}{"age": 30},
		0,
		0,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Print results
	for _, record := range result {
		fmt.Printf("ID: %v, Name: %v, Age: %v\n",
			record["id"],
			record["name"],
			record["age"],
		)
	}

	// List indexes
	fmt.Println("\nTable indexes:")
	indexes, err := database.ListIndexes("users")
	if err != nil {
		log.Fatal(err)
	}

	for _, index := range indexes {
		fmt.Printf("- %s (columns: %v, type: %v, unique: %v)\n",
			index.Name,
			index.Columns,
			index.Type,
			index.Unique,
		)
	}

	// Try to insert a duplicate email (should fail)
	fmt.Println("\nTrying to insert a duplicate email:")
	err = database.Insert("users", map[string]interface{}{
		"id":    4,
		"name":  "Alice Brown",
		"age":   35,
		"email": "john@example.com", // Duplicate email
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Update data
	fmt.Println("\nUpdating user age:")
	updateData := map[string]interface{}{
		"age": 31,
	}
	if err := database.Update("users", updateData, map[string]interface{}{"id": 1}); err != nil {
		log.Fatal(err)
	}

	// Query updated data
	result, err = database.Query(
		"users",
		[]string{"id", "name", "age"},
		map[string]interface{}{"id": 1},
		1,
		0,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Updated user:")
	for _, record := range result {
		fmt.Printf("ID: %v, Name: %v, Age: %v\n",
			record["id"],
			record["name"],
			record["age"],
		)
	}

	// Drop the age index
	if err := database.DropIndex("users", "idx_age"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nDatabase operations completed successfully!")
}
