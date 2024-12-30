# EZ File DB

A high-performance file-based database system written in Go that stores tabular data similar to SQL databases, optimized for memory usage and prevents out-of-memory issues.

## Features

### Core Features
- Database Management (Create, modify, and delete databases)
- Table and Schema Management
- CRUD Operations with SQL-like queries
- Memory-Optimized File Management
- Indexing and Caching
- Concurrent Operations Support
- Transaction Support (ACID)
- Data Validation and Constraints

### Additional Features
- REST API Interface
- Authentication and Authorization
- Backup and Restore
- Monitoring and Management Tools
- Import/Export Functionality

## Installation

```bash
go get github.com/tungpsit/ez-file-db
```

## Quick Start

```go
package main

import (
    "github.com/tungpsit/ez-file-db/db"
)

func main() {
    // Create a new database
    database, err := db.New("mydb", db.DefaultConfig())
    if err != nil {
        panic(err)
    }
    defer database.Close()

    // Create a table
    err = database.CreateTable("users", []db.Column{
        {Name: "id", Type: db.Int, PrimaryKey: true},
        {Name: "name", Type: db.String},
        {Name: "age", Type: db.Int},
    })
    if err != nil {
        panic(err)
    }

    // Insert data
    err = database.Insert("users", map[string]interface{}{
        "id": 1,
        "name": "John Doe",
        "age": 30,
    })
    if err != nil {
        panic(err)
    }
}
```

## Documentation

For detailed documentation, please visit our [Wiki](https://github.com/tungpsit/ez-file-db/wiki).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 