# levels

`levels` is a basic LevelDB clone

## Context

LevelDB introduces a toolkit for managing in-memory key-value stores with support for serialization into a simple SSTable (Sorted Strings Table) format. This implementation includes robust implementations of various data structures, such as simple key-value stores, linked lists, and skip lists, each equipped with CRUD operations and range scans. In addition, efficient serialization of in-memory data to disk in an SSTable format enables high-performance access to immutable data.

## Features

- **Simple Key-Value Store**: A basic in-memory key-value store with straightforward get, put, and delete operations.
- **Linked List**: An implementation of a doubly linked list for ordered data storage and access.
- **Skip List**: A probabilistic data structure offering efficient insert, delete, and search operations with complexity comparable to balanced trees.
- **SSTable Serialization**: Utilities to serialize the in-memory data into an SSTable format, enabling efficient disk storage and range scans.

## Quickstart

### Creating a Key-Value Store

Choose one of the available data structures and instantiate it:

```go
db := NewSimpleDB() // For a simple key-value store
db := NewLinkedListDB() // For a linked list-based store
db := NewSkipListDB() // For a skip list-based store
```

### Basic Operations

- **Put**: Add or update a key-value pair.

```go
err := db.Put([]byte("key"), []byte("value"))

if err != nil {
    log.Fatal(err)
}
```

- **Get**: Retrieve the value for a given key.

```go
value, err := db.Get([]byte("key"))

if err != nil {
    log.Fatal(err)
}

fmt.Println("Value:", string(value))
```

- **Delete**: Remove a key-value pair.

```go
err := db.Delete([]byte("key"))

if err != nil {
    log.Fatal(err)
}
```

### Range Scan

Scan for key-value pairs within a specified key range:

```go
iter, err := db.RangeScan([]byte("startKey"), []byte("endKey"))

if err != nil {
    log.Fatal(err)
}

for iter.Next() {
    fmt.Printf("Key: %s, Value: %s", iter.Key(), iter.Value())
}

if err := iter.Error(); err != nil {
    log.Fatal(err)
}
```

### Flushing to Disk

Serialize the in-memory store to an SSTable format:

```go
file, err := os.Create("path/to/sstable")

if err != nil {
    log.Fatal(err)
}

defer file.Close()

err = db.Flush(file)

if err != nil {
    log.Fatal(err)
}
```

### Running Tests

To run tests for this module, execute:

```bash
go test ./...
```

*These exercises were completed as a part of the 4th module of Bradfield's [Computer Science Intensive](https://bradfieldcs.com/csi) program.*
