

![GitHub top language](https://img.shields.io/github/languages/top/youzeliang/rdb) ![GitHub stars](https://img.shields.io/github/stars/youzeliang/rdb)


English | [简体中文](README_CN.md)


RDB is a high-performance, Bitcask-based, embeddable Key-Value storage engine implemented in Go. It employs a log-structured merge (LSM-like) storage approach with multiple index type support, offering a rich set of features and functionalities.

! [paper address](https://riak.com/assets/bitcask-intro.pdf)

## Features

- Multiple Index Types Support
    - B-Tree Index
    - Adaptive Radix Tree (ART) Index
    - B+ Tree Index (with persistence support)
- High-performance Read/Write Operations
- Transaction Support
- Data Persistence and Recovery
- Data File Merge Operations
- MMap Loading Support
- Batch Write Operations
- Database Backup Support
- Iterator Support
- File Locking for Data Safety

## Design Details

### 1. Storage Engine Architecture

The storage engine uses a log-structured approach with the following components:

- **Data Files**: Size-based segmented files, including active and archived files
- **Memory Index**: Support for multiple index implementations
- **Write-Ahead Log**: Ensures data durability and consistency
- **Merge Mechanism**: Space reclamation through merge operations
- **Transaction Management**: Supports atomic operations

### 2. Data Organization

- **Data Files**:
    - Configurable file size (default 256MB)
    - Append-only writing
    - MMap loading support

- **Index Structures**:
    - B-Tree: Balanced tree index, suitable for general scenarios
    - ART: Adaptive Radix Tree, memory-efficient
    - B+ Tree: Persistent tree-based index

### 3. Main Configuration configs

```go
type configs struct {
    DirPath            string      // Database directory path
    FileSize       int64       // Size of data files
    SyncWrites         bool        // Whether to sync writes
    IndexType          IndexerType // Type of index to use
    BytesPerSync       int         // Bytes to accumulate before sync
    MMapAtStartup      bool        // Whether to use MMap at startup
    DataFileMergeRatio float32     // Threshold for data file merging
}
```

## Usage Examples

```go
// Open database
configs := rdbrdb.DefaultOptions
configs.DirPath = "/tmp/rdb"
db, err := rdbrdb.Open(configs)
if err != nil {
    panic(err)
}
defer db.Close()

// Write data
err = db.Put([]byte("key"), []byte("value"))

// Read data
value, err := db.Get([]byte("key"))

// Delete data
err = db.Delete([]byte("key"))

// Batch write
batch := db.NewWriteBatch(rdbrdb.DefaultWriteBatchConfigs)
batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
err = batch.Commit()
```

## Advanced Features

### 1. Iterator

Support for iterating over data in key dictionary order:
```go
configs := rdbrdb.DefaultIteratorConfigs
iterator := db.Iterator(configs)
for iterator.Rewind(); iterator.Valid(); iterator.Next() {
    key := iterator.Key()
    value := iterator.Value()
}
```

### 2. Database Backup

Support for online database backup:
```go
err := db.Backup("/path/to/backup")
```

### 3. Transaction Support

Provides atomic batch write support:
```go
batch := db.NewWriteBatch(rdbrdb.DefaultWriteBatchConfigs)
defer batch.Commit()

batch.Put([]byte("key1"), []byte("value1"))
batch.Delete([]byte("key2"))
```

## Performance Optimizations

1. **MMap Loading**: Uses memory mapping for accelerated data loading
2. **Batch Writing**: Efficient batch write operations through WriteBatch
3. **Async Persistence**: Configurable data accumulation before persistence
4. **Space Reclamation**: Invalid data space reclamation through merge operations

## Important Notes

1. Ensure proper read/write permissions for the data directory
2. Configure appropriate data file size and merge threshold
3. Choose suitable index type based on your scenario
4. Ensure all operations are complete before closing the database

## Contributing

Issues and Pull Requests are welcome!

## License

MIT License
