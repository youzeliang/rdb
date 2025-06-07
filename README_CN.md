RDB 是一个用 Go 语言实现的高性能、基于Bitcask模型、可嵌入的 Key-Value 存储引擎。它采用日志结构化合并（LSM-like）的存储方式，支持多种索引类型，并提供了丰富的功能特性。

! [论文地址](https://riak.com/assets/bitcask-intro.pdf)


## 特性

- 支持多种索引类型
    - B-Tree 索引 
    - 自适应基数树（ART）索引
    - B+ 树索引（支持持久化）
- 高性能的读写操作
- 支持事务操作
- 数据持久化和故障恢复
- 支持数据文件合并（Merge）操作
- 支持 MMap 方式加载数据
- 支持批量写入操作
- 支持数据备份
- 支持迭代器遍历
- 文件锁保证数据安全

## 设计细节

### 1. 存储引擎架构

存储引擎采用日志结构化的存储方式，主要包含以下组件：

- **数据文件**：按大小切分的数据文件，包含活跃文件和归档文件
- **内存索引**：支持多种索引实现
- **写前日志**：保证数据的持久性和一致性
- **Merge 机制**：通过合并操作回收空间
- **事务管理**：支持原子性操作

### 2. 数据组织

- **数据文件**：
    - 文件大小可配置
    - 使用追加写入方式
    - 支持 MMap 方式加载

- **索引结构**：
    - B-Tree：平衡树索引，适用于一般场景
    - ART：自适应基数树，内存效率高
    - B+ 树：支持持久化的树形索引

### 3. 主要配置选项

```go
type configs struct {
    DirPath            string      // 数据库数据目录
    FileSize       int64       // 数据文件的大小
    SyncWrites         bool        // 每次写数据是否持久化
    IndexType          IndexerType // 索引类型
    BytesPerSync       int         // 积累多少字节写入后进行持久化
    MMapAtStartup      bool        // 启动时是否使用 MMap 加载数据
    DataFileMergeRatio float32     // 数据文件合并的阈值
}
```

## 使用示例

```go
// 打开数据库
configs := rdb.DefaultOptions
configs.DirPath = "/tmp/rdb"
db, err := rdb.Open(configs)
if err != nil {
    panic(err)
}
defer db.Close()

// 写入数据
err = db.Put([]byte("key"), []byte("value"))

// 读取数据
value, err := db.Get([]byte("key"))

// 删除数据
err = db.Delete([]byte("key"))

// 批量写入
batch := db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
err = batch.Commit()
```

## 高级特性

### 1. 迭代器

支持按照 key 的字典序遍历数据：
```go
configs := rdb.DefaultIteratorConfigs
iterator := db.Iterator(configs)
for iterator.Rewind(); iterator.Valid(); iterator.Next() {
    key := iterator.Key()
    value := iterator.Value()
}
```

### 2. 数据备份

支持在线备份数据库：
```go
err := db.Backup("/path/to/backup")
```

### 3. 事务支持

提供批量写入的事务支持，保证原子性：
```go
batch := db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
defer batch.Commit()

batch.Put([]byte("key1"), []byte("value1"))
batch.Delete([]byte("key2"))
```

## 性能优化

1. **MMap 加载**：支持使用内存映射加速数据加载
2. **批量写入**：通过 WriteBatch 支持高效的批量写入
3. **异步持久化**：可配置累积一定量的数据后再进行持久化
4. **空间回收**：通过 Merge 操作回收无效数据占用的空间

## 注意事项

1. 确保数据目录具有适当的读写权限
2. 合理配置数据文件大小和合并阈值
3. 根据场景选择合适的索引类型
4. 在关闭数据库前确保所有操作已完成

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License 
