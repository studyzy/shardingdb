# shardingdb

This package enables LevelDB to support sharding and concurrent reads/writes, and can be used as a drop-in replacement for LevelDB.

-----------

## Requirements

* Requires at least `go1.14` or newer.

## How to use

### 1. Resharding
#### 1.0 Build the resharding tool
```bash
make
cd bin
```
#### 1.1 Migrate data from LevelDB to new shardingdb
For example, if you have 1 LevelDB data and want to migrate it to 3 shardingdb data, print summary log(1), you can run the following command:
```bash
./resharding -i /data1 -o /newfolder1,/newfolder2,/newfolder3 -l 1
```
#### 1.2 Add sharding db
For example, if you have 1 LevelDB data and want to add 2 more LevelDB folders to shardingdb, print no log(0), you can run the following command:
```bash

```bash
./resharding -i /data1 -o /data1,/data2,/data3 
```

For example, if you have 3 LevelDB data and want to add 1 more LevelDB folder to shardingdb, print detail log(2), you can run the following command:
```bash

```bash
./resharding -i /data1,/data2,/data3 -o /data1,/data2,/data3,/data4 -l 2
```

### 2. Code example
#### 2.0 Get the package
```go
go get github.com/studyzy/shardingdb
```
#### 2.1 Import the package
```go
import "github.com/studyzy/shardingdb"
```
#### 2.2 Use shardingdb
```go
inputPathList := []string{"/data1", "/data2"}
sdb, err := shardingdb.OpenFile(inputPathList, nil)
sdb.Put([]byte("key"), []byte("value"), nil)
sdb.Get([]byte("key"), nil)
...
```
### 2.3 Another example
```go
db1, err := leveldb.OpenFile(getTempDir(), nil)
if err != nil {
    t.Fatal(err)
}
db2, err := leveldb.OpenFile(getTempDir(), nil)
if err != nil {
    t.Fatal(err)
}
// Create a new sharding db
sdb, err := NewShardingDb(Sha256Sharding, db1, db2)
...
```


Most interfaces are the same as [goleveldb](https://github.com/syndtr/goleveldb). For my interface definition, please refer to [DbHandle](https://github.com/studyzy/shardingdb/blob/main/interfaces.go).