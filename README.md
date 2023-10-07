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

## Performance Benchmark

### Environment
* Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz  * 10 Core
* 40GB RAM
* 3 SSD: /data, /data1, /

generate data by command:
```bash
go test -timeout 60m -run "TestCompareDbPerformance"
```
Test case: total 1000000 key-value pairs, 100 go routines, 100 key-value pairs per batch.

### 1. PutData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.27s  |        0.659s         |        0.581s         |            0.953s             |
|   200B    |  4.45s  |         1.07s         |        0.683s         |             1.9s              |
|   500B    |  15.3s  |         3.36s         |         1.49s         |             6.4s              |
|    1KB    |  48.9s  |         9.42s         |         3.74s         |            17.69s             |
|   10KB    |  1117s  |         351s          |         123s          |             308s              |

### 2. GetData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.23s  |         1.25s         |         1.02s         |             1.86s             |
|   200B    |  3.09s  |         1.42s         |         1.27s         |             2.24s             |
|   500B    |  4.17s  |         1.91s         |         1.62s         |             3.73s             |
|    1KB    |  7.97s  |         2.37s         |         2.26s         |             4.53s             |
|   10KB    | 12.75s  |         9.54s         |        11.03s         |            13.85s             |

### 3. GetData not found

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.14s  |         1.36s         |         0.87s         |             1.43s             |
|   200B    |  2.07s  |         1.47s         |         0.9s          |             1.6s              |
|   500B    |  2.05s  |         1.51s         |         0.93s         |             1.81s             |
|    1KB    |  2.35s  |         1.64s         |        0.891s         |             2.28s             |
|   10KB    |  8.68s  |         5.56s         |         2.48s         |             7.75s             |


### 4. DeleteData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  3.82s  |         2.76s         |         1.02s         |             1.72s             |
|   200B    |  3.81s  |         1.71s         |         1.02s         |             1.74s             |
|   500B    |  3.85s  |         1.76s         |         1.05s         |             1.69s             |
|    1KB    |  3.84s  |         1.72s         |         1.04s         |             1.74s             |
|   10KB    | 3.844s  |         1.78s         |         1.06s         |             1.76s             |

### 5. Iterator

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    | 0.133s  |        0.184s         |        0.222s         |             0.18s             |
|   200B    | 0.151s  |        0.246s         |        0.246s         |            0.191s             |
|   500B    | 0.282s  |        0.351s         |         0.41s         |            0.344s             |
|    1KB    | 0.514s  |        0.419s         |        0.472s         |            0.541s             |
|   10KB    |  2.46s  |         2.39s         |         1.96s         |             2.3s              |


### 6. Sharding count compare

#### 6.1 PutData
| Data Size | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(9 folders) | ShardingDB(30 folders) | ShardingDB(60 folders)  |
|:---------:|:---------------------:|:---------------------:|:---------------------:|:----------------------:|:-----------------------:|
|   100B    |        0.659s         |        0.581s         |                       |                        |                         |
|   200B    |         1.07s         |        0.683s         |                       |                        |                         |
|   500B    |         3.36s         |         1.49s         |                       |                        |                         |
|    1KB    |         9.42s         |         3.74s         |                       |                        |                         |
|   10KB    |         351s          |         123s          |                       |                        |                         |
#### 6.2 GetData
| Data Size | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(9 folders) | ShardingDB(30 folders) | ShardingDB(60 folders) |
|:---------:|:---------------------:|:---------------------:|:---------------------:|:----------------------:|:----------------------:|
|   100B    |         1.25s         |         1.02s         |                       |                        |                        |
|   200B    |         1.42s         |         1.27s         |                       |                        |                        |
|   500B    |         1.91s         |         1.62s         |                       |                        |                        |
|    1KB    |         2.37s         |         2.26s         |                       |                        |                        |
|   10KB    |         9.54s         |         11.03s        |                       |                        |                        |


Most interfaces are the same as [goleveldb](https://github.com/syndtr/goleveldb). For my interface definition, please refer to [DbHandle](https://github.com/studyzy/shardingdb/blob/main/interfaces.go).