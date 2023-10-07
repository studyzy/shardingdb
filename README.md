# shardingdb

ShardingDB is an open-source, sharded database enhancing LevelDB with concurrent reads/writes support. 
It significantly improves performance, boosting PutData by 60x and GetData by 7x, making it an ideal drop-in replacement for LevelDB.
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
sdb, err := shardingdb.NewShardingDb(shardingdb.WithDbHandles(db1,db2), shardingdb.WithShardingFunc(MurmurSharding))
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
result means the time cost(second) of the whole operation.
### 1. PutData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.27  |        0.659         |        0.581         |            0.953             |
|   200B    |  4.45  |         1.07         |        0.683         |             1.9              |
|   500B    |  15.3  |         3.36         |         1.49         |             6.4              |
|    1KB    |  48.9  |         9.42         |         3.74         |            17.69             |
|   10KB    |  1117  |         351          |         123          |             308              |

### 2. GetData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.23  |         1.25         |         1.02         |             1.86             |
|   200B    |  3.09  |         1.42         |         1.27         |             2.24             |
|   500B    |  4.17  |         1.91         |         1.62         |             3.73             |
|    1KB    |  7.97  |         2.37         |         2.26         |             4.53             |
|   10KB    | 12.75  |         9.54         |        11.03         |            13.85             |

### 3. GetData not found

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  2.14  |         1.36         |         0.87         |             1.43             |
|   200B    |  2.07  |         1.47         |         0.9          |             1.6              |
|   500B    |  2.05  |         1.51         |         0.93         |             1.81             |
|    1KB    |  2.35  |         1.64         |        0.891         |             2.28             |
|   10KB    |  8.68  |         5.56         |         2.48         |             7.75             |


### 4. DeleteData

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    |  3.82  |         2.76         |         1.02         |             1.72             |
|   200B    |  3.81  |         1.71         |         1.02         |             1.74             |
|   500B    |  3.85  |         1.76         |         1.05         |             1.69             |
|    1KB    |  3.84  |         1.72         |         1.04         |             1.74             |
|   10KB    | 3.844  |         1.78         |         1.06         |             1.76             |

### 5. Iterator

| Data Size | LevelDB | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(encrypt 3 folders) |
|:---------:|:-------:|:---------------------:|:---------------------:|:-----------------------------:|
|   100B    | 0.133  |        0.184         |        0.222         |             0.18             |
|   200B    | 0.151  |        0.246         |        0.246         |            0.191             |
|   500B    | 0.282  |        0.351         |         0.41         |            0.344             |
|    1KB    | 0.514  |        0.419         |        0.472         |            0.541             |
|   10KB    |  2.46  |         2.39         |         1.96         |             2.3              |


### 6. Sharding count compare
run command:
```bash
go test  -timeout 60m -run "TestCompareShardingCountPerformance"
```

#### 6.1 PutData
| Data Size | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(9 folders) | ShardingDB(30 folders) | ShardingDB(60 folders) |
|:---------:|:---------------------:|:---------------------:|:---------------------:|:----------------------:|:----------------------:|
|   100B    |        0.659         |        0.581         |        0.506         |         0.564         |         0.728         |
|   200B    |         1.07         |        0.683         |        0.624         |         0.685         |         0.782         |
|   500B    |         3.36         |         1.49         |         1.20         |         1.18          |         1.21          |
|    1KB    |         9.42         |         3.74         |         2.33         |         1.92          |         1.96          |
|   10KB    |         351          |         123          |          54          |          26           |         18.2          |
#### 6.2 GetData
| Data Size | ShardingDB(3 folders) | ShardingDB(6 folders) | ShardingDB(9 folders) | ShardingDB(30 folders) | ShardingDB(60 folders) |
|:---------:|:---------------------:|:---------------------:|:---------------------:|:----------------------:|:----------------------:|
|   100B    |         1.25         |         1.02         |         1.03         |         0.343         |         0.366         |
|   200B    |         1.42         |         1.27         |         1.01         |         0.66          |         0.373         |
|   500B    |         1.91         |         1.62         |         1.21         |         0.96          |         1.34          |
|    1KB    |         2.37         |         2.26         |         1.83         |         1.18          |         1.19          |
|   10KB    |         9.54         |        11.03         |         7.67         |          4.8          |          3.4          |


Most interfaces are the same as [goleveldb](https://github.com/syndtr/goleveldb). For my interface definition, please refer to [DbHandle](https://github.com/studyzy/shardingdb/blob/main/interfaces.go).