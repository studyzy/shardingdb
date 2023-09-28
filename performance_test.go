/*
 * Copyright [2023] [studyzy(studyzy@gmail.com)]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shardingdb

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	thread      = 100
	loop        = 100
	batchSize   = 1000
	valueLength = 200
)

func stringToBytesWithPadding(s string, length int) []byte {
	result := make([]byte, length)
	copy(result, []byte(s))
	return result
}

func BenchmarkShardingDb_Put(b *testing.B) {
	db := initDb(3)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := stringToBytesWithPadding(fmt.Sprintf("value-%3d", i), valueLength)
		db.Put([]byte(fmt.Sprintf("key-%03d", i)), value, nil)
	}
}
func BenchmarkLevledb_Put(b *testing.B) {
	db, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(b, err)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := stringToBytesWithPadding(fmt.Sprintf("value-%3d", i), valueLength)
		db.Put([]byte(fmt.Sprintf("key-%03d", i)), value, nil)
	}
}
func BenchmarkShardingDb_Get(b *testing.B) {
	db := initDb(3)
	defer db.Close()
	for i := 0; i < 100000; i++ {
		value := stringToBytesWithPadding(fmt.Sprintf("value-%3d", i), valueLength)
		db.Put([]byte(fmt.Sprintf("key-%03d", i)), value, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key-%03d", i)), nil)
	}
}
func BenchmarkLeveldb_Get(b *testing.B) {
	db, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(b, err)
	defer db.Close()
	for i := 0; i < 100000; i++ {
		value := stringToBytesWithPadding(fmt.Sprintf("value-%3d", i), valueLength)
		db.Put([]byte(fmt.Sprintf("key-%03d", i)), value, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key-%03d", i)), nil)
	}
}

func TestShardingDbPerformance(t *testing.T) {
	pathList := []string{"/data/leveldb", "/data1/leveldb", getTempDir()}
	//remove all folder
	for _, path := range pathList {
		os.RemoveAll(path)
	}
	fmt.Printf("ShardingDb path[%v]", pathList)
	//Test shardingdb performance
	db, _ := OpenFile(pathList, nil)
	testDbPerformance(t, db, "shardingdb")
	db.Close()

	//Print every folder size
	for _, path := range pathList {
		size, _ := folderSize(path)
		fmt.Printf("Folder[%s] size:%d\n", path, size)
	}
}

func TestShardingDbReplicationPerformance(t *testing.T) {
	pathList := []string{"/data/leveldb", "/data/leveldb1", "/data1/leveldb", "/data1/leveldb1", getTempDir(), getTempDir()}
	var err error
	dbs := make([]LevelDbHandle, len(pathList))
	for i := 0; i < len(pathList); i++ {
		//remove all folder
		os.RemoveAll(pathList[i])
		dbs[i], err = leveldb.OpenFile(pathList[i], nil)
		if err != nil {
			//close all opened db
			for j := 0; j < i; j++ {
				dbs[j].Close()
			}
			return
		}
	}
	fmt.Printf("Replication[2] ShardingDb path[%v]", pathList)
	//Test shardingdb performance
	db, _ := NewShardingDb(WithDbHandles(dbs...), WithReplication(2))
	testDbPerformance(t, db, "Replication2ShardingDb")
	db.Close()

	//Print every folder size
	for _, path := range pathList {
		size, _ := folderSize(path)
		fmt.Printf("Folder[%s] size:%d\n", path, size)
	}
}

func TestLeveldbPerformance(t *testing.T) {
	dir := getTempDir()
	fmt.Printf("Leveldb path[%s]", dir)
	db, _ := leveldb.OpenFile(dir, nil)
	testDbPerformance(t, db, "leveldb")
	db.Close()
	//Print  folder size
	size, _ := folderSize(dir)
	fmt.Printf("Folder[%s] size:%d\n", dir, size)
}

func testDbPerformance(t *testing.T, db CommonDbHandle, testName string) {
	fmt.Printf("start db performance test,batch[%d] thread[%d] loop[%d],record count:%d\n",
		batchSize, thread, loop, batchSize*thread*loop)
	wg := sync.WaitGroup{}
	wg.Add(thread)
	start := time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				batch := new(leveldb.Batch)
				for k := 0; k < batchSize; k++ {
					value := stringToBytesWithPadding(fmt.Sprintf("value-%d", j*k), valueLength)
					batch.Put([]byte(fmt.Sprintf("key-%02d-%03d-%03d", thr, j, k)), value)
				}
				err := db.Write(batch, nil)
				assert.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("%s write batch[%d] thread[%d] loop[%d] cost:%v\n",
		testName, batchSize, thread, loop, time.Now().Sub(start))
	//Test Get performance
	wg = sync.WaitGroup{}
	wg.Add(thread)
	start = time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				for k := 0; k < batchSize; k++ {
					_, err := db.Get([]byte(fmt.Sprintf("key-%02d-%03d-%03d", thr, j, k)), nil)
					assert.NoError(t, err)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("%s get batch[%d] thread[%d] loop[%d] cost:%v\n",
		testName, batchSize, thread, loop, time.Now().Sub(start))
	//Test Get not found performance
	wg = sync.WaitGroup{}
	wg.Add(thread)
	start = time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				for k := 0; k < batchSize; k++ {
					v, _ := db.Get([]byte(fmt.Sprintf("key-%02d-%03d-x", thr, k)), nil)
					assert.Nil(t, v)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("%s get not found batch[%d] thread[%d] loop[%d] cost:%v\n",
		testName, batchSize, thread, loop, time.Now().Sub(start))
	//Test delete performance
	wg = sync.WaitGroup{}
	wg.Add(thread)
	start = time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				for k := 0; k < batchSize; k++ {
					if k%2 == 1 { //delete half of the data
						err := db.Delete([]byte(fmt.Sprintf("key-%02d-%03d-%03d", thr, j, k)), nil)
						assert.NoError(t, err)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("%s delete batch[%d] thread[%d] loop[%d] cost:%v\n",
		testName, batchSize, thread, loop, time.Now().Sub(start))

	//Test Iterator performance
	wg = sync.WaitGroup{}
	wg.Add(thread)
	start = time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				r := util.BytesPrefix([]byte(fmt.Sprintf("key-%02d-%03d-", thr, j)))
				iter := db.NewIterator(r, nil)
				count := 0
				for iter.Next() {
					count++
				}
				iter.Release()
				assert.Equal(t, batchSize/2, count) //half of the data is deleted
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("%s iterator batch[%d] thread[%d] loop[%d] cost:%v\n",
		testName, batchSize, thread, loop, time.Now().Sub(start))

}

func folderSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
