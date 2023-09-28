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

	db, _ := OpenFile(pathList, nil)
	fmt.Printf("start shardingdb performance test,batch[%d] thread[%d] loop[%d],sharding[%d]，record count:%d\n",
		batchSize, thread, loop, db.length, batchSize*thread*loop)
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
	fmt.Printf("ShardingDb write batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
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
	fmt.Printf("ShardingDb get batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
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
	fmt.Printf("ShardingDb get not found batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
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
	fmt.Printf("ShardingDb delete batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))

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
				assert.Equal(t, batchSize/2, count)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("ShardingDb iterator batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
	db.Close()
	//Print every folder size
	for _, path := range pathList {
		size, _ := folderSize(path)
		fmt.Printf("Folder[%s] size:%d\n", path, size)
	}
}
func TestLeveldbPerformance(t *testing.T) {
	dir := getTempDir()
	fmt.Printf("start leveldb performance test,batch[%d] thread[%d] loop[%d],Path[%s]，record count:%d\n",
		batchSize, thread, loop, dir, batchSize*thread*loop)
	db, _ := leveldb.OpenFile(dir, nil)
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
	fmt.Printf("Leveldb write batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
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
	fmt.Printf("Leveldb get batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
	//Test Get not found performance
	wg = sync.WaitGroup{}
	wg.Add(thread)
	start = time.Now()
	for i := 0; i < thread; i++ {
		go func(thr int) {
			defer wg.Done()
			for j := 0; j < loop; j++ {
				for k := 0; k < batchSize; k++ {
					v, _ := db.Get([]byte(fmt.Sprintf("key-%02d-%03d-x", thr, j)), nil)
					assert.Nil(t, v)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Leveldb get not found batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
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
				assert.Equal(t, batchSize/2, count)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Leveldb iterator batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
	db.Close()
	//Print  folder size
	size, _ := folderSize(dir)
	fmt.Printf("Folder[%s] size:%d\n", dir, size)
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
