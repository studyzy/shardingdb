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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	thread      = 500
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

// const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
//
//	func randomString(seed int64, length int) string {
//		rand.Seed(seed)
//		result := make([]byte, length)
//		for i := range result {
//			result[i] = charset[rand.Intn(len(charset))]
//		}
//		return string(result)
//	}

func TestShardingDbPerformance(t *testing.T) {
	os.RemoveAll("/data/leveldb")
	os.RemoveAll("/data1/leveldb")

	db, _ := OpenFile([]string{"/data/leveldb", "/data1/leveldb", getTempDir()}, nil)
	defer db.Close()
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
					v, _ := db.Get([]byte(fmt.Sprintf("key-%02d-%03d", thr, k+batchSize)), nil)
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
			r := util.BytesPrefix([]byte(fmt.Sprintf("key-%02d-", thr)))
			iter := db.NewIterator(r, nil)
			count := 0
			for iter.Next() {
				count++
			}
			iter.Release()
			assert.Equal(t, batchSize/2, count)
		}(i)
	}
	wg.Wait()
	fmt.Printf("ShardingDb iterator batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
}
func TestLeveldbPerformance(t *testing.T) {
	dir := getTempDir()
	fmt.Println(dir)
	db, _ := leveldb.OpenFile(dir, nil)
	defer db.Close()
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
					v, _ := db.Get([]byte(fmt.Sprintf("key-%02d-%03d", thr, k+batchSize)), nil)
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
			r := util.BytesPrefix([]byte(fmt.Sprintf("key-%02d-", thr)))
			iter := db.NewIterator(r, nil)
			count := 0
			for iter.Next() {
				count++
			}
			iter.Release()
			assert.Equal(t, batchSize, count)
		}(i)
	}
	wg.Wait()
	fmt.Printf("Leveldb iterator batch[%d] thread[%d] loop[%d] cost:%v\n", batchSize, thread, loop, time.Now().Sub(start))
}