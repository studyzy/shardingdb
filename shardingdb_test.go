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
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func getTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("shardingdb_%d", time.Now().UnixNano()))
}

func initDb(n int) *ShardingDb {
	dbHandles := make([]LevelDbHandle, n)
	for i := 0; i < n; i++ {
		db, err := leveldb.OpenFile(getTempDir(), nil)
		if err != nil {
			panic(err)
		}
		dbHandles[i] = db
	}

	db, err := NewShardingDb(
		WithDbHandles(dbHandles...),
		WithEncryptor(NewAESCryptor([]byte("1234567890123456"))),
	)
	if err != nil {
		panic(err)
	}
	return db
}

func TestPutGet(t *testing.T) {
	//create a new leveldb on temp dir
	db1, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	db2, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	// Create a new sharding db
	db, err := NewShardingDb(WithDbHandles(db1, db2), WithShardingFunc(Sha256Sharding))
	assert.NoError(t, err)
	defer db.Close()
	// Put a key-value pair
	err = db.Put([]byte("key"), []byte("value"), nil)
	assert.NoError(t, err)
	// Get the value by key
	value, err := db.Get([]byte("key"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "value", string(value))
	exist, err := db.Has([]byte("key100"), nil)
	assert.NoError(t, err)
	assert.False(t, exist)
	noValue, err := db.Get([]byte("key100"), nil)
	assert.Error(t, err)
	assert.Nil(t, noValue)
}

func getCount(db DbHandle, print bool) int {
	iter := db.NewIterator(nil, nil)
	count := 0
	for iter.Next() {
		count++
		if print {
			fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		}
	}
	return count
}

func TestBatchWriteAndIterator(t *testing.T) {
	// Create a new sharding db
	db := initDb(3)
	defer db.Close()
	batch := new(leveldb.Batch)
	for i := 0; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err := db.Write(batch, nil)
	assert.NoError(t, err)
	assert.Equal(t, 100, getCount(db, false))
	iter := db.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count := 0
	//print iterator result
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 10, count)
	ranges := []util.Range{
		util.Range{Start: []byte("a"), Limit: []byte("d")},
		util.Range{Start: []byte("x"), Limit: []byte("z")},
	}
	size, err := db.SizeOf(ranges)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(size))
	//test delete
	err = db.Delete([]byte("key-021"), nil)
	assert.NoError(t, err)
	iter = db.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count = 0
	//print iterator result
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 9, count)
}
func TestShardingDb_Resharding(t *testing.T) {
	db1, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	batch := new(leveldb.Batch)
	for i := 0; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err = db1.Write(batch, nil)
	assert.NoError(t, err)
	db2, _ := leveldb.OpenFile(getTempDir(), nil)
	db3, _ := leveldb.OpenFile(getTempDir(), nil)
	db, err := NewShardingDb(WithDbHandles(db1, db2, db3))
	assert.NoError(t, err)
	defer db.Close()
	count := 0
	for i := 0; i < 10; i++ {
		value, err := db.Get([]byte(fmt.Sprintf("key-%03d", i)), nil)
		if err == nil {
			count++
			fmt.Printf("key=%s, value=%s\n", fmt.Sprintf("key-%03d", i), value)
		}
	}
	assert.NotEqual(t, 10, count)
	err = db.Resharding()
	assert.NoError(t, err)
	count = 0
	for i := 0; i < 10; i++ {
		value, err := db.Get([]byte(fmt.Sprintf("key-%03d", i)), nil)
		if err == nil {
			count++
			fmt.Printf("key=%s, value=%s\n", fmt.Sprintf("key-%03d", i), value)
		}
	}
	assert.Equal(t, 10, count)
}

func TestShardingDb_Transaction(t *testing.T) {
	db := initDb(2)
	defer db.Close()
	//create a transaction
	tx1, err := db.OpenTransaction()
	assert.NoError(t, err)
	batch := new(leveldb.Batch)
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			tx1.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)), nil)
		} else {
			batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
		}
	}
	//get tx iterator
	iter := tx1.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count := 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 5, count)
	err = tx1.Write(batch, nil)
	assert.NoError(t, err)
	//get tx iterator after write batch
	iter = tx1.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count = 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 10, count)
	//query from db before commit tx
	iter = db.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count = 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 0, count)
	err = tx1.Commit()
	assert.NoError(t, err)
	//query from db after commit tx
	iter = db.NewIterator(util.BytesPrefix([]byte("key-02")), nil)
	count = 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 10, count)
}
func TestShardingDb_Snapshot(t *testing.T) {
	db := initDb(2)
	defer db.Close()

	batch := new(leveldb.Batch)
	for i := 0; i < 50; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err := db.Write(batch, nil)
	assert.NoError(t, err)
	snapshot, err := db.GetSnapshot()
	assert.NoError(t, err)
	batch = new(leveldb.Batch)
	for i := 50; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err = db.Write(batch, nil)
	assert.NoError(t, err)
	//get snapshot iterator
	iter := snapshot.NewIterator(util.BytesPrefix([]byte("key-")), nil)
	count := 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 50, count)
	//get db iterator
	iter = db.NewIterator(util.BytesPrefix([]byte("key-")), nil)
	count = 0
	for iter.Next() {
		count++
	}
	assert.Equal(t, 100, count)

}
func TestShardingDb_Iterator(t *testing.T) {
	db := initDb(2)
	defer db.Close()

	batch := new(leveldb.Batch)
	for i := 0; i < 10; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err := db.Write(batch, nil)
	assert.NoError(t, err)

	iter := db.NewIterator(util.BytesPrefix([]byte("key-")), nil)

	batch = new(leveldb.Batch)
	for i := 10; i < 20; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err = db.Write(batch, nil)
	assert.NoError(t, err)
	//get snapshot iterator

	count := 0
	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
		count++
	}
	assert.Equal(t, 10, count)
	//get db iterator
	iter = db.NewIterator(util.BytesPrefix([]byte("key-")), nil)
	count = 0
	for iter.Next() {
		count++
	}
	assert.Equal(t, 20, count)

}
