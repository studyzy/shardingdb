package goleveldb_sharding

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

func TestPutGet(t *testing.T) {
	//create a new leveldb on temp dir
	db1, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	db2, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	// Create a new sharding db
	db := NewShardingDb(Sha256Sharding, db1, db2)
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

func TestBatchWriteAndIterator(t *testing.T) {
	//create a new leveldb on temp dir
	db1, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	db2, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	db3, err := leveldb.OpenFile(getTempDir(), nil)
	assert.NoError(t, err)
	// Create a new sharding db
	db := NewShardingDb(MurmurSharding, db1, db2, db3)
	defer db.Close()
	batch := new(leveldb.Batch)
	for i := 0; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i)))
	}
	err = db.Write(batch, nil)
	assert.NoError(t, err)
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
}
