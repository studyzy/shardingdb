package goleveldb_sharding

import (
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ DbHandle = (*ShardingDb)(nil)

type ShardingDb struct {
	dbHandles    []LevelDbHandle
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
	lock         *sync.RWMutex
}

// NewShardingDb creates a new ShardingDb
// @param shardingFunc
// @param dbHandles
// @return *ShardingDb
func NewShardingDb(shardingFunc func(key []byte, max uint16) uint16, dbHandles ...LevelDbHandle) *ShardingDb {
	if len(dbHandles) == 0 {
		panic("dbHandles is empty")
	}
	if len(dbHandles) > 65535 {
		panic("dbHandles is too large")
	}
	return &ShardingDb{
		dbHandles:    dbHandles,
		length:       uint16(len(dbHandles)),
		shardingFunc: shardingFunc,
		lock:         new(sync.RWMutex),
	}
}

// Get get value by key
// @param key
// @param ro
// @return value
// @return err
func (sdb *ShardingDb) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Get(key, ro)
}

// Has check if key exists
// @param key
// @param ro
// @return ret
// @return err
func (sdb *ShardingDb) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Has(key, ro)
}

// NewIterator create a new iterator
// @param slice
// @param ro
// @return iterator.Iterator
func (sdb *ShardingDb) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iterators := make([]iterator.Iterator, 0)
	for _, dbHandle := range sdb.dbHandles {
		iterators = append(iterators, dbHandle.NewIterator(slice, ro))
	}
	return iterator.NewMergedIterator(iterators, comparer.DefaultComparer, true)

}

// GetSnapshot get snapshot
// @return Snapshot
// @return error
func (sdb *ShardingDb) GetSnapshot() (Snapshot, error) {
	allSnapshots := make([]Snapshot, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		snapshot, err := dbHandle.GetSnapshot()
		if err != nil {
			return nil, err
		}
		allSnapshots[idx] = snapshot
	}
	return ShardingSnapshot{dbHandles: allSnapshots, length: sdb.length, shardingFunc: sdb.shardingFunc}, nil
}

// GetProperty get property
// @param name
// @return value
// @return err
func (sdb *ShardingDb) GetProperty(name string) (value string, err error) {
	result := []string{}
	for _, dbHandle := range sdb.dbHandles {
		v, err := dbHandle.GetProperty(name)
		if err != nil {
			return "", err
		}
		result = append(result, v)
	}
	return strings.Join(result, ","), nil
}

// Stats get stats
// @param s
// @return error
func (sdb *ShardingDb) Stats(s *leveldb.DBStats) error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Stats(s); err != nil {
			return err
		}
	}
	return nil
}

// SizeOf get size of ranges
// @param ranges
// @return leveldb.Sizes
// @return error
func (sdb *ShardingDb) SizeOf(ranges []util.Range) (leveldb.Sizes, error) {
	result := make(leveldb.Sizes, 0)
	for _, dbHandle := range sdb.dbHandles {
		sizes, err := dbHandle.SizeOf(ranges)
		if err != nil {
			return nil, err
		}
		result = append(result, sizes...)
	}
	return result, nil
}

// Close close all db
// @return error
func (sdb *ShardingDb) Close() error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Close(); err != nil {
			return err
		}
	}
	return nil
}

// OpenTransaction open transaction
// @return Transaction
// @return error
func (sdb *ShardingDb) OpenTransaction() (Transaction, error) {
	sdb.lock.Lock()
	allTx := make([]Transaction, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		tx, err := dbHandle.OpenTransaction()
		if err != nil {
			return nil, err
		}
		allTx[idx] = tx
	}
	return ShardingTransaction{dbHandles: allTx, length: sdb.length, shardingFunc: sdb.shardingFunc, lock: sdb.lock}, nil
}

// Write write batch
// @param batch
// @param wo
// @return error
func (sdb *ShardingDb) Write(batch Batch, wo *opt.WriteOptions) error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	//Split batch into multiple batches
	batches, err := splitBatch(batch, sdb.length, sdb.shardingFunc)
	if err != nil {
		return err
	}
	//Write batches to different dbHandles
	for idx, b := range batches {
		if err := sdb.dbHandles[idx].Write(b, wo); err != nil {
			return err
		}
	}
	return nil
}

func splitBatch(batch Batch, length uint16, shardingFunc func(key []byte, max uint16) uint16) (map[uint16]*leveldb.Batch, error) {
	shardingBath := NewShardingBatch(length, shardingFunc)
	err := batch.Replay(shardingBath)
	if err != nil {
		return nil, err
	}
	return shardingBath.GetSplitBatch(), nil
}

// Put put key value
// @param key
// @param value
// @param wo
// @return error
func (sdb *ShardingDb) Put(key, value []byte, wo *opt.WriteOptions) error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Put(key, value, wo)
}

// Delete delete key
// @param key
// @param wo
// @return error
func (sdb *ShardingDb) Delete(key []byte, wo *opt.WriteOptions) error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Delete(key, wo)
}

// CompactRange compact range
// @param r
// @return error
func (sdb *ShardingDb) CompactRange(r util.Range) error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.CompactRange(r); err != nil {
			return err
		}
	}
	return nil
}

// SetReadOnly set read only
// @return error
func (sdb *ShardingDb) SetReadOnly() error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.SetReadOnly(); err != nil {
			return err
		}
	}
	return nil
}

// Resharding changed leveldb count, reorganize all data
// @return error
func (sdb *ShardingDb) Resharding() error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	for i, dbHandle := range sdb.dbHandles {
		iter := dbHandle.NewIterator(nil, nil)

		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			dbIndex := sdb.shardingFunc(key, sdb.length)
			if dbIndex != uint16(i) {
				if err := sdb.dbHandles[dbIndex].Put(key, value, nil); err != nil {
					iter.Release()
					return err
				}
				if err := dbHandle.Delete(key, nil); err != nil {
					iter.Release()
					return err
				}
			}
		}
		iter.Release()
	}
	return nil
}
