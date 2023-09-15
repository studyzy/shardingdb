package goleveldb_sharding

import (
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ShardingDb struct {
	dbHandles    []LevelDbHandle
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
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
	}
}

func (sdb *ShardingDb) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Get(key, ro)
}

func (sdb *ShardingDb) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Has(key, ro)
}

func (sdb *ShardingDb) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iterators := make([]iterator.Iterator, 0)
	for _, dbHandle := range sdb.dbHandles {
		iterators = append(iterators, dbHandle.NewIterator(slice, ro))
	}
	return iterator.NewMergedIterator(iterators, comparer.DefaultComparer, true)

}

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

func (sdb *ShardingDb) Stats(s *leveldb.DBStats) error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Stats(s); err != nil {
			return err
		}
	}
	return nil
}

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

func (sdb *ShardingDb) Close() error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (sdb *ShardingDb) OpenTransaction() (Transaction, error) {
	allTx := make([]Transaction, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		tx, err := dbHandle.OpenTransaction()
		if err != nil {
			return nil, err
		}
		allTx[idx] = tx
	}
	return ShardingTransaction{dbHandles: allTx, length: sdb.length, shardingFunc: sdb.shardingFunc}, nil
}

func (sdb *ShardingDb) Write(batch Batch, wo *opt.WriteOptions) error {
	//Split batch into multiple batches
	batches, err := sdb.splitBatch(batch)
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

func (sdb *ShardingDb) splitBatch(batch Batch) (map[uint16]*leveldb.Batch, error) {
	shardingBath := NewShardingBatch(sdb.length, sdb.shardingFunc)
	err := batch.Replay(shardingBath)
	if err != nil {
		return nil, err
	}
	return shardingBath.GetSplitBatch(), nil
}

func (sdb *ShardingDb) Put(key, value []byte, wo *opt.WriteOptions) error {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Put(key, value, wo)
}

func (sdb *ShardingDb) Delete(key []byte, wo *opt.WriteOptions) error {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	return sdb.dbHandles[dbIndex].Delete(key, wo)
}

func (sdb *ShardingDb) CompactRange(r util.Range) error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.CompactRange(r); err != nil {
			return err
		}
	}
	return nil
}

func (sdb *ShardingDb) SetReadOnly() error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.SetReadOnly(); err != nil {
			return err
		}
	}
	return nil
}
