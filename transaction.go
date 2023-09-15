package goleveldb_sharding

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ShardingTransaction struct {
	dbHandles    []Transaction
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
}

func (s ShardingTransaction) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Get(key, ro)
}

func (s ShardingTransaction) Has(key []byte, ro *opt.ReadOptions) (bool, error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Has(key, ro)
}

func (s ShardingTransaction) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iters := make([]iterator.Iterator, s.length)
	for idx, dbHandle := range s.dbHandles {
		iters[idx] = dbHandle.NewIterator(slice, ro)
	}
	return iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
}

func (s ShardingTransaction) Put(key, value []byte, wo *opt.WriteOptions) error {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Put(key, value, wo)
}

func (s ShardingTransaction) Delete(key []byte, wo *opt.WriteOptions) error {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Delete(key, wo)
}

func (s ShardingTransaction) Write(b *leveldb.Batch, wo *opt.WriteOptions) error {
	//Split batch into multiple batches
	batches, err := splitBatch(b, s.length, s.shardingFunc)
	if err != nil {
		return err
	}
	//Write batches to different dbHandles
	for idx, b := range batches {
		if err := s.dbHandles[idx].Write(b, wo); err != nil {
			return err
		}
	}
	return nil
}

func (s ShardingTransaction) Commit() error {
	for _, dbHandle := range s.dbHandles {
		err := dbHandle.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s ShardingTransaction) Discard() {
	for _, dbHandle := range s.dbHandles {
		dbHandle.Discard()
	}
}

var _ Transaction = (*ShardingTransaction)(nil)
