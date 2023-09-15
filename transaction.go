package shardingdb

import (
	"errors"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ShardingTransaction struct {
	txHandles    []Transaction
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
	lock         *sync.RWMutex
}

func (s ShardingTransaction) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.txHandles[dbIndex].Get(key, ro)
}

func (s ShardingTransaction) Has(key []byte, ro *opt.ReadOptions) (bool, error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.txHandles[dbIndex].Has(key, ro)
}

func (s ShardingTransaction) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iters := make([]iterator.Iterator, s.length)
	for idx, dbHandle := range s.txHandles {
		iters[idx] = dbHandle.NewIterator(slice, ro)
	}
	return iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
}

func (s ShardingTransaction) Put(key, value []byte, wo *opt.WriteOptions) error {
	dbIndex := s.shardingFunc(key, s.length)
	return s.txHandles[dbIndex].Put(key, value, wo)
}

func (s ShardingTransaction) Delete(key []byte, wo *opt.WriteOptions) error {
	dbIndex := s.shardingFunc(key, s.length)
	return s.txHandles[dbIndex].Delete(key, wo)
}

func (s ShardingTransaction) Write(b *leveldb.Batch, wo *opt.WriteOptions) error {
	//Split batch into multiple batches
	batches, err := splitBatch(b, s.length, s.shardingFunc)
	if err != nil {
		return err
	}
	//Write batches to different txHandles
	for idx, b := range batches {
		if err := s.txHandles[idx].Write(b, wo); err != nil {
			return err
		}
	}
	return nil
}

func (s ShardingTransaction) Commit() error {
	defer s.lock.Unlock()
	if len(s.txHandles) == 0 {
		return errors.New("no transaction to commit")
	}
	for _, dbHandle := range s.txHandles {
		err := dbHandle.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s ShardingTransaction) Discard() {
	defer s.lock.Unlock()
	for _, dbHandle := range s.txHandles {
		dbHandle.Discard()
	}
}

var _ Transaction = (*ShardingTransaction)(nil)
