package goleveldb_sharding

import (
	"github.com/syndtr/goleveldb/leveldb"
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
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Has(key []byte, ro *opt.ReadOptions) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Put(key, value []byte, wo *opt.WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Delete(key []byte, wo *opt.WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Write(b *leveldb.Batch, wo *opt.WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Commit() error {
	//TODO implement me
	panic("implement me")
}

func (s ShardingTransaction) Discard() {
	//TODO implement me
	panic("implement me")
}

var _ Transaction = (*ShardingTransaction)(nil)
