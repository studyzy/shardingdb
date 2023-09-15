package goleveldb_sharding

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ShardingSnapshot struct {
	dbHandles    []Snapshot
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
}

func (s ShardingSnapshot) String() string {
	//TODO implement me
	panic("implement me")
}

func (s ShardingSnapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (s ShardingSnapshot) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	//TODO implement me
	panic("implement me")
}

func (s ShardingSnapshot) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	//TODO implement me
	panic("implement me")
}

func (s ShardingSnapshot) Release() {
	//TODO implement me
	panic("implement me")
}

var _ Snapshot = (*ShardingSnapshot)(nil)
