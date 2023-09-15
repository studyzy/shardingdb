package shardingdb

import (
	"strings"

	"github.com/syndtr/goleveldb/leveldb/comparer"
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
	var result []string
	for _, dbHandle := range s.dbHandles {
		result = append(result, dbHandle.String())
	}
	return strings.Join(result, ",")
}

func (s ShardingSnapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Get(key, ro)
}

func (s ShardingSnapshot) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Has(key, ro)
}

func (s ShardingSnapshot) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iters := make([]iterator.Iterator, s.length)
	for idx, dbHandle := range s.dbHandles {
		iters[idx] = dbHandle.NewIterator(slice, ro)
	}
	return iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
}

func (s ShardingSnapshot) Release() {
	for _, dbHandle := range s.dbHandles {
		dbHandle.Release()
	}
}

var _ Snapshot = (*ShardingSnapshot)(nil)
