package shardingdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDbHandle interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	GetSnapshot() (*leveldb.Snapshot, error)
	GetProperty(name string) (value string, err error)
	Stats(s *leveldb.DBStats) error
	SizeOf(ranges []util.Range) (leveldb.Sizes, error)
	Close() error
	OpenTransaction() (*leveldb.Transaction, error)
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
	Put(key, value []byte, wo *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	CompactRange(r util.Range) error
	SetReadOnly() error
}
