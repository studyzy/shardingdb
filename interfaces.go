package goleveldb_sharding

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DbHandle interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	GetSnapshot() (Snapshot, error)
	GetProperty(name string) (value string, err error)
	Stats(s *leveldb.DBStats) error
	SizeOf(ranges []util.Range) (leveldb.Sizes, error)
	Close() error
	OpenTransaction() (Transaction, error)
	Write(batch Batch, wo *opt.WriteOptions) error
	Put(key, value []byte, wo *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	CompactRange(r util.Range) error
	SetReadOnly() error
}

var _ DbHandle = (*ShardingDb)(nil)

type Transaction interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	Has(key []byte, ro *opt.ReadOptions) (bool, error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	Put(key, value []byte, wo *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	Write(b *leveldb.Batch, wo *opt.WriteOptions) error
	Commit() error
	Discard()
}
type Snapshot interface {
	String() string
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	Release()
}
type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Dump() []byte
	Load(data []byte) error
	Replay(r leveldb.BatchReplay) error
	Len() int
	Reset()
}
