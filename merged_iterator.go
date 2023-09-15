package goleveldb_sharding

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type MergedIterator struct {
	iterators []iterator.Iterator
}

func (m MergedIterator) First() bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Last() bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Seek(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Next() bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Prev() bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Release() {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) SetReleaser(releaser util.Releaser) {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Error() error {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (m MergedIterator) Value() []byte {
	//TODO implement me
	panic("implement me")
}

var _ iterator.Iterator = (*MergedIterator)(nil)
