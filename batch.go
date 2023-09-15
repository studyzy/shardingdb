package goleveldb_sharding

import "github.com/syndtr/goleveldb/leveldb"

type ShardingBatch struct {
	dbHandles    []*leveldb.Batch
	length       uint16
	shardingFunc func(key []byte, max uint16) uint16
}

func NewShardingBatch(len uint16, shardingFunc func(key []byte, max uint16) uint16) *ShardingBatch {
	batches := make([]*leveldb.Batch, len)
	for i := uint16(0); i < len; i++ {
		batches[i] = new(leveldb.Batch)
	}
	return &ShardingBatch{
		dbHandles:    batches,
		length:       len,
		shardingFunc: shardingFunc,
	}
}

func (s *ShardingBatch) Put(key, value []byte) {
	dbIndex := s.shardingFunc(key, s.length)
	s.dbHandles[dbIndex].Put(key, value)
}

func (s *ShardingBatch) Delete(key []byte) {
	dbIndex := s.shardingFunc(key, s.length)
	s.dbHandles[dbIndex].Delete(key)
}
func (s *ShardingBatch) GetSplitBatch() map[uint16]*leveldb.Batch {
	batches := make(map[uint16]*leveldb.Batch)
	for idx, dbHandle := range s.dbHandles {
		if dbHandle.Len() != 0 {
			batches[uint16(idx)] = dbHandle
		}
	}
	return batches
}

var _ leveldb.BatchReplay = (*ShardingBatch)(nil)
