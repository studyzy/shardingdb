/*
 * Copyright [2023] [studyzy(studyzy@gmail.com)]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shardingdb

import "github.com/syndtr/goleveldb/leveldb"

// ShardingBatch is a batch of multiple db
type ShardingBatch struct {
	batchHandles []*leveldb.Batch
	length       uint16
	shardingFunc ShardingFunc
	encryptor    Encryptor
}

// NewShardingBatch returns a new ShardingBatch
func NewShardingBatch(len uint16, shardingFunc ShardingFunc, e Encryptor) *ShardingBatch {
	batches := make([]*leveldb.Batch, len)
	for i := uint16(0); i < len; i++ {
		batches[i] = new(leveldb.Batch)
	}
	return &ShardingBatch{
		batchHandles: batches,
		length:       len,
		shardingFunc: shardingFunc,
		encryptor:    e,
	}
}

// Put sets the value for the given key
func (s *ShardingBatch) Put(key, value []byte) {
	dbIndex := s.shardingFunc(key, s.length)
	if s.encryptor != nil && len(value) > 0 {
		evalue, err := s.encryptor.Encrypt(value)
		if err != nil {
			panic(err)
		}
		s.batchHandles[dbIndex].Put(key, evalue)
	} else {
		s.batchHandles[dbIndex].Put(key, value)
	}
}

// Delete deletes the value for the given key
func (s *ShardingBatch) Delete(key []byte) {
	dbIndex := s.shardingFunc(key, s.length)
	s.batchHandles[dbIndex].Delete(key)
}

// GetSplitBatch returns a map of db index to batch
func (s *ShardingBatch) GetSplitBatch() map[uint16]*leveldb.Batch {
	batches := make(map[uint16]*leveldb.Batch)
	for idx, dbHandle := range s.batchHandles {
		if dbHandle.Len() != 0 {
			batches[uint16(idx)] = dbHandle
		}
	}
	return batches
}

var _ leveldb.BatchReplay = (*ShardingBatch)(nil)
