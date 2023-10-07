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

import (
	"strings"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ShardingSnapshot is a snapshot of multiple db
type ShardingSnapshot struct {
	dbHandles    []Snapshot
	length       uint16
	shardingFunc ShardingFunc
	encryptor    Encryptor
}

// String returns a string representation of the snapshot
func (s ShardingSnapshot) String() string {
	var result []string
	for _, dbHandle := range s.dbHandles {
		result = append(result, dbHandle.String())
	}
	return strings.Join(result, ",")
}

// Get returns the value for the given key
func (s ShardingSnapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	dbIndex := s.shardingFunc(key, s.length)
	val, err := s.dbHandles[dbIndex].Get(key, ro)
	if err != nil {
		return nil, err
	}
	if s.encryptor != nil && len(val) > 0 {
		val, err = s.encryptor.Decrypt(val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

// Has returns whether the DB does contains the given key
func (s ShardingSnapshot) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	dbIndex := s.shardingFunc(key, s.length)
	return s.dbHandles[dbIndex].Has(key, ro)
}

// NewIterator returns an iterator for the latest snapshot of the DB
func (s ShardingSnapshot) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iters := make([]iterator.Iterator, s.length)
	for idx, dbHandle := range s.dbHandles {
		iters[idx] = dbHandle.NewIterator(slice, ro)
	}
	miter := iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
	if s.encryptor != nil {
		return encryptIterator{iter: miter, encryptor: s.encryptor}
	}
	return miter
}

// Release releases the snapshot
func (s ShardingSnapshot) Release() {
	for _, dbHandle := range s.dbHandles {
		dbHandle.Release()
	}
}

var _ Snapshot = (*ShardingSnapshot)(nil)
