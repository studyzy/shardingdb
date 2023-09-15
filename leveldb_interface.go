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
