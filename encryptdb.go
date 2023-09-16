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
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (sdb *ShardingDb) get(dbHandle LevelDbHandle, key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	val, err := dbHandle.Get(key, ro)
	if err != nil {
		return nil, err
	}
	if sdb.encryptor != nil && len(val) > 0 {
		val, err = sdb.encryptor.Decrypt(val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}
func (sdb *ShardingDb) put(dbHandle LevelDbHandle, key, value []byte, wo *opt.WriteOptions) error {
	if sdb.encryptor != nil && len(value) > 0 {
		evalue, err := sdb.encryptor.Encrypt(value)
		if err != nil {
			return err
		}
		return dbHandle.Put(key, evalue, wo)
	}
	return dbHandle.Put(key, value, wo)
}

type encryptIterator struct {
	iter      iterator.Iterator
	encryptor Encryptor
}

func (e encryptIterator) First() bool {
	return e.iter.First()
}

func (e encryptIterator) Last() bool {
	return e.iter.Last()
}

func (e encryptIterator) Seek(key []byte) bool {
	return e.iter.Seek(key)
}

func (e encryptIterator) Next() bool {
	return e.iter.Next()
}

func (e encryptIterator) Prev() bool {
	return e.iter.Prev()
}

func (e encryptIterator) Release() {
	e.iter.Release()
}

func (e encryptIterator) SetReleaser(releaser util.Releaser) {
	e.iter.SetReleaser(releaser)
}

func (e encryptIterator) Valid() bool {
	return e.iter.Valid()
}

func (e encryptIterator) Error() error {
	return e.iter.Error()
}

func (e encryptIterator) Key() []byte {
	return e.iter.Key()
}

func (e encryptIterator) Value() []byte {
	val := e.iter.Value()
	var err error
	if e.encryptor != nil && len(val) > 0 {
		val, err = e.encryptor.Decrypt(val)
		if err != nil {
			return nil
		}

	}
	return val
}

var _ iterator.Iterator = (*encryptIterator)(nil)
