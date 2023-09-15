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
	"errors"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// NewShardingDb creates a new ShardingDb
// @param shardingFunc
// @param txHandles
// @return *ShardingDb
func NewShardingDb(shardingFunc func(key []byte, max uint16) uint16, dbHandles ...LevelDbHandle) (*ShardingDb, error) {
	if len(dbHandles) == 0 {
		return nil, errors.New("txHandles is empty")
	}
	if len(dbHandles) > 65535 {
		return nil, errors.New("txHandles is too large")
	}
	return &ShardingDb{
		dbHandles:    dbHandles,
		length:       uint16(len(dbHandles)),
		shardingFunc: shardingFunc,
		lock:         new(sync.RWMutex),
	}, nil
}

// OpenFile opens multi db
// @param path
// @param o
// @return db
// @return err
func OpenFile(path []string, o *opt.Options) (db *ShardingDb, err error) {
	dbs := make([]LevelDbHandle, len(path))
	for i := 0; i < len(path); i++ {
		dbs[i], err = leveldb.OpenFile(path[i], o)
		if err != nil {
			//close all opened db
			for j := 0; j < i; j++ {
				dbs[j].Close()
			}
			return nil, err
		}
	}
	return NewShardingDb(MurmurSharding, dbs...)
}

// Migration changed leveldb count, reorganize all data to the new leveldb
// @param dbReaders
// @param sdb
// @return error
func Migration(dbReaders []LevelDbHandle, sdb *ShardingDb) error {
	sdb.lock.Lock()
	defer sdb.lock.Unlock()
	wg := sync.WaitGroup{}
	for i, dbHandle := range dbReaders {
		wg.Add(1)
		//concurrent resharding
		go func(index int, dbReader LevelDbHandle) {
			defer wg.Done()
			iter := dbReader.NewIterator(nil, nil)
			sdb.Infof("Resharding db[%d]", index)
			for iter.Next() {
				key := iter.Key()
				value := iter.Value()
				dbIndex := sdb.shardingFunc(key, sdb.length)
				//put data to new db
				sdb.Debugf("Move kv from db[%d] to db[%d]", index, dbIndex)
				if err := sdb.dbHandles[dbIndex].Put(key, value, nil); err != nil {
					iter.Release()
					panic(err)
				}
			}
			iter.Release()
			sdb.Infof("Resharding db[%d] finished", index)
		}(i, dbHandle)
	}
	wg.Wait()
	return nil
}
