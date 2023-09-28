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

type DbOption func(db *ShardingDb)

func WithDbHandles(dbHandles ...LevelDbHandle) DbOption {
	return func(s *ShardingDb) {
		s.dbHandles = dbHandles
		s.length = uint16(len(dbHandles))
	}
}

func WithDbPaths(paths ...string) DbOption {
	dbHandles := make([]LevelDbHandle, len(paths))
	for i := 0; i < len(paths); i++ {
		db, err := leveldb.OpenFile(paths[i], nil)
		if err != nil {
			panic(err)
		}
		dbHandles[i] = db
	}
	return func(s *ShardingDb) {
		s.dbHandles = dbHandles
		s.length = uint16(len(dbHandles))
	}
}
func WithShardingFunc(f func(key []byte, max uint16) uint16) DbOption {
	return func(s *ShardingDb) {
		s.shardingFunc = f
	}
}

func WithLogger(l Logger) DbOption {
	return func(s *ShardingDb) {
		s.logger = l
	}
}

func WithEncryptor(e Encryptor) DbOption {
	return func(s *ShardingDb) {
		s.encryptor = e
	}
}

func WithReplication(count uint16) DbOption {
	return func(s *ShardingDb) {
		s.replication = count
	}
}