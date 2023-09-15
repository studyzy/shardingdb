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
	for i, dbHandle := range dbReaders {
		iter := dbHandle.NewIterator(nil, nil)

		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			dbIndex := sdb.shardingFunc(key, sdb.length)
			if dbIndex != uint16(i) {
				if err := sdb.dbHandles[dbIndex].Put(key, value, nil); err != nil {
					iter.Release()
					return err
				}
			}
		}
		iter.Release()
	}
	return nil
}
