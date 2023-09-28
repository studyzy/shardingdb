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
	"fmt"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ ShardingDbHandle = (*ShardingDb)(nil)

type ShardingDb struct {
	dbHandles    []LevelDbHandle
	length       uint16
	shardingFunc ShardingFunc
	//lock         sync.RWMutex
	logger      Logger
	encryptor   Encryptor
	replication uint16
}

func (sdb *ShardingDb) ShardCount() uint16 {
	return sdb.length
}

// Get get value by key
// @param key
// @param ro
// @return value
// @return err
func (sdb *ShardingDb) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)
	// 1 data replication, no need to merge
	if sdb.replication <= 1 {
		return sdb.get(sdb.dbHandles[dbIndex], key, ro)
	}

	// 2 or more data replication, merge data
	resultChan := make(chan []byte, sdb.replication)
	errChan := make(chan error, sdb.replication)

	// get data from different db
	for i := uint16(0); i < sdb.replication; i++ {
		go func(index uint16) {
			res, err := sdb.get(sdb.dbHandles[(dbIndex+index)%sdb.length], key, ro)
			if err != nil {
				errChan <- err
			} else {
				resultChan <- res
			}
		}(i)
	}

	// merge data
	successCount := uint16(0)
	errorCount := uint16(0)
	for {
		select {
		case res := <-resultChan:
			successCount++
			if successCount == 1 {
				value = res
				return
			}
		case err = <-errChan:
			errorCount++
			if errorCount == sdb.replication {
				return
			}
		}
	}
}

// Has checks if the given key exists in the database. If there are multiple replicas,
// it checks for the key in all replicas concurrently and returns true if any replica
// contains the key.
// @param key
// @param ro
// @return ret
// @return err
func (sdb *ShardingDb) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	dbIndex := sdb.shardingFunc(key, sdb.length)

	if sdb.replication <= 1 {
		return sdb.dbHandles[dbIndex].Has(key, ro)
	}

	// Create channels to receive results and errors
	resultChan := make(chan bool, sdb.replication)
	errChan := make(chan error, sdb.replication)

	// Concurrently check for the key in all replicas
	for i := uint16(0); i < sdb.replication; i++ {
		go func(index uint16) {
			has, err := sdb.dbHandles[(dbIndex+index)%sdb.length].Has(key, ro)
			if err != nil {
				errChan <- err
			} else {
				resultChan <- has
			}
		}(i)
	}

	// Wait for the first successful result or errors from all replicas
	errorCount := uint16(0)
	for {
		select {
		case has := <-resultChan:
			ret = has
			return
		case err = <-errChan:
			errorCount++
			if errorCount == sdb.replication {
				return
			}
		}
	}
}

// NewIterator create a new iterator
// @param slice
// @param ro
// @return iterator.Iterator
func (sdb *ShardingDb) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iterators := make([]iterator.Iterator, 0)
	for _, dbHandle := range sdb.dbHandles {
		iterators = append(iterators, dbHandle.NewIterator(slice, ro))
	}

	miter := NewMergedIterator(iterators, comparer.DefaultComparer, true, sdb.shardingFunc, sdb.length, sdb.replication)
	if sdb.encryptor != nil {
		return &encryptIterator{iter: miter, encryptor: sdb.encryptor}
	}
	return miter

}

// GetSnapshot get snapshot
// @return Snapshot
// @return error
func (sdb *ShardingDb) GetSnapshot() (Snapshot, error) {
	allSnapshots := make([]Snapshot, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		snapshot, err := dbHandle.GetSnapshot()
		if err != nil {
			return nil, err
		}
		allSnapshots[idx] = snapshot
	}
	return ShardingSnapshot{
		dbHandles:    allSnapshots,
		length:       sdb.length,
		shardingFunc: sdb.shardingFunc,
		encryptor:    sdb.encryptor,
	}, nil
}

// GetProperty get property
// @param name
// @return value
// @return err
func (sdb *ShardingDb) GetProperty(name string) (value string, err error) {
	result := []string{}
	for _, dbHandle := range sdb.dbHandles {
		v, err := dbHandle.GetProperty(name)
		if err != nil {
			return "", err
		}
		result = append(result, v)
	}
	return strings.Join(result, ","), nil
}

// Stats get stats
// @param s
// @return error
func (sdb *ShardingDb) Stats(s *leveldb.DBStats) error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Stats(s); err != nil {
			return err
		}
	}
	return nil
}

// SizeOf get size of ranges
// @param ranges
// @return leveldb.Sizes
// @return error
func (sdb *ShardingDb) SizeOf(ranges []util.Range) (leveldb.Sizes, error) {
	result := make(leveldb.Sizes, len(ranges))
	for _, dbHandle := range sdb.dbHandles {
		sizes, err := dbHandle.SizeOf(ranges)
		if err != nil {
			return nil, err
		}
		for idx, size := range sizes {
			result[idx] += size
		}
	}
	return result, nil
}

// Close close all db
// @return error
func (sdb *ShardingDb) Close() error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.Close(); err != nil {
			return err
		}
	}
	return nil
}

// OpenTransaction open transaction
// @return Transaction
// @return error
func (sdb *ShardingDb) OpenTransaction() (Transaction, error) {
	//sdb.lock.Lock()
	allTx := make([]Transaction, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		tx, err := dbHandle.OpenTransaction()
		if err != nil {
			return nil, err
		}
		allTx[idx] = tx
	}
	return ShardingTransaction{
		txHandles:    allTx,
		length:       sdb.length,
		shardingFunc: sdb.shardingFunc,
		//lock: &sdb.lock,
	}, nil
}

// Write write batch
// @param batch
// @param wo
// @return error
// Write applies the given batch to the database. If there are multiple replicas,
// it applies the batch to all replicas concurrently and waits for all of them to complete.
func (sdb *ShardingDb) Write(batch *leveldb.Batch, wo *opt.WriteOptions) error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()

	// Split batch into multiple batches
	batches, err := splitBatch(batch, sdb.length, sdb.shardingFunc, sdb.encryptor)
	if err != nil {
		return err
	}

	if sdb.replication <= 1 {
		// Write batches to different txHandles
		for idx, b := range batches {
			if err := sdb.dbHandles[idx].Write(b, wo); err != nil {
				return err
			}
		}
		return nil
	}

	// Create a channel to receive errors
	errChan := make(chan error, sdb.replication*sdb.length)

	// Concurrently apply the batch to all replicas
	var wg sync.WaitGroup

	for batchIdx, b := range batches {
		for repI := uint16(0); repI < sdb.replication; repI++ {
			wg.Add(1)
			go func(index uint16, batch *leveldb.Batch) {
				defer wg.Done()
				err := sdb.dbHandles[index].Write(batch, wo)
				errChan <- err
			}((batchIdx+repI)%sdb.length, b)
		}
	}

	// Wait for all replicas to complete
	wg.Wait()
	close(errChan)

	// Check for errors from the replicas
	errors := make([]error, 0)
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// If all replicas return errors, return an error
	if len(errors) == int(sdb.replication*sdb.length) {
		return fmt.Errorf("Write operation failed on all replicas since: %v", errors)
	}

	return nil
}

func splitBatch(batch Batch, length uint16, shardingFunc ShardingFunc, e Encryptor) (map[uint16]*leveldb.Batch, error) {
	shardingBath := NewShardingBatch(length, shardingFunc, e)
	err := batch.Replay(shardingBath)
	if err != nil {
		return nil, err
	}
	return shardingBath.GetSplitBatch(), nil
}

// Put put key value
// @param key
// @param value
// @param wo
// @return error
// Put writes the given key-value pair to the database. If there are multiple replicas,
// it writes the key-value pair to all replicas concurrently and waits for all of them to complete.
func (sdb *ShardingDb) Put(key, value []byte, wo *opt.WriteOptions) error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()
	dbIndex := sdb.shardingFunc(key, sdb.length)

	if sdb.replication <= 1 {
		return sdb.put(sdb.dbHandles[dbIndex], key, value, wo)
	}

	// Create a channel to receive errors
	errChan := make(chan error, sdb.replication)

	// Concurrently write the key-value pair to all replicas
	var wg sync.WaitGroup
	for i := uint16(0); i < sdb.replication; i++ {
		wg.Add(1)
		go func(index uint16) {
			defer wg.Done()
			err := sdb.put(sdb.dbHandles[(dbIndex+index)%sdb.length], key, value, wo)
			errChan <- err
		}(i)
	}

	// Wait for all replicas to complete
	wg.Wait()
	close(errChan)

	// Check for errors from the replicas
	errors := make([]error, 0)
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// If all replicas return errors, return an error
	if len(errors) == int(sdb.replication) {
		return fmt.Errorf("Put operation failed on all replicas since: %v", errors)
	}

	return nil
}

// Delete delete key
// @param key
// @param wo
// @return error
// Delete removes the given key from the database. If there are multiple replicas,
// it removes the key from all replicas concurrently and waits for all of them to complete.
func (sdb *ShardingDb) Delete(key []byte, wo *opt.WriteOptions) error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()
	dbIndex := sdb.shardingFunc(key, sdb.length)

	if sdb.replication <= 1 {
		return sdb.dbHandles[dbIndex].Delete(key, wo)
	}

	// Create a channel to receive errors
	errChan := make(chan error, sdb.replication)

	// Concurrently delete the key from all replicas
	var wg sync.WaitGroup
	for i := uint16(0); i < sdb.replication; i++ {
		wg.Add(1)
		go func(index uint16) {
			defer wg.Done()
			err := sdb.dbHandles[(dbIndex+index)%sdb.length].Delete(key, wo)
			errChan <- err
		}(i)
	}

	// Wait for all replicas to complete
	wg.Wait()
	close(errChan)

	// Check for errors from the replicas
	errors := make([]error, 0)
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// If all replicas return errors, return an error
	if len(errors) == int(sdb.replication) {
		return fmt.Errorf("Delete operation failed on all replicas since: %v", errors)
	}

	return nil
}

// CompactRange compact range
// @param r
// @return error
func (sdb *ShardingDb) CompactRange(r util.Range) error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.CompactRange(r); err != nil {
			return err
		}
	}
	return nil
}

// SetReadOnly set read only
// @return error
func (sdb *ShardingDb) SetReadOnly() error {
	for _, dbHandle := range sdb.dbHandles {
		if err := dbHandle.SetReadOnly(); err != nil {
			return err
		}
	}
	return nil
}

// Debugf log debug
// @param msg
// @param a
func (sdb *ShardingDb) Debugf(msg string, a ...interface{}) {
	if sdb.logger != nil {
		sdb.logger.Debug(fmt.Sprintf(msg, a...))
	}
}

// Infof log info
// @param msg
// @param a
func (sdb *ShardingDb) Infof(msg string, a ...interface{}) {
	if sdb.logger != nil {
		sdb.logger.Info(fmt.Sprintf(msg, a...))
	}
}

// Resharding changed leveldb count, reorganize all data in the original leveldb
// @return error
func (sdb *ShardingDb) Resharding() error {
	//sdb.lock.Lock()
	//defer sdb.lock.Unlock()
	if sdb.replication > 1 {
		return sdb.reshardingWithReplication()
	}
	return sdb.reshardingWithoutReplication()

}
func (sdb *ShardingDb) reshardingWithoutReplication() error {
	//get all snapshots
	snapshots := make([]Snapshot, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		snapshot, err := dbHandle.GetSnapshot()
		if err != nil {
			return err
		}
		snapshots[idx] = snapshot
	}
	wg := sync.WaitGroup{}
	for x, snapshot := range snapshots {
		wg.Add(1)
		//concurrent resharding
		go func(index int, dbReader Snapshot) {
			defer wg.Done()
			iter := dbReader.NewIterator(nil, nil)
			sdb.Infof("Resharding db[%d]", index)
			for iter.Next() {
				key := iter.Key()
				value := iter.Value()
				dbIndex := sdb.shardingFunc(key, sdb.length)
				if dbIndex != uint16(index) {
					sdb.Debugf("Move kv from db[%d] to db[%d]", index, dbIndex)
					if err := sdb.dbHandles[dbIndex].Put(key, value, nil); err != nil {
						iter.Release()
						panic(err)
					}
					//delete data from old db
					if err := sdb.dbHandles[index].Delete(key, nil); err != nil {
						iter.Release()
						panic(err)
					}
				}
			}
			sdb.Infof("Resharding db[%d] finished", index)
			iter.Release()
			dbReader.Release()
		}(x, snapshot)
	}
	wg.Wait()
	return nil
}

// reshardingWithReplication changed leveldb count or replication number, reorganize all data in the original leveldb
// resharding will duplicate data to new db, and delete data from old db
// @return error
func (sdb *ShardingDb) reshardingWithReplication() error {
	// Get all snapshots
	snapshots := make([]Snapshot, sdb.length)
	for idx, dbHandle := range sdb.dbHandles {
		snapshot, err := dbHandle.GetSnapshot()
		if err != nil {
			return err
		}
		snapshots[idx] = snapshot
	}

	wg := sync.WaitGroup{}
	for x, snapshot := range snapshots {
		wg.Add(1)

		// Concurrent resharding
		go func(index int, dbReader Snapshot) {
			defer wg.Done()
			iter := dbReader.NewIterator(nil, nil)
			sdb.Infof("Resharding db[%d]", index)

			for iter.Next() {
				key := iter.Key()
				value := iter.Value()
				dbIndex := sdb.shardingFunc(key, sdb.length)

				sdb.Debugf("Move kv from db[%d] to db[%d]", index, dbIndex)
				// Duplicate write data to new db
				for i := uint16(0); i < sdb.replication; i++ {
					wg.Add(1)
					go func(idx uint16) {
						defer wg.Done()
						err := sdb.dbHandles[idx].Put(key, value, nil)
						if err != nil {
							iter.Release()
							panic(err)
						}
					}((dbIndex + i) % sdb.length)
				}
				dbIndexEnd := dbIndex + sdb.replication
				if (uint16(index) >= dbIndex && uint16(index) < dbIndexEnd) ||
					(dbIndexEnd >= sdb.length && uint16(index) < dbIndexEnd%sdb.length) {
					//data in new db, no need to delete
				} else {
					// Delete data from old db
					if err := sdb.dbHandles[index].Delete(key, nil); err != nil {
						iter.Release()
						panic(err)
					}
				}
			}

			sdb.Infof("Resharding db[%d] finished", index)
			iter.Release()
			dbReader.Release()
		}(x, snapshot)
	}

	wg.Wait()
	return nil
}
func GetKeyShardingIndexes(key []byte, shardingFunc ShardingFunc, length uint16, replication uint16) []uint16 {
	idx := shardingFunc(key, length)
	result := make([]uint16, 0)
	for i := uint16(0); i < replication; i++ {
		result = append(result, (idx+i)%length)
	}
	return result
}
