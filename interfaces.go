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

// CommonDbHandle is the interface that wraps the basic methods of a leveldb.DB
type CommonDbHandle interface {
	// Get returns the value for the given key.
	// @param key
	// @param ro
	// @return value
	// @return err
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	// Has returns whether the DB does contains the given key.
	// @param key
	// @param ro
	// @return ret
	// @return err
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	// NewIterator returns an iterator for the latest snapshot of the DB.
	// @param slice
	// @param ro
	// @return iterator.Iterator
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator

	// GetProperty returns the value of the given property for the DB.
	// @param name
	// @return value
	// @return err
	GetProperty(name string) (value string, err error)
	// Stats returns the DB's leveldb.DBStats.
	// @param s
	// @return error
	Stats(s *leveldb.DBStats) error
	// SizeOf returns the approximate file system space used by keys in the given ranges.
	// @param ranges
	// @return leveldb.Sizes
	// @return error
	SizeOf(ranges []util.Range) (leveldb.Sizes, error)
	// Close closes the DB.
	// @return error
	Close() error

	// Write writes the given batch to the DB.
	// @param batch
	// @param wo
	// @return error
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
	// Put sets the value for the given key.
	// @param key
	// @param value
	// @param wo
	// @return error
	Put(key, value []byte, wo *opt.WriteOptions) error
	// Delete deletes the value for the given key.
	// @param key
	// @param wo
	// @return error
	Delete(key []byte, wo *opt.WriteOptions) error
	// CompactRange manually compacts the underlying DB for the given key range.
	// @param r
	// @return error
	CompactRange(r util.Range) error
	// SetReadOnly sets the DB to read-only mode.
	// @return error
	SetReadOnly() error
}

// ShardingDbHandle is the interface that wraps the basic methods of a leveldb.DB
type ShardingDbHandle interface {
	CommonDbHandle
	// GetSnapshot returns a new snapshot of the DB.
	// @return Snapshot
	// @return error
	GetSnapshot() (Snapshot, error)
	// OpenTransaction opens a transaction.
	// @return Transaction
	// @return error
	OpenTransaction() (Transaction, error)
	// Resharding resharding the DB.
	// @return error
	Resharding() error
	// ShardCount returns the shard count of the DB.
	ShardCount() uint16
}

// Transaction is the interface that wraps the basic methods of a leveldb.Transaction
type Transaction interface {
	// Get returns the value for the given key.
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	// Has returns whether the DB does contains the given key.
	Has(key []byte, ro *opt.ReadOptions) (bool, error)
	// NewIterator returns an iterator for the latest snapshot of the DB.
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	// Put sets the value for the given key.
	Put(key, value []byte, wo *opt.WriteOptions) error
	// Delete deletes the value for the given key.
	Delete(key []byte, wo *opt.WriteOptions) error
	// Write writes the given batch to the DB.
	Write(b *leveldb.Batch, wo *opt.WriteOptions) error
	// Commit commits the transaction.
	Commit() error
	// Discard discards the transaction.
	Discard()
}

// Snapshot is the interface that wraps the basic methods of a leveldb.Snapshot
type Snapshot interface {
	// String returns a string representation of the snapshot
	String() string
	// Get returns the value for the given key
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	// Has returns whether the DB does contains the given key
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	// NewIterator returns an iterator for the latest snapshot of the DB
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
	// Release releases the snapshot
	Release()
}

// Batch is the interface that wraps the basic methods of a leveldb.Batch
type Batch interface {
	// Put sets the value for the given key.
	// @param key
	// @param value
	Put(key, value []byte)
	// Delete deletes the value for the given key.
	// @param key
	Delete(key []byte)
	// Dump returns the serialized representation of the batch.
	// @return []byte
	Dump() []byte
	// Load loads the batch from the serialized representation returned by Dump.
	// @param data
	// @return error
	Load(data []byte) error
	// Replay replays the batch contents into the given handler.
	// @param r
	// @return error
	Replay(r leveldb.BatchReplay) error
	// Len returns the number of updates in the batch.
	// @return int
	Len() int
	// Reset resets the batch contents.
	Reset()
}

// Logger is the interface that wraps the basic methods of a logger
type Logger interface {
	// Debug logs a debug message.
	Debug(msg string)
	// Info logs an info message.
	Info(msg string)
}

// Encryptor is the interface that wraps the basic methods of a encryptor
type Encryptor interface {
	// Encrypt encrypts the given data.
	Encrypt(data []byte) ([]byte, error)
	// Decrypt decrypts the given data.
	Decrypt(data []byte) ([]byte, error)
}
