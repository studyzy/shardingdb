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
	"crypto/sha256"
	"encoding/binary"
)

// Sha256Sharding sharding function
// @param key
// @param max
// @return int
func Sha256Sharding(key []byte, max uint16) uint16 {
	// SHA256 hash key
	hash := sha256.Sum256(key)
	// Get the first 2 bytes
	hashBytes := hash[:2]
	// Convert to uint16
	hashInt := binary.BigEndian.Uint16(hashBytes)
	// Get the remainder
	return hashInt % max
}

const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
	r1 = 15
	r2 = 13
	m  = 5
	n  = 0xe6546b64
)

func rotl32(x, r uint32) uint32 {
	return (x << r) | (x >> (32 - r))
}

func fmix32(k uint32) uint32 {
	k ^= k >> 16
	k *= 0x85ebca6b
	k ^= k >> 13
	k *= 0xc2b2ae35
	k ^= k >> 16
	return k
}

// MurmurSharding sharding function
// @param key
// @param max
// @return uint16
func MurmurSharding(key []byte, max uint16) uint16 {
	length := uint32(len(key))
	nblocks := length / 4

	var h1 uint32 = 1 // seed

	buf := key
	for i := uint32(0); i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint32(buf[i*4 : (i+1)*4])

		k1 *= c1
		k1 = rotl32(k1, r1)
		k1 *= c2

		h1 ^= k1
		h1 = rotl32(h1, r2)
		h1 = h1*m + n
	}

	tail := key[nblocks*4:]
	var k1 uint32
	switch length & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = rotl32(k1, r1)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= length
	h1 = fmix32(h1)

	return uint16(h1 % uint32(max))
}

// XorSharding16 sharding function,but actually max is uint8
// @param key
// @param max
// @return uint16
func XorSharding16(key []byte, max uint16) uint16 {
	return uint16(XorSharding(key, uint8(max)))
}

func XorSharding(key []byte, max uint8) uint8 {
	var xorResult uint8
	for _, b := range key {
		xorResult ^= b
	}
	return xorResult % max
}
