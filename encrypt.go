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
	"crypto/aes"
	"crypto/cipher"
	"errors"
)

var _ Encryptor = (*AESCryptor)(nil)

// AESCryptor is a encryptor using AES
type AESCryptor struct {
	key []byte
}

// NewAESCryptor creates a new AESCryptor
func NewAESCryptor(key []byte) *AESCryptor {
	return &AESCryptor{key: key}
}

// Encrypt encrypts data with AES
func (a *AESCryptor) Encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(a.key)
	if err != nil {
		return nil, err
	}

	paddingSize := aes.BlockSize - len(data)%aes.BlockSize
	padding := make([]byte, paddingSize)
	for i := 0; i < paddingSize; i++ {
		padding[i] = byte(paddingSize)
	}
	//create a new slice with padding
	encryptedData := make([]byte, len(data)+paddingSize)
	copy(encryptedData, data)
	copy(encryptedData[len(data):], padding)

	ciphertext := make([]byte, len(encryptedData)+aes.BlockSize)
	iv := ciphertext[:aes.BlockSize]
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], encryptedData)

	return ciphertext, nil
}

// Decrypt decrypts data
func (a *AESCryptor) Decrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(a.key)
	if err != nil {
		return nil, err
	}

	if len(data) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}

	iv := data[:aes.BlockSize]
	decryptedData := make([]byte, len(data)-aes.BlockSize)
	copy(decryptedData, data[aes.BlockSize:])
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(decryptedData, decryptedData)

	paddingSize := int(decryptedData[len(decryptedData)-1])
	return decryptedData[:len(decryptedData)-paddingSize], nil
}
