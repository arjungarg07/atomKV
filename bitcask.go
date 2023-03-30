package atomkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

// Bitcask is an append-only key-value store with an in-memory index.
type Bitcask struct {
	file  *os.File
	path  string
	index map[string]int64
	mu    sync.RWMutex
}

// Open creates or opens a Bitcask database at the given path.
func Open(path string) (*Bitcask, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Bitcask{
		file:  file,
		path:  path,
		index: make(map[string]int64),
	}, nil
}

// Set writes a key-value pair to disk and updates the in-memory index.
func (b *Bitcask) Set(key, value string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	offset, err := b.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Buffer the entire record before writing
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, time.Now().UnixNano())
	binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes)))
	binary.Write(buf, binary.LittleEndian, uint32(len(valueBytes)))
	buf.Write(keyBytes)
	buf.Write(valueBytes)

	if _, err := b.file.Write(buf.Bytes()); err != nil {
		return err
	}

	b.index[key] = offset
	return nil
}

// Get retrieves a value by key using the in-memory index.
func (b *Bitcask) Get(key string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	offset, exists := b.index[key]
	if !exists {
		return "", ErrKeyNotFound
	}

	// Read header: timestamp(8) + keySize(4) + valueSize(4) = 16 bytes
	header := make([]byte, 16)
	if _, err := b.file.ReadAt(header, offset); err != nil {
		return "", err
	}

	keySize := binary.LittleEndian.Uint32(header[8:12])
	valueSize := binary.LittleEndian.Uint32(header[12:16])

	// Read value at offset + header + key
	valueBytes := make([]byte, valueSize)
	valueOffset := offset + 16 + int64(keySize)
	if _, err := b.file.ReadAt(valueBytes, valueOffset); err != nil {
		return "", err
	}

	return string(valueBytes), nil
}

// Load rebuilds the in-memory index from the data file.
func (b *Bitcask) Load() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		offset, err := b.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		var timestamp int64
		if err := binary.Read(b.file, binary.LittleEndian, &timestamp); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		var keySize, valueSize uint32
		if err := binary.Read(b.file, binary.LittleEndian, &keySize); err != nil {
			return err
		}
		if err := binary.Read(b.file, binary.LittleEndian, &valueSize); err != nil {
			return err
		}

		keyBytes := make([]byte, keySize)
		if _, err := io.ReadFull(b.file, keyBytes); err != nil {
			return err
		}

		if _, err := b.file.Seek(int64(valueSize), io.SeekCurrent); err != nil {
			return err
		}

		b.index[string(keyBytes)] = offset
	}

	return nil
}

// Compact creates a new file with only the latest value for each key.
func (b *Bitcask) Compact() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tempPath := b.path + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	newIndex := make(map[string]int64)

	for key, oldOffset := range b.index {
		if _, err := b.file.Seek(oldOffset, io.SeekStart); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return err
		}

		var timestamp int64
		var keySize, valueSize uint32
		binary.Read(b.file, binary.LittleEndian, &timestamp)
		binary.Read(b.file, binary.LittleEndian, &keySize)
		binary.Read(b.file, binary.LittleEndian, &valueSize)

		b.file.Seek(int64(keySize), io.SeekCurrent)
		valueBytes := make([]byte, valueSize)
		io.ReadFull(b.file, valueBytes)

		newOffset, _ := tempFile.Seek(0, io.SeekEnd)
		binary.Write(tempFile, binary.LittleEndian, timestamp)
		binary.Write(tempFile, binary.LittleEndian, uint32(len(key)))
		binary.Write(tempFile, binary.LittleEndian, valueSize)
		tempFile.Write([]byte(key))
		tempFile.Write(valueBytes)

		newIndex[key] = newOffset
	}

	b.file.Close()
	tempFile.Close()

	if err := os.Rename(tempPath, b.path); err != nil {
		return err
	}

	newFile, err := os.OpenFile(b.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	b.file = newFile
	b.index = newIndex
	return nil
}

// Keys returns all keys in the database.
func (b *Bitcask) Keys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.index))
	for k := range b.index {
		keys = append(keys, k)
	}
	return keys
}

// Close closes the database file.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.file.Close()
}
