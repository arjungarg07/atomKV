package atomkv

import (
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
	index map[string]int64
	mu    sync.RWMutex
}

// Open creates or opens a Bitcask database at the given path.
func Open(path string) (*Bitcask, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &Bitcask{
		file:  file,
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

	timestamp := time.Now().UnixNano()
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	if err := binary.Write(b.file, binary.LittleEndian, timestamp); err != nil {
		return err
	}
	if err := binary.Write(b.file, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return err
	}
	if err := binary.Write(b.file, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
		return err
	}
	if _, err := b.file.Write(keyBytes); err != nil {
		return err
	}
	if _, err := b.file.Write(valueBytes); err != nil {
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

	if _, err := b.file.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}

	var timestamp int64
	var keySize, valueSize uint32

	if err := binary.Read(b.file, binary.LittleEndian, &timestamp); err != nil {
		return "", err
	}
	if err := binary.Read(b.file, binary.LittleEndian, &keySize); err != nil {
		return "", err
	}
	if err := binary.Read(b.file, binary.LittleEndian, &valueSize); err != nil {
		return "", err
	}

	if _, err := b.file.Seek(int64(keySize), io.SeekCurrent); err != nil {
		return "", err
	}

	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(b.file, valueBytes); err != nil {
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

// Close closes the database file.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.file.Close()
}
