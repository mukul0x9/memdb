package main

package main

import (
	"bytes"
	"encoding/binary"
	"sync"
)

const (
	NumShards  = 256
	HeaderSize = 12
	ChunkSize  = 1024 * 1024
)

type shard struct {
	mu sync.RWMutex

	buckets []uint32
	arena   []byte

	bucketEntryCount uint32
}

type arenaHeader struct {
	keyLen     uint32
	valLen     uint32
	nextOffset uint32
}

type DB struct {
	shards [NumShards]shard
}

func (db *DB) Init() {
	for i := range db.shards {
		db.shards[i] = shard{
			buckets: make([]uint32, 1024),
			arena:   make([]byte, 1, ChunkSize),
		}
	}
}

func (db *DB) getShard(hash uint32) *shard {
	return &db.shards[hash%NumShards]
}

func hashIndex(key string) uint32 {
	var h uint32

	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}

	return h
}

func findEntryOffset(arena []byte, targetKey string, offset uint32) (uint32, bool) {
	cur := offset
	target := []byte(targetKey)

	for cur != 0 {
		key, _, valLen, nextOffset := readEntry(arena, cur)

		if bytes.Equal(key, target) && valLen != 0 {
			return cur, true
		}
		cur = nextOffset
	}

	return 0, false
}

func writeHeader(headerByteBuffer []byte, a arenaHeader) {

	binary.LittleEndian.PutUint32(headerByteBuffer, a.keyLen)
	binary.LittleEndian.PutUint32(headerByteBuffer, a.valLen)
	binary.LittleEndian.PutUint32(headerByteBuffer, a.nextOffset)

}

func getNextOffsetOfCurrent(arena []byte, offset uint32) (nextOffset uint32) {
	nextOffset = binary.LittleEndian.Uint32(arena[offset+8 : offset+12])
	return
}

func readEntry(arena []byte, offset uint32) (key []byte, value []byte, valLen uint32, nextOffset uint32) {
	keyLen := binary.LittleEndian.Uint32(arena[offset : offset+4])
	valLen = binary.LittleEndian.Uint32(arena[offset+4 : offset+8])
	nextOffset = binary.LittleEndian.Uint32(arena[offset+8 : offset+12])

	keyStart := offset + 12
	keyEnd := keyStart + keyLen
	valStart := keyEnd
	valEnd := valStart + valLen

	key = arena[keyStart:keyEnd]
	value = arena[valStart:valEnd]
	return
}

func growArena(old []byte, requiredEntry int) []byte {
	newCap := cap(old) * 2

	if newCap == 0 {
		newCap = ChunkSize
	}

	for (len(old) + requiredEntry) > newCap {
		newCap = newCap * 2
	}

	newArena := make([]byte, len(old), newCap)

	copy(newArena, old)
	return newArena
}
func writeNextOffset(arena []byte, offset uint32, nextOffset uint32) {
	binary.LittleEndian.PutUint32(arena[offset+8:offset+12], nextOffset)
}

func rebuildBucket(old []byte, bucketLen int) []uint32 {
	buckets := make([]uint32, bucketLen)

	offset := uint32(1)

	for offset < uint32(len(old)) {

		key, value, vallen, _ := readEntry(old, offset)

		if vallen != 0 {
			hash := hashIndex(string(key))

			finalIndex := hash % uint32(bucketLen)

			writeNextOffset(old, offset, buckets[finalIndex])

			buckets[finalIndex] = offset

		}

		offset += uint32(HeaderSize + len(key) + len(value))

	}

	return buckets

}

func (db *DB) set(key string, value string) {
	h := hashIndex(key)
	s := db.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	finalIndex := h % uint32(len(s.buckets))

	keyByte := []byte(key)
	valueByte := []byte(value)

	nextOffset := s.buckets[finalIndex]

	entrySize := HeaderSize + len(key) + len(value)

	loadFactor := float64(s.bucketEntryCount) / float64(len(s.buckets))

	if loadFactor > 0.8 {
		s.buckets = rebuildBucket(s.arena, len(s.buckets)*2)
		finalIndex = h % uint32(len(s.buckets))
		nextOffset = s.buckets[finalIndex]

	}

	if len(s.arena)+entrySize > cap(s.arena) {
		s.arena = growArena(s.arena, entrySize)
	}

	headerStruct := arenaHeader{
		keyLen:     uint32(len(keyByte)),
		valLen:     uint32(len(valueByte)),
		nextOffset: nextOffset,
	}

	headerBuffer := make([]byte, HeaderSize)

	writeHeader(headerBuffer, headerStruct)

	newOffset := uint32(len(s.arena))

	s.arena = append(s.arena, headerBuffer...)
	s.arena = append(s.arena, keyByte...)
	s.arena = append(s.arena, valueByte...)

	s.buckets[finalIndex] = newOffset

	s.bucketEntryCount++

}

func getValue(arena []byte, targetKey string, offset uint32) (string, bool) {
	cur := offset

	target := []byte(targetKey)

	for cur != 0 {
		key, value, valLen, nextOffset := readEntry(arena, cur)

		if bytes.Equal(key, target) && valLen != 0 {
			return string(value), true
		}
		cur = nextOffset
	}

	return "", false

}
func (db *DB) get(key string) (string, bool) {
	h := hashIndex(key)
	s := db.getShard(h)

	s.mu.RLock()
	defer s.mu.RUnlock()

	finalIndex := h % uint32(len(s.buckets))

	if s.buckets[finalIndex] != 0 {
		offset := s.buckets[finalIndex]

		value, ok := getValue(s.arena, key, offset)

		if ok {
			return value, true
		}

		return "", false

	}

	return "", false

}

func (db *DB) del(key string) (string, bool) {

	h := hashIndex(key)
	s := db.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	finalIndex := h % uint32(len(s.buckets))

	if s.buckets[finalIndex] != 0 {

		entryOffset, ok := findEntryOffset(s.arena, key, s.buckets[finalIndex])

		if ok {

			binary.LittleEndian.PutUint32(s.arena[entryOffset+4:entryOffset+8], uint32(0))

			return "DELETED", true

		}

	}

	return "no key value exist", false

}
