package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"sync"
)

const (
	NumShards           = 256
	HeaderSize          = 12
	ChunkSize           = 1024 * 1024
	WasteRatioThreshold = 0.25
	MinArenaSize        = 1 << 20 // 1 MB
)

type Command struct {
	Operation string
	Key       string
	Value     string
}

type shard struct {
	mu sync.RWMutex

	buckets []uint32
	arena   []byte

	bucketEntryCount uint32
	wastedBytes      uint32
	isEnqueued       bool
}

type arenaHeader struct {
	keyLen     uint32
	valLen     uint32
	nextOffset uint32
}

type DB struct {
	shards    [NumShards]shard
	queue     chan uint32
	maxMemory int64
}

func (db *DB) Init() {
	db.maxMemory = getMemoryLimit()
	db.queue = make(chan uint32, len(db.shards))

	for i := range db.shards {
		db.shards[i] = shard{
			buckets: make([]uint32, 1024),
			arena:   make([]byte, 1, ChunkSize),
		}
	}
}

func (db *DB) StartCompactionWorker() {
	go func() {
		// loop reads from the channel sequentially.

		for shardID := range db.queue {
			s := db.getShard(shardID)
			if s == nil {
				continue
			}

			s.CompactSingleThreaded()

			s.mu.Lock()
			s.isEnqueued = false
			s.mu.Unlock()
		}
	}()
}

func (s *shard) CompactSingleThreaded() {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf(
		"Compacting shard: arena=%d wasted=%d\n",
		len(s.arena),
		s.wastedBytes,
	)
	if s.wastedBytes == 0 || len(s.arena) <= 1 {
		return
	}

	newBuckets := make([]uint32, len(s.buckets))

	liveBytes := len(s.arena) - int(s.wastedBytes)
	if liveBytes < 1 {
		liveBytes = 1
	}

	newArena := make([]byte, 1, liveBytes)

	var activeEntries uint32

	for i := 0; i < len(s.buckets); i++ {

		seen := make(map[string]struct{})

		offset := s.buckets[i]

		for offset != 0 {
			key, value, valLen, nextOffset := readEntry(s.arena, offset)

			// Skip deleted entries
			if valLen == 0 {
				offset = nextOffset
				continue
			}

			keyStr := string(key)

			if _, exists := seen[keyStr]; exists {
				offset = nextOffset
				continue
			}

			newOffset := uint32(len(newArena))

			header := arenaHeader{
				keyLen:     uint32(len(key)),
				valLen:     uint32(len(value)),
				nextOffset: newBuckets[i],
			}

			headerBuf := make([]byte, HeaderSize)
			writeHeader(headerBuf, header)

			newArena = append(newArena, headerBuf...)
			newArena = append(newArena, key...)
			newArena = append(newArena, value...)

			newBuckets[i] = newOffset
			activeEntries++

			offset = nextOffset
		}

		fmt.Printf("Compaction finished in %v\n", time.Since(start))

	}

	s.arena = newArena
	s.buckets = newBuckets
	s.bucketEntryCount = activeEntries
	s.wastedBytes = 0
}
func (db *DB) usedMemory() int64 {
	var total int64

	for i := range db.shards {
		total += int64(len(db.shards[i].arena))
		total += int64(len(db.shards[i].buckets) * 4)
	}

	return total
}

func (db *DB) reservedMemory() int64 {
	var total int64

	for i := range db.shards {
		total += int64(cap(db.shards[i].arena))
		total += int64(len(db.shards[i].buckets) * 4)
	}

	return total
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
	binary.LittleEndian.PutUint32(headerByteBuffer[0:4], a.keyLen)
	binary.LittleEndian.PutUint32(headerByteBuffer[4:8], a.valLen)
	binary.LittleEndian.PutUint32(headerByteBuffer[8:12], a.nextOffset)

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
	fmt.Println("rebuilding buckets...")
	buckets := make([]uint32, bucketLen)

	offset := uint32(1)

	for offset < uint32(len(old)) {

		key, value, vallen, _ := readEntry(old, offset)

		entrySize := uint32(HeaderSize + len(key) + len(value))

		if vallen != 0 {
			hash := hashIndex(string(key))

			finalIndex := hash % uint32(bucketLen)

			writeNextOffset(old, offset, buckets[finalIndex])

			buckets[finalIndex] = offset

		}
		offset += entrySize

	}

	return buckets

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
