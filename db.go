package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"

	"sync/atomic"

	"sync"
)

const (
	NumShards           = 256
	HeaderSize          = 12
	ChunkSize           = 1024 * 1024
	WasteRatioThreshold = 0.25
	MinbyteArraySize    = 1 << 20 // 1 MB
)

type Command struct {
	Operation string
	Key       string
	Value     string
}

type shard struct {
	mu sync.RWMutex

	buckets   []uint32
	byteArray []byte

	bucketEntryCount uint32
	wastedBytes      uint32
	isEnqueued       bool
}

type byteArrayHeader struct {
	keyLen     uint32
	valLen     uint32
	nextOffset uint32
}

type DB struct {
	shards    [NumShards]shard
	queue     chan uint32
	maxMemory int64
	usedBytes atomic.Int64
	totalKeys atomic.Int64
}

func (db *DB) Init() {
	db.maxMemory = getMemoryLimit()
	db.queue = make(chan uint32, len(db.shards))

	db.usedBytes.Store(int64(NumShards))

	for i := range db.shards {
		db.shards[i] = shard{
			buckets:   make([]uint32, 16384),
			byteArray: make([]byte, 1, ChunkSize),
		}
	}

	db.StartCompactionWorker()
}

func (db *DB) Stats() string {
	used := db.usedBytes.Load()
	max := db.maxMemory
	keys := db.totalKeys.Load()

	// Build 30-character usage bar
	const barWidth = 30

	var filled int
	if max > 0 {
		filled = int((used * barWidth) / max)
		if filled > barWidth {
			filled = barWidth
		}
	}

	bar := strings.Repeat("█", filled) +
		strings.Repeat("░", barWidth-filled)

	percent := float64(used) * 100 / float64(max)

	return fmt.Sprintf(
		"MEMORY USAGE\n"+
			"[%s] %.2f%%\n\n"+
			"Used Memory    : %d bytes\n"+
			"Max Memory     : %d bytes\n"+
			"Live Keys      : %d\n"+
			"Queue Length   : %d\n",
		bar,
		percent,
		used,
		max,
		keys,
		len(db.queue),
	)
}

func (db *DB) StartCompactionWorker() {
	go func() {
		for shardID := range db.queue {
			db.compactShard(shardID)
		}
	}()
}

func (db *DB) compactShard(shardID uint32) {
	s := db.getShard(shardID)
	if s == nil {
		return
	}

	defer func() {
		s.mu.Lock()
		s.isEnqueued = false
		s.mu.Unlock()
	}()

	s.CompactSingleThreaded(db)
}

func (s *shard) CompactSingleThreaded(db *DB) {

	s.mu.Lock()
	defer s.mu.Unlock()

	oldbyteArrayLen := len(s.byteArray)

	if s.wastedBytes == 0 || len(s.byteArray) <= 1 {
		return
	}

	newBuckets := make([]uint32, len(s.buckets))

	liveBytes := len(s.byteArray) - int(s.wastedBytes)
	if liveBytes < 1 {
		liveBytes = 1
	}

	newbyteArray := make([]byte, 1, liveBytes)

	var activeEntries uint32

	for i := 0; i < len(s.buckets); i++ {

		seen := make(map[string]struct{})

		offset := s.buckets[i]

		for offset != 0 {
			key, value, valLen, nextOffset := readEntry(s.byteArray, offset)

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

			seen[keyStr] = struct{}{}

			newOffset := uint32(len(newbyteArray))

			header := byteArrayHeader{
				keyLen:     uint32(len(key)),
				valLen:     uint32(len(value)),
				nextOffset: newBuckets[i],
			}

			headerBuf := make([]byte, HeaderSize)
			writeHeader(headerBuf, header)

			newbyteArray = append(newbyteArray, headerBuf...)
			newbyteArray = append(newbyteArray, key...)
			newbyteArray = append(newbyteArray, value...)

			newBuckets[i] = newOffset
			activeEntries++

			offset = nextOffset
		}

	}

	s.byteArray = newbyteArray
	s.buckets = newBuckets
	s.bucketEntryCount = activeEntries
	s.wastedBytes = 0

	savedBytes := int64(oldbyteArrayLen) - int64(len(newbyteArray))

	db.usedBytes.Add(-savedBytes)

}

func (db *DB) usedMemory() int64 {
	var total int64

	for i := range db.shards {
		total += int64(len(db.shards[i].byteArray))
		total += int64(len(db.shards[i].buckets) * 4)
	}

	return total
}

func (db *DB) reservedMemory() int64 {
	var total int64

	for i := range db.shards {
		total += int64(cap(db.shards[i].byteArray))
		total += int64(len(db.shards[i].buckets) * 4)
	}

	return total
}

func (db *DB) getShard(hash uint32) *shard {
	return &db.shards[hash%NumShards]
}

func hashIndex(key string) uint32 {
	// var h uint32

	// for i := 0; i < len(key); i++ {
	// 	h = h*31 + uint32(key[i])
	// }

	// return h
	//
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func findEntryOffset(byteArray []byte, targetKey string, offset uint32) (uint32, bool) {
	cur := offset
	target := []byte(targetKey)

	for cur != 0 {
		key, _, valLen, nextOffset := readEntry(byteArray, cur)

		if bytes.Equal(key, target) && valLen != 0 {
			return cur, true
		}
		cur = nextOffset
	}

	return 0, false
}

func writeHeader(headerByteBuffer []byte, a byteArrayHeader) {
	binary.LittleEndian.PutUint32(headerByteBuffer[0:4], a.keyLen)
	binary.LittleEndian.PutUint32(headerByteBuffer[4:8], a.valLen)
	binary.LittleEndian.PutUint32(headerByteBuffer[8:12], a.nextOffset)

}

func getNextOffsetOfCurrent(byteArray []byte, offset uint32) (nextOffset uint32) {
	nextOffset = binary.LittleEndian.Uint32(byteArray[offset+8 : offset+12])
	return
}

func readEntry(byteArray []byte, offset uint32) (key []byte, value []byte, valLen uint32, nextOffset uint32) {
	keyLen := binary.LittleEndian.Uint32(byteArray[offset : offset+4])

	valLen = binary.LittleEndian.Uint32(byteArray[offset+4 : offset+8])
	nextOffset = binary.LittleEndian.Uint32(byteArray[offset+8 : offset+12])

	keyStart := offset + 12
	keyEnd := keyStart + keyLen
	valStart := keyEnd
	valEnd := valStart + valLen

	key = byteArray[keyStart:keyEnd]

	value = byteArray[valStart:valEnd]
	return
}

func growbyteArray(old []byte, requiredEntry int) []byte {
	newCap := cap(old) * 2

	if newCap == 0 {
		newCap = ChunkSize
	}

	for (len(old) + requiredEntry) > newCap {
		newCap = newCap * 2
	}

	newbyteArray := make([]byte, len(old), newCap)

	copy(newbyteArray, old)
	return newbyteArray
}
func writeNextOffset(byteArray []byte, offset uint32, nextOffset uint32) {
	binary.LittleEndian.PutUint32(byteArray[offset+8:offset+12], nextOffset)
}

func rebuildBucket(old []byte, bucketLen int) []uint32 {
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

func getValue(byteArray []byte, targetKey string, offset uint32) (string, bool) {
	cur := offset

	target := []byte(targetKey)

	for cur != 0 {
		key, value, valLen, nextOffset := readEntry(byteArray, cur)

		if bytes.Equal(key, target) && valLen != 0 {
			return string(value), true
		}
		cur = nextOffset
	}

	return "", false

}
