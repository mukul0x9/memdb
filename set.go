package main

import (
	"encoding/binary"
	"fmt"
)

func (db *DB) set(key string, value string) error {

	h := hashIndex(key)

	s := db.getShard(h)

	if s == nil {
		return fmt.Errorf("nil shard")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.arena) >= MinArenaSize {
		if float64(s.wastedBytes)/float64(len(s.arena)) >= WasteRatioThreshold && !s.isEnqueued {
			s.isEnqueued = true
			select {
			case db.queue <- h:
			default:
				s.isEnqueued = false
			}
		}
	}

	if len(s.buckets) == 0 {
		return fmt.Errorf("buckets not initialized")
	}

	finalIndex := h % uint32(len(s.buckets))

	keyByte := []byte(key)
	valueByte := []byte(value)

	nextOffset := s.buckets[finalIndex]

	oldOffset, found := findEntryOffset(s.arena, key, nextOffset)

	if found {
		oldKey, oldValue, _, _ := readEntry(s.arena, oldOffset)
		oldSize := HeaderSize + len(oldKey) + len(oldValue)
		s.wastedBytes += uint32(oldSize)
		binary.LittleEndian.PutUint32(s.arena[oldOffset+4:oldOffset+8], uint32(0))
	}

	entrySize := HeaderSize + len(key) + len(value)

	newTotal := db.usedBytes.Add(int64(entrySize))

	// Reject if this write would exceed maxMemory
	if newTotal > db.maxMemory {
		db.usedBytes.Add(-int64(entrySize))
		return fmt.Errorf("OOM(out of memory): maxmemory limit reached")
	}

	loadFactor := float64(s.bucketEntryCount) / float64(len(s.buckets))

	if loadFactor > 0.8 {
		newBuckets := rebuildBucket(s.arena, len(s.buckets)*2)

		if len(newBuckets) == 0 {
			db.usedBytes.Add(-int64(entrySize))
			return fmt.Errorf("failed to rebuild buckets")
		}
		s.buckets = newBuckets
		finalIndex = h % uint32(len(s.buckets))
		nextOffset = s.buckets[finalIndex]

	}

	if len(s.arena)+entrySize > cap(s.arena) {
		s.arena = growArena(s.arena, entrySize)

		if len(s.arena)+entrySize > cap(s.arena) {
			db.usedBytes.Add(-int64(entrySize))
			return fmt.Errorf("arena growth failed")
		}
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

	if !found {
		s.bucketEntryCount++
		db.totalKeys.Add(1)
	}

	return nil
}
