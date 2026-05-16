package main

import "encoding/binary"

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
