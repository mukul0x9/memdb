package main

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
