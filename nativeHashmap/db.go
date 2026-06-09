package main

import (
	"sync"
	"time"
)

type DB[t any] struct {
	mu   sync.RWMutex
	data map[string]t
}

func New[t any]() *DB[t] {
	return &DB[t]{
		data: make(map[string]t),
	}
}

func (db *DB[t]) set(key string, value t) time.Duration {
	start := time.Now()
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
	return time.Since(start)
}

func (db *DB[t]) get(key string) (t, bool, time.Duration) {
	start := time.Now()
	db.mu.RLock()
	defer db.mu.RUnlock()
	value, ok := db.data[key]

	return value, ok, time.Since(start)
}

func (db *DB[t]) delete(key string) time.Duration {

	start := time.Now()
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data, key)
	return time.Since(start)
}
