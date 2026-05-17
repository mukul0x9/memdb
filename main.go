package main

import (
	"log"
	"os"
	"strconv"
)

func main() {

	var db DB

	db.Init()

	tcpListener(&db)
}

func getMemoryLimit() int64 {
	// Default: 512 MB
	const defaultLimit = int64(512 * 1024 * 1024)

	value := os.Getenv("CACHE_MEMORY_LIMIT")
	if value == "" {
		return defaultLimit
	}

	if bytes, err := strconv.ParseInt(value, 10, 64); err == nil {
		return bytes
	}

	log.Printf("Invalid CACHE_MEMORY_LIMIT=%q, using default", value)
	return defaultLimit
}
