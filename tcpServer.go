package main

import (
	"bufio"
	"log"
	"net"
	"strings"
	"time"
)

// type logEntry struct {
// 	seq  int
// 	op   string
// 	Key  string
// 	Data string
// }

const bufSize = 100

// var appendCh = make(chan logEntry, 100)

const maxBatch = 100
const flushInterval = 50 * time.Millisecond

//func flushBufferToDisk(buffer []logEntry) {

//}

// func bufferLogWriter(seq int) {
// 	buffer := []logEntry{}

// 	ticker := time.NewTicker(500 * time.Millisecond)

// 	defer ticker.Stop()

// 	flush := func() {
// 		if len(buffer) == 0 {
// 			return
// 		}
// 		flushBufferToDisk(buffer)
// 		buffer = buffer[:0]
// 	}

// 	for {
// 		select {
// 		case data, ok := <-appendCh:
// 			if !ok {
// 				flush()
// 				return
// 			}
// 			seq++
// 			data.seq = seq
// 			buffer = append(buffer, data)

// 			if len(buffer) >= 100 {
// 				flush()
// 			}

// 		case <-ticker.C:
// 			flush()
// 		}
// 	}

// }

func listener(db *DB) {

	listner, err := net.Listen("tcp", ":8888")

	if err != nil {
		log.Fatal("error while creating listener", err)
	}

	defer listner.Close()

	for {
		conn, err := listner.Accept()

		if err != nil {
			log.Println("error accepting conn", err)
		}

		handleConnection(conn, db)

	}

}

func handleConnection(conn net.Conn, db *DB) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		message, err := reader.ReadString('\n')

		message = strings.TrimSpace(message)

		if err != nil {
			log.Println("error while reading message", err)
		}

		log.Println(message)

		parts := strings.Fields(message)

		operation := parts[0]

		key := parts[1]

		switch operation {
		case "SET":
			value := strings.Join(parts[2:], " ")
			// entry := logEntry{op: operation, Key: key, Data: value}
			// appendCh <- entry
			db.set(key, value)
			response(operation, conn, key)
		case "GET":
			data, ok := db.get(key)

			if !ok {
				response(operation, conn, "")
			}

			response(operation, conn, data)
		default:
			response(operation, conn, operation)

		}

	}

}

func response(operation string, conn net.Conn, data string) {

	var message string = "provide correct operation"
	if operation == "SET" {

		message = "DATA SAVED"

	}
	if operation == "GET" {

		log.Println(data)
		message = data
	}

	_, err := conn.Write([]byte(message + "\r\n"))

	if err != nil {
		log.Printf("error writing resposne ")
	}
}

// func set(kv map[string]string, key string, value string) {
// 	kv[key] = value
// }

// func get(kv map[string]string, key string) string {
// 	return kv[key]
// }
