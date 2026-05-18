package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

func tcpListener(db *DB) {

	listner, err := net.Listen("tcp", ":8888")

	fmt.Println("SERVER STARTED - LISTENING ON 8888")

	if err != nil {
		log.Fatal("error while creating listener", err)
	}

	defer listner.Close()

	for {
		conn, err := listner.Accept()

		if err != nil {
			log.Println("error accepting conn", err)
		}

		go handleCon(conn, db)

	}

}

func handleCon(conn net.Conn, db *DB) {
	defer func() {
			if r := recover(); r != nil {
				log.Printf("CRITICAL: Recovered from panic in worker connection: %v", r)

				// Inform the benchmark client so it registers as a StatusServerError
				responseWriter(conn, "ERROR_SERVER_PANIC")
				responseWriter(conn, "END")

				// Explicitly close the connection so the worker client doesn't freeze waiting for data
				conn.Close()
			}
		}()
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		cmd, err := validator(reader)

		if err != nil {

			if err == io.EOF {
				return
			}

			responseWriter(conn, err.Error())
			responseWriter(conn, "END")
			continue
		}

		switch cmd.Operation {
		case "SET":

			// entry := logEntry{op: operation, Key: key, Data: value}
			// appendCh <- entry
			//

			data := db.set(cmd.Key, cmd.Value)

			if data != nil {

				fmt.Println(data.Error())

				responseWriter(conn, "ERROR_SERVER"+data.Error())
				responseWriter(conn, "END")

			} else {
				responseWriter(conn, "ok")
				responseWriter(conn, "END")
			}

		case "GET":
			data, ok := db.get(cmd.Key)

			if !ok {
				responseWriter(conn, "NO_DATA_FOUND")
				responseWriter(conn, "END")
			} else {
				responseWriter(conn, data)
				responseWriter(conn, "END")
			}

		case "DEL":
			data, ok := db.del(cmd.Key)

			if !ok {
				responseWriter(conn, "NO_DATA_FOUND")
				responseWriter(conn, "END")
			} else {
				responseWriter(conn, data)
				responseWriter(conn, "END")
			}

		case "STATS":
			stats := db.Stats()
			responseWriter(conn, stats)
			responseWriter(conn, "END")

		default:
			responseWriter(conn, "ERROR_INVALID_OPERATION")
			responseWriter(conn, "END")

		}

	}

}

func responseWriter(conn net.Conn, message string) error {

	_, err := conn.Write([]byte(message + "\r\n"))

	if err != nil {
		log.Printf("error writing resposne ")
	}

	return err
}
