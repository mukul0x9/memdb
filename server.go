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

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		cmd, err := validator(reader)

		if err != nil {

			if err == io.EOF {
				return
			}

			responseWriter(conn, err.Error())
			continue
		}

		switch cmd.Operation {
		case "SET":

			// entry := logEntry{op: operation, Key: key, Data: value}
			// appendCh <- entry
			data := db.set(cmd.Key, cmd.Value)

			if data != nil {

				responseWriter(conn, "server error"+data.Error())

			} else {
				responseWriter(conn, "ok")
			}

		case "GET":
			data, ok := db.get(cmd.Key)

			if !ok {
				responseWriter(conn, "No data found")
			} else {
				responseWriter(conn, data)
			}

		case "DEL":
			data, ok := db.del(cmd.Key)

			if !ok {
				responseWriter(conn, "No data found")
			} else {
				responseWriter(conn, data)
			}

		default:
			responseWriter(conn, "Wrong operation")

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
