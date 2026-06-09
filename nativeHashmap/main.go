package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	db := New[string]()
	tcpListener(db)
}

func tcpListener(db *DB[string]) {
	listner, err := net.Listen("tcp", ":8080")

	if err != nil {
		fmt.Println(err)

	}
	fmt.Println("SERVER STARTED - LISTENING ON ", listner.Addr())
	defer listner.Close()
	for {
		conn, err := listner.Accept()
		if err != nil {
			fmt.Println(err)
			continue

		}

		go handleConnection(conn, db)

	}
}

func handleConnection(conn net.Conn, db *DB[string]) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(" panic error", r)

			responseWriter(conn, "panic error", "END")

		}

	}()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		parts := strings.Fields(line)
		if len(parts) < 1 {
			continue
		}
		operation := strings.ToUpper(parts[0])

		key := ""
		value := ""

		switch operation {
		case "SET":
			if len(parts) < 3 {
				continue
			}
			key = parts[1]
			value = strings.Join(parts[2:], " ")
			db.set(key, value)
			responseWriter(conn, "ok", "END")
		case "GET", "DEL":
			if len(parts) < 2 {
				continue
			}
			key = parts[1]

			value, ok, _ := db.get(key)
			if !ok {
				responseWriter(conn, "not found", "END")
				continue
			}

			if operation == "GET" {
				responseWriter(conn, value, "END")
			} else if operation == "DEL" {
				db.delete(key)
				responseWriter(conn, "ok", "END")
			}

		default:
			responseWriter(conn, "unknownoperation", "END")
		}

	}

}

func responseWriter(conn net.Conn, messages ...string) error {
	for _, message := range messages {
		_, err := conn.Write([]byte(message + "\r\n"))
		if err != nil {
			fmt.Println("error writing to connection", err)
			return err
		}
	}
	return nil
}
