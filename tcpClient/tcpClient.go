package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	serverAddr := "127.0.0.1:8888"

	fmt.Printf("Connecting to TCP server at %s...\n", serverAddr)

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected!")
	fmt.Println("Type commands like:")
	fmt.Println("  SET HELLO WORLD")
	fmt.Println("  GET HELLO")
	fmt.Println("  DEL HELLO")
	fmt.Println("  STATS")

	fmt.Println("------------------------------------------------------------")

	userInputReader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("You: ")

		// Read user input
		input, err := userInputReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			break
		}

		input = strings.TrimRight(input, "\r\n")

		if input == "exit" {
			fmt.Println("Exiting...")
			break
		}

		// Send command to server
		_, err = conn.Write([]byte(input + "\r\n"))
		if err != nil {
			fmt.Printf("Error sending data: %v\n", err)
			break
		}

		// Read response until END marker
		for {
			reply, err := serverReader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("\n[Server disconnected]")
				} else {
					fmt.Printf("\n[Error reading from server: %v]\n", err)
				}
				return
			}

			reply = strings.TrimRight(reply, "\r\n")

			// END marks end of server response
			if reply == "END" {
				break
			}

			fmt.Println("Server:", reply)
		}
	}
}
