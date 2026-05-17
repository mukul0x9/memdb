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
	fmt.Println("Connected! .")
	fmt.Println("-------------------------------------------------------------------")

	userInputReader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("You: ")

		//  Wait for user input
		input, err := userInputReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			break
		}

		if input == "exit\n" {
			fmt.Println("Exiting...")
			break
		}

		input = strings.Replace(input, "\r\n", "\n", -1)

		_, err = conn.Write([]byte(input))
		if err != nil {
			fmt.Printf("Error sending data: %v\n", err)
			break
		}

		reply, err := serverReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\n[Server disconnected]")
			} else {
				fmt.Printf("\n[Error reading from server: %v]\n", err)
			}
			break
		}

		fmt.Printf("Server: %s", reply)
	}
}
