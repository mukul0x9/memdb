package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type Response struct {
	WorkerID int
	Command  string
	Response string
	Error    error
}

func main() {
	var wg sync.WaitGroup

	numGoroutines := 100
	responses := make(chan Response, numGoroutines)
	done := make(chan struct{})

	timeout := 5 * time.Second

	go func() {
		time.Sleep(timeout)
		close(done)
	}()

	count := 10000
	stringsArray := make([]string, count)

	commandString := []string{
		"SET",
		"GET",
		"DEL",
	}

	for i := 0; i < count; i++ {
		stringsArray[i] = generateRandomString(10)
	}

	start := time.Now()
	var statsWG sync.WaitGroup
	statsWG.Add(1)
	go func() {

		defer statsWG.Done()

		var totalOps int
		var totalErrors int

		var setOps int
		var getOps int
		var delOps int

		var setSuccess int
		var getHits int
		var getMisses int
		var delSuccess int
		for response := range responses {
			if response.Error != nil {
				totalErrors++
				continue
			}

			totalOps++

			// Remove trailing newline characters.
			resp := strings.TrimSpace(response.Response)

			switch response.Command {
			case "SET":
				setOps++
				if resp == "ok" {
					setSuccess++
				}

			case "GET":
				getOps++
				if resp == "NO_DATA_FOUND" {
					getMisses++
				} else {
					getHits++
				}

			case "DEL":
				delOps++
				if resp == "DELETED" {
					delSuccess++
				}
			}

		}

		elapsed := time.Since(start)

		fmt.Println("\n========== Benchmark Results ==========")
		fmt.Printf("Duration:        %v\n", elapsed)
		fmt.Printf("Total Ops:       %d\n", totalOps)
		fmt.Printf("Throughput:      %.2f ops/sec\n",
			float64(totalOps)/elapsed.Seconds())
		fmt.Printf("Errors:          %d\n", totalErrors)

		fmt.Println("\nCommand Breakdown")
		fmt.Printf("SET Ops:         %d (successful: %d)\n",
			setOps, setSuccess)
		fmt.Printf("GET Ops:         %d (hits: %d, misses: %d)\n",
			getOps, getHits, getMisses)
		fmt.Printf("DEL Ops:         %d (deleted: %d)\n",
			delOps, delSuccess)
		fmt.Println("=======================================")
	}()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			conn, err := net.Dial("tcp", "localhost:8888")
			defer wg.Done()
			if err != nil {
				fmt.Printf("Goroutine %d: failed to connect", id)
				return
			}
			reader := bufio.NewReader(conn)
			defer conn.Close()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for {
				select {
				case <-done:
					return
				default:

					keyString := stringsArray[rng.Intn(count)]

					command := commandString[rng.Intn(len(commandString))]

					if command == "SET" {
						valueString := generateRandomString(10)
						_, err := conn.Write([]byte(command + " " + keyString + " " + valueString + "\n"))
						if err != nil {
							fmt.Printf("Goroutine %d: failed to write", id)
							continue
						}
					} else if command == "GET" {
						_, err := conn.Write([]byte(command + " " + keyString + "\n"))
						if err != nil {
							fmt.Printf("Goroutine %d: failed to write", id)
							continue
						}
					} else if command == "DEL" {
						_, err := conn.Write([]byte(command + " " + keyString + "\n"))
						if err != nil {
							fmt.Printf("Goroutine %d: failed to write", id)
							continue
						}

					}
					var fullResponse strings.Builder
					for {
						response, err := reader.ReadString('\n')
						if err != nil {
							fmt.Printf("Goroutine %d: failed to read: %v\n", id, err)
							break
						}

						response = strings.TrimRight(response, "\r\n")

						// END marks the end of this command's response.
						if response == "END" {
							break
						}

						fullResponse.WriteString(response)
						fullResponse.WriteString("\n")
					}

					responses <- Response{
						WorkerID: id,
						Command:  command,
						Response: fullResponse.String(),
						Error:    nil,
					}

				}
			}
		}(i)
	}
	wg.Wait()
	close(responses)
	statsWG.Wait()
	fmt.Println("All goroutines have finished")

}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func getRandomCommand() string {
	r := rand.Float64()
	if r < 0.4 {
		return "GET"
	} else if r < 0.7 {
		return "SET"
	}
	return "DEL"
}
