package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	serverAddr = "127.0.0.1:8888"

	numKeys    = 1_000_000
	keyLength  = 10
	valueSize  = 100

	numWorkers = 100
	testTime   = 5 * time.Second
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var globalRNG = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	// Pre-generate keys
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = randomString(keyLength)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTime)
	defer cancel()

	var totalRequests uint64
	var totalErrors uint64

	var getCount uint64
	var setCount uint64
	var delCount uint64

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			// One TCP connection per worker (much faster than reconnecting every request)
			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				atomic.AddUint64(&totalErrors, 1)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)

			// Per-worker RNG
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := keys[rng.Intn(len(keys))]
				op := rng.Intn(100)

				var cmd string
				var counter *uint64

				switch {
				case op < 70: // 70% GET
					cmd = fmt.Sprintf("GET %s\r\n", key)
					counter = &getCount

				case op < 90: // 20% SET
					value := randomString(valueSize)
					cmd = fmt.Sprintf("SET %s %s\r\n", key, value)
					counter = &setCount

				default: // 10% DEL
					cmd = fmt.Sprintf("DEL %s\r\n", key)
					counter = &delCount
				}

				if err := sendCommand(conn, reader, cmd); err != nil {
					atomic.AddUint64(&totalErrors, 1)
					return
				}

				atomic.AddUint64(&totalRequests, 1)
				atomic.AddUint64(counter, 1)
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)

	total := atomic.LoadUint64(&totalRequests)
	errors := atomic.LoadUint64(&totalErrors)

	fmt.Printf("\n===== Benchmark Results =====\n")
	fmt.Printf("Duration: %.2f sec\n", elapsed.Seconds())
	fmt.Printf("Workers: %d\n", numWorkers)
	fmt.Printf("Total Success: %d\n", total)
	fmt.Printf("Errors: %d\n", errors)
	fmt.Printf("Throughput: %.2f req/sec\n", float64(total)/elapsed.Seconds())
	fmt.Printf("GET: %d\n", atomic.LoadUint64(&getCount))
	fmt.Printf("SET: %d\n", atomic.LoadUint64(&setCount))
	fmt.Printf("DEL: %d\n", atomic.LoadUint64(&delCount))
}

// sendCommand sends one command and reads until END
func sendCommand(conn net.Conn, reader *bufio.Reader, cmd string) error {
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("server disconnected")
			}
			return err
		}

		line = strings.TrimRight(line, "\r\n")

		if line == "END" {
			return nil
		}


	}
}


func randomString(n int) string {
	for {
		b := make([]byte, n)
		for i := range b {
			b[i] = charset[globalRNG.Intn(len(charset))]
		}

		s := string(b)
		if !strings.HasPrefix(s, "ERR") {
			return s
		}
	}
}
