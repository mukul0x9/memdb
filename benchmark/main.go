package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	serverAddr   = "127.0.0.1:8888" // Your custom TCP DB address
	concurrency  = 100              // 100 concurrent workers
	testDuration = 1 * time.Minute  // Run for exactly 1 minute
	charset      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	keyLength    = 16
	valueLength  = 256
)

func generateString(length int) string {
	b := make([]byte, length)

	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func preGenerateSetKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = generateString(keyLength)
	}

	return keys

}

func preGenerateValues(valueLength int, count int) []string {

	values := make([]string, count)
	for i := 0; i < count; i++ {
		values[i] = generateString(valueLength)
	}

	return values

}

func set(workerId int, totalRequests *int64, stopChan <-chan struct{}, wg *sync.WaitGroup, keys []string, values []string) {

	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerId)))

	conn, err := net.Dial("tcp", serverAddr)

	if err != nil {
		fmt.Printf("Worker %d failed to connect to server: %v\n", workerId, err)
		return
	}

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-stopChan:
			return
		default:
			key := keys[rng.Intn(len(keys))]

			payload := fmt.Sprintf("SET %s %s\n", key, values[rng.Intn(len(values))])

			_, err := conn.Write([]byte(payload))
			if err != nil {
				fmt.Printf("Worker %d failed to write to server: %v\n", workerId, err)
				return
			}

			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					fmt.Printf("Worker %d failed to read from server: %v\n", workerId, err)
					return
				}
				if strings.TrimRight(line, "\r\n") == "END" {
					break
				}
			}

			atomic.AddInt64(totalRequests, 1)

		}
	}

}

func setOperation(keys []string, values []string) {

	var totalRequests int64

	var wg sync.WaitGroup

	stopChan := make(chan struct{})

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go set(i, &totalRequests, stopChan, &wg, keys, values)
	}

	time.Sleep(testDuration)

	close(stopChan)
	wg.Wait()

	actualDuration := time.Since(startTime)

	rps := float64(totalRequests) / actualDuration.Seconds()

	fmt.Println("benchmark Result")
	fmt.Printf("total sets : %d\n", totalRequests)
	fmt.Printf("duration : %s\n", actualDuration)
	fmt.Printf("RPS : %.2f\n", rps)
}

func main() {

	keys := preGenerateSetKeys(1000000)
	staticValues := preGenerateValues(valueLength, 10000)

	setOperation(keys, staticValues)

}
