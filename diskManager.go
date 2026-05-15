package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func buildCache(fileName string, globalKV map[string]string) {
	fmt.Println(globalKV)

	var finalSeq = 0

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error Opening file", err)
		return
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		splittedText := strings.Split(line, "|")

		fmt.Println("Read Line:", splittedText)

		if len(splittedText) > 0 {

			seqValue, err := strconv.Atoi(splittedText[0])

			if err != nil {
				fmt.Println("seq not an integer")
			} else {
				finalSeq += seqValue
			}

			operation := splittedText[1]

			key := splittedText[2]

			value := splittedText[3]

			if operation == "SET" {
				globalKV[key] = value
			}
			if operation == "DEL" {
				delete(globalKV, key)
			}

		}

	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file line-by-line:", err)
	}

}
