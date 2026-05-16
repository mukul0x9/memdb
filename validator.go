package main

import (
	"bufio"
	"fmt"
	"strings"
)

func validator(reader *bufio.Reader) (Command, error) {

	message, err := reader.ReadString('\n')

	if err != nil {
		return Command{}, err
	}

	parts := strings.Fields(message)

	if len(parts) < 2 {
		return Command{}, fmt.Errorf("invalid command")
	}

	operation := strings.ToUpper(parts[0])

	value := ""

	switch operation {
	case "SET":
		if len(parts) < 3 {
			return Command{}, fmt.Errorf("SET require Key and Value")
		}

		value = strings.Join(parts[2:], " ")

	case "GET", "DEL":
		if len(parts) != 2 {
			return Command{}, fmt.Errorf("%s requires a key", operation)
		}
	default:
		return Command{}, fmt.Errorf("unknown command")
	}

	key := parts[1]

	cmd := Command{
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	return cmd, nil

}
