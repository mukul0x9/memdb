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

	if len(parts) < 1 {
		return Command{}, fmt.Errorf("invalid command")
	}

	operation := strings.ToUpper(parts[0])

	value := ""
	key := ""

	switch operation {
	case "SET":
		if len(parts) < 3 {
			return Command{}, fmt.Errorf("SET require Key and Value")
		}
		key = parts[1]

		value = strings.Join(parts[2:], " ")

	case "GET", "DEL":
		key = parts[1]
		if len(parts) != 2 {
			return Command{}, fmt.Errorf("%s requires a key", operation)
		}
	case "STATS":
		if len(parts) != 1 {
			return Command{}, fmt.Errorf("STATS does not require any arguments")
		}
	default:
		return Command{}, fmt.Errorf("unknown command")
	}



	cmd := Command{
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	return cmd, nil

}
