package main

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestResurrectionBug(t *testing.T) {
	var db DB
	db.Init()

	key := "resurrect_test_key"
	val1 := "value_one"
	val2 := "value_two"

	// 1. SET key -> val1
	if err := db.set(key, val1); err != nil {
		t.Fatalf("Failed to set key to val1: %v", err)
	}

	// 2. SET key -> val2
	if err := db.set(key, val2); err != nil {
		t.Fatalf("Failed to set key to val2: %v", err)
	}

	// 3. DEL key
	if _, ok := db.del(key); !ok {
		t.Fatalf("Failed to delete key")
	}

	// 4. GET key immediately (should be not found)
	if val, ok := db.get(key); ok {
		t.Errorf("Expected key to be deleted, but found value: %q", val)
	}

	// 5. Trigger compaction manually
	h := hashIndex(key)
	shard := db.getShard(h)
	shard.CompactSingleThreaded(&db)

	// 6. GET key again (should STILL be not found, not resurrected)
	if val, ok := db.get(key); ok {
		t.Errorf("BUG! Key was resurrected after compaction: %q", val)
	}
}

func TestStandardCompaction(t *testing.T) {
	var db DB
	db.Init()

	key := "compaction_test_key"
	val1 := "value_one"
	val2 := "value_two"

	// 1. SET key -> val1
	if err := db.set(key, val1); err != nil {
		t.Fatalf("Failed to set key to val1: %v", err)
	}

	// 2. SET key -> val2
	if err := db.set(key, val2); err != nil {
		t.Fatalf("Failed to set key to val2: %v", err)
	}

	// 3. Trigger compaction manually
	h := hashIndex(key)
	shard := db.getShard(h)
	shard.CompactSingleThreaded(&db)

	// 4. GET key (should return the latest value)
	if val, ok := db.get(key); !ok {
		t.Errorf("Expected key to exist, but not found")
	} else if val != val2 {
		t.Errorf("Expected latest value %q, but got %q", val2, val)
	}
}

func TestValidatorPanicsAndValidationError(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		isValError  bool
	}{
		{
			name:        "Valid SET",
			input:       "SET mykey myval\n",
			expectError: false,
		},
		{
			name:        "Valid GET",
			input:       "GET mykey\n",
			expectError: false,
		},
		{
			name:        "Valid DEL",
			input:       "DEL mykey\n",
			expectError: false,
		},
		{
			name:        "Valid STATS",
			input:       "STATS\n",
			expectError: false,
		},
		{
			name:        "Short SET command",
			input:       "SET mykey\n",
			expectError: true,
			isValError:  true,
		},
		{
			name:        "Short GET command (prevent panic)",
			input:       "GET\n",
			expectError: true,
			isValError:  true,
		},
		{
			name:        "Short DEL command (prevent panic)",
			input:       "DEL\n",
			expectError: true,
			isValError:  true,
		},
		{
			name:        "GET with too many arguments",
			input:       "GET key extra\n",
			expectError: true,
			isValError:  true,
		},
		{
			name:        "STATS with extra arguments",
			input:       "STATS extra\n",
			expectError: true,
			isValError:  true,
		},
		{
			name:        "Unknown command",
			input:       "UNKNOWN\n",
			expectError: true,
			isValError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure it doesn't panic
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Validator panicked: %v", r)
				}
			}()

			reader := bufio.NewReader(strings.NewReader(tt.input))
			_, err := validator(reader)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, but got nil")
				} else {
					_, isValidationError := err.(ValidationError)
					if tt.isValError && !isValidationError {
						t.Errorf("Expected error of type ValidationError, but got %T (%v)", err, err)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got: %v", err)
				}
			}
		})
	}
}

func TestValidatorNetworkError(t *testing.T) {
	// Simulate reader that returns immediately with EOF
	reader := bufio.NewReader(strings.NewReader(""))
	_, err := validator(reader)

	if err == nil {
		t.Fatalf("Expected error on empty reader, got nil")
	}

	if err != io.EOF {
		t.Errorf("Expected io.EOF on empty reader, got: %v", err)
	}

	// Make sure EOF is NOT a ValidationError
	if _, ok := err.(ValidationError); ok {
		t.Errorf("Expected socket EOF error to NOT be a ValidationError type")
	}
}
