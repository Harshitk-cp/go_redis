package datastore

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func (s *DataStore) Recover(snapshotPath string, aofPath string) error {

	if err := s.LoadSnapshotBinary(snapshotPath); err != nil {
		fmt.Println("No snapshot found or failed to load. Starting fresh.")
	} else {
		fmt.Println("Snapshot successfully loaded.")
	}

	file, err := os.Open(aofPath)
	if err != nil {
		fmt.Println("No AOF file found or failed to load. Continuing with snapshot.")
		return nil
	}
	defer file.Close()

	// Process each line in the AOF file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			fmt.Printf("Malformed AOF line: %s\n", line)
			continue
		}

		command := parts[0]
		dbIndex := 0
		key := parts[1]
		params := parts[2:]

		switch command {
		case "SET":
			if len(params) != 1 {
				fmt.Printf("Malformed SET command: %s\n", line)
				continue
			}
			s.SelectDatabase(dbIndex).Set(key, params[0])

		case "LPUSH", "RPUSH":

			if len(params) == 0 {
				fmt.Printf("Malformed %s command: %s\n", command, line)
				continue
			}
			list := s.SelectDatabase(dbIndex).GetList(key)
			if list == nil {
				list = s.SelectDatabase(dbIndex).CreateList(key)
			}
			if command == "LPUSH" {
				s.SelectDatabase(dbIndex).LPush(key, params)
			} else {
				s.SelectDatabase(dbIndex).RPush(key, params)
			}

		case "HSET":
			if len(params) < 2 || len(params)%2 != 0 {
				fmt.Printf("Malformed HSET command: %s\n", line)
				continue
			}
			hash := s.SelectDatabase(dbIndex).GetHash(key)
			if hash == nil {
				hash = s.SelectDatabase(dbIndex).CreateHash(key)
			}
			for i := 0; i < len(params); i += 2 {
				hash.Set(params[i], params[i+1])
			}

		case "SADD":
			if len(params) == 0 {
				fmt.Printf("Malformed SADD command: %s\n", line)
				continue
			}
			set := s.SelectDatabase(dbIndex).GetSet(key)
			if set == nil {
				set = s.SelectDatabase(dbIndex).CreateSet(key)
			}
			set.Add(params...)

		default:
			fmt.Printf("Unsupported command in AOF: %s\n", command)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading AOF file: %w", err)
	}

	return nil
}
