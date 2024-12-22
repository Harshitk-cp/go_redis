package datastore

import (
	"fmt"
	"os"
)

func (s *DataStore) Recover(filePath string, aofPath string) error {

	if err := s.LoadSnapshotBinary(filePath); err != nil {
		fmt.Println("No snapshot found or failed to load. Starting fresh.")
	} else {
		fmt.Println("Snapshot successfully loaded.")
	}

	file, err := os.Open(aofPath)
	if err != nil {
		return nil
	}
	defer file.Close()

	var dbIndex int
	var key, value, command string
	for {
		_, err := fmt.Fscanf(file, "%s %d %s %s\n", &command, &dbIndex, &key, &value)
		if err != nil {
			break
		}
		if command == "SET" {
			s.SelectDatabase(dbIndex).Set(key, value)
		}
	}
	return nil
}
