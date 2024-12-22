package datastore

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type AppendOnlyFile struct {
	mu   sync.Mutex
	file *os.File
}

func NewAOF(filePath string) (*AppendOnlyFile, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyFile{file: file}, nil
}

func (aof *AppendOnlyFile) Write(command string) error {
	log.Printf("Writing command to AOF: %s", command)
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.WriteString(command + "\n")
	return err
}

func (aof *AppendOnlyFile) Close() error {
	return aof.file.Close()
}

func (s *DataStore) LogCommand(command string) {
	select {
	case s.aofLog <- command:
	default:
		log.Println("AOF log channel is full, dropping command:", command)
	}
}

func (db *Database) SetWithLogging(store *DataStore, key, value string, dbIndex int) {
	db.Set(key, value)
	// store.NotifyKeyChange()
	command := fmt.Sprintf("SET %d %s %s", dbIndex, key, value)
	store.LogCommand(command)
}

func (s *DataStore) WriteAOF() {
	for command := range s.aofLog {
		err := s.aof.Write(command)
		if err != nil {
			log.Printf("Error writing AOF command: %v", err)
		}
	}
}
