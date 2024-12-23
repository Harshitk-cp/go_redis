package datastore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
)

type Snapshot struct {
	ID        string
	Timestamp time.Time
	Data      map[string]interface{}
}

type RDBConfig struct {
	SaveInterval time.Duration
	//TODO: Add saveinterval field
	FilePath string
}

func NewSnapshot(id string, data map[string]interface{}) *Snapshot {
	return &Snapshot{
		ID:        id,
		Timestamp: time.Now(),
		Data:      data,
	}
}

func (s *DataStore) SaveSnapshotBinary(filePath string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := new(bytes.Buffer)

	for dbIndex, db := range s.databases {
		for key, value := range db.data {

			if err := binary.Write(buffer, binary.LittleEndian, int32(dbIndex)); err != nil {
				return err
			}

			keyBytes := []byte(key)
			if err := binary.Write(buffer, binary.LittleEndian, int32(len(keyBytes))); err != nil {
				return err
			}
			if _, err := buffer.Write(keyBytes); err != nil {
				return err
			}

			valueBytes := []byte(value)
			if err := binary.Write(buffer, binary.LittleEndian, int32(len(valueBytes))); err != nil {
				return err
			}
			if _, err := buffer.Write(valueBytes); err != nil {
				return err
			}
		}
	}

	_, err = file.Write(buffer.Bytes())
	return err
}

func (s *DataStore) LoadSnapshotBinary(filePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var dbIndex int32
	var keyLen, valueLen int32

	for {
		if err := binary.Read(file, binary.LittleEndian, &dbIndex); err != nil {
			break
		}

		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := file.Read(keyBytes); err != nil {
			return err
		}

		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			return err
		}
		valueBytes := make([]byte, valueLen)
		if _, err := file.Read(valueBytes); err != nil {
			return err
		}

		key := string(keyBytes)
		value := string(valueBytes)

		db := s.databases[int(dbIndex)]
		db.Set(key, value)
	}
	return nil
}

func (ds *DataStore) StartSnapshotRoutine() {
	ticker := time.NewTicker(ds.rdbConfig.SaveInterval)
	go func() {
		for range ticker.C {
			data := make(map[string]interface{})
			for k, v := range ds.databases {
				data[fmt.Sprintf("%d", k)] = v
			}
			NewSnapshot("snapshot_id", data)
			if err := ds.SaveSnapshotBinary(ds.rdbConfig.FilePath); err != nil {
				log.Printf("Error saving snapshot: %v", err)
			}
		}
	}()
}

func (s *DataStore) NotifyKeyChange() {
	select {
	case s.keyChangeCh <- struct{}{}:
	default:
	}
}

func (ds *DataStore) UpdateRDBConfig(interval time.Duration, filePath string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.rdbConfig.SaveInterval = interval
	if filePath != "" {
		ds.rdbConfig.FilePath = filePath
	}
}
