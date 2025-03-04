package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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
		db.mu.RLock()

		// Save strings
		for key, value := range db.strings {
			if err := writeEntry(buffer, dbIndex, "string", key, value); err != nil {
				return err
			}
		}

		// Save lists
		for key, list := range db.lists {
			if err := writeEntry(buffer, dbIndex, "list", key, list); err != nil {
				return err
			}
		}

		// Save hashes
		for key, hash := range db.hashes {
			if err := writeEntry(buffer, dbIndex, "hash", key, hash); err != nil {
				return err
			}
		}

		// Save sets
		for key, set := range db.sets {
			members := make([]string, 0, len(set))
			for member := range set {
				members = append(members, member)
			}
			if err := writeEntry(buffer, dbIndex, "set", key, members); err != nil {
				return err
			}
		}

		// Save sorted sets
		for key, sortedSet := range db.sortedSets {
			if err := writeEntry(buffer, dbIndex, "sortedSet", key, sortedSet); err != nil {
				return err
			}
		}

		db.mu.RUnlock()
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
	var keyLen, dataTypeLen, valueLen int32

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

		if err := binary.Read(file, binary.LittleEndian, &dataTypeLen); err != nil {
			return err
		}
		dataTypeBytes := make([]byte, dataTypeLen)
		if _, err := file.Read(dataTypeBytes); err != nil {
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
		dataType := string(dataTypeBytes)
		value, err := deserializeValue(dataType, valueBytes)
		if err != nil {
			return err
		}

		db := s.databases[int(dbIndex)]
		db.mu.Lock()
		switch dataType {
		case "string":
			db.strings[key] = value.(string)
		case "list":
			db.lists[key] = value.([]string)
		case "hash":
			db.hashes[key] = value.(map[string]string)
		case "set":
			set := make(map[string]struct{})
			for _, member := range value.([]string) {
				set[member] = struct{}{}
			}
			db.sets[key] = set
		case "sortedSet":
			db.sortedSets[key] = value.(map[string]float64)
		}
		db.mu.Unlock()
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

func writeEntry(buffer *bytes.Buffer, dbIndex int, dataType, key string, value interface{}) error {
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

	dataTypeBytes := []byte(dataType)
	if err := binary.Write(buffer, binary.LittleEndian, int32(len(dataTypeBytes))); err != nil {
		return err
	}
	if _, err := buffer.Write(dataTypeBytes); err != nil {
		return err
	}

	valueBytes, err := serializeValue(value)
	if err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(len(valueBytes))); err != nil {
		return err
	}
	_, err = buffer.Write(valueBytes)
	return err
}

func serializeValue(value interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(value)
	return buffer.Bytes(), err
}

func deserializeValue(dataType string, data []byte) (interface{}, error) {
	buffer := bytes.NewReader(data)
	decoder := gob.NewDecoder(buffer)
	var value interface{}
	switch dataType {
	case "string":
		value = ""
	case "list":
		value = []string{}
	case "hash":
		value = map[string]string{}
	case "set":
		value = []string{}
	case "sortedSet":
		value = map[string]float64{}
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType)
	}
	err := decoder.Decode(&value)
	return value, err
}
