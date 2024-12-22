package datastore

import (
	"log"
	"sync"
	"time"
)

type Database struct {
	mu   sync.RWMutex
	data map[string]string
}

type DataStore struct {
	mu          sync.RWMutex
	databases   map[int]*Database
	rdbConfig   RDBConfig
	keyChangeCh chan struct{}
	aofLog      chan string
	aof         *AppendOnlyFile
}

func NewDataStore(numDB int) *DataStore {
	dataStore := &DataStore{
		databases: make(map[int]*Database),
		rdbConfig: RDBConfig{
			SaveInterval: 10 * time.Second,
			FilePath:     "snapshot.rdb",
		},
		aofLog:      make(chan string, 100),
		keyChangeCh: make(chan struct{}, 1),
	}

	for i := 0; i < numDB; i++ {
		dataStore.databases[i] = &Database{data: make(map[string]string)}
	}
	aof, err := NewAOF("appendonly.aof")
	if err != nil {
		log.Fatalf("Failed to open AOF file: %v", err)
	}

	dataStore.aof = aof

	go dataStore.WriteAOF()
	return dataStore
}

func (ds *DataStore) SelectDatabase(dbIndex int) *Database {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.databases[dbIndex]
}

func (db *Database) Set(key, value string) {
	log.Printf("Setting key: %s, value: %s", key, value)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value

}

func (db *Database) Get(key string) (interface{}, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, exists := db.data[key]
	return val, exists
}

func (db *Database) Delete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data, key)
}
