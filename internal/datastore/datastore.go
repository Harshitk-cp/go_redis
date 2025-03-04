package datastore

import (
	"log"
	"sort"
	"sync"
	"time"
)

type List []string
type Hash map[string]string
type Set map[string]struct{}
type SortedSet map[string]float64

type Database struct {
	mu         sync.RWMutex
	strings    map[string]string
	lists      map[string]List
	hashes     map[string]Hash
	sets       map[string]Set
	sortedSets map[string]SortedSet
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
		dataStore.databases[i] = &Database{
			strings:    make(map[string]string),
			lists:      make(map[string]List),
			hashes:     make(map[string]Hash),
			sets:       make(map[string]Set),
			sortedSets: make(map[string]SortedSet),
		}
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

// String operations
func (db *Database) Set(key, value string) {
	log.Printf("Pushing values: %v to list with key: %s", value, key)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.strings[key] = value
}

func (db *Database) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, exists := db.strings[key]
	return val, exists
}

func (db *Database) Delete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.strings, key)
	delete(db.lists, key)
	delete(db.hashes, key)
	delete(db.sets, key)
	delete(db.sortedSets, key)
}

// List operations
func (db *Database) LPush(key string, values []string) {
	log.Printf("Pushing values: %v to list with key: %s", values, key)
	db.mu.Lock()
	defer db.mu.Unlock()
	list, exists := db.lists[key]
	if !exists {
		list = []string{}
	}
	db.lists[key] = append(values, list...)
}

func (db *Database) RPush(key string, values []string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	list, exists := db.lists[key]
	if !exists {
		list = []string{}
	}
	db.lists[key] = append(list, values...)
}

func (db *Database) LPop(key string) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	list, exists := db.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}
	value := list[0]
	db.lists[key] = list[1:]
	return value, true
}

func (db *Database) RPop(key string) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	list, exists := db.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}
	value := list[len(list)-1]
	db.lists[key] = list[:len(list)-1]
	return value, true
}

func (db *Database) LRange(key string, start, stop int) ([]string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	list, exists := db.lists[key]
	if !exists {
		return nil, false
	}

	// Normalize start and stop
	if start < 0 {
		start = len(list) + start
	}
	if stop < 0 {
		stop = len(list) + stop
	}
	if start > len(list) {
		start = len(list)
	}
	if stop >= len(list) {
		stop = len(list) - 1
	}

	return list[start : stop+1], true
}

// Hash operations
func (db *Database) HSet(key, field, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.hashes[key]; !exists {
		db.hashes[key] = make(map[string]string)
	}
	db.hashes[key][field] = value
}

func (db *Database) HGet(key, field string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if hash, exists := db.hashes[key]; exists {
		val, exists := hash[field]
		return val, exists
	}
	return "", false
}

func (db *Database) HDel(key string, fields ...string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if hash, exists := db.hashes[key]; exists {
		for _, field := range fields {
			delete(hash, field)
		}
	}
}

// Set operations
func (db *Database) SAdd(key string, members ...string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.sets[key]; !exists {
		db.sets[key] = make(map[string]struct{})
	}
	for _, member := range members {
		db.sets[key][member] = struct{}{}
	}
}

func (db *Database) SRem(key string, members ...string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if set, exists := db.sets[key]; exists {
		for _, member := range members {
			delete(set, member)
		}
	}
}

func (db *Database) SMembers(key string) ([]string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if set, exists := db.sets[key]; exists {
		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}
		return members, true
	}
	return nil, false
}

// Sorted Set operations
func (db *Database) ZAdd(key string, score float64, member string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.sortedSets[key]; !exists {
		db.sortedSets[key] = make(map[string]float64)
	}
	db.sortedSets[key][member] = score
}

func (db *Database) ZRem(key string, members ...string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if sortedSet, exists := db.sortedSets[key]; exists {
		for _, member := range members {
			delete(sortedSet, member)
		}
	}
}

func (db *Database) ZRange(key string, start, stop int) ([]string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if sortedSet, exists := db.sortedSets[key]; exists {
		// Convert map to slice of pairs
		type pair struct {
			member string
			score  float64
		}
		pairs := make([]pair, 0, len(sortedSet))
		for member, score := range sortedSet {
			pairs = append(pairs, pair{member, score})
		}

		// Sort pairs by score
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].score < pairs[j].score
		})

		// Normalize start and stop
		if start < 0 {
			start = len(pairs) + start
		}
		if stop < 0 {
			stop = len(pairs) + stop
		}
		if start > len(pairs) {
			start = len(pairs)
		}
		if stop >= len(pairs) {
			stop = len(pairs) - 1
		}

		// Extract members in range
		members := make([]string, 0, stop-start+1)
		for i := start; i <= stop; i++ {
			members = append(members, pairs[i].member)
		}
		return members, true
	}
	return nil, false
}

func (db *Database) GetList(key string) *List {
	list := db.lists[key]
	return &list
}

// CreateList creates a new list with the given key

func (db *Database) CreateList(key string) *List {

	list := &List{}

	db.lists[key] = *list

	return list

}

func (l *List) LPush(values ...string) {
	log.Printf("Pushing values: %v", values)
	*l = append(values, *l...)
}

func (l *List) RPush(values ...string) {
	*l = append(*l, values...)
}

func (db *Database) GetHash(key string) *Hash {
	if hash, exists := db.hashes[key]; exists {
		return &hash
	}
	return nil
}

func (db *Database) CreateHash(key string) *Hash {
	hash := make(Hash)
	db.hashes[key] = hash
	return &hash
}

func (h *Hash) Set(field string, value string) {
	(*h)[field] = value
}

func (db *Database) GetSet(key string) *Set {
	if set, exists := db.sets[key]; exists {
		return &set
	}
	return nil
}

func (db *Database) CreateSet(key string) *Set {
	set := make(Set)
	db.sets[key] = set
	return &set
}

func (s *Set) Add(members ...string) {
	for _, member := range members {
		(*s)[member] = struct{}{}
	}
}
