package datastore

type ConnectionContext struct {
	DataStore *DataStore
	CurrentDB int
}

func NewConnection() *ConnectionContext {
	return &ConnectionContext{
		DataStore: NewDataStore(16),
		CurrentDB: 0,
	}
}
