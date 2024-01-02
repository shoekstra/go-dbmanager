package dbmanager

import "fmt"

// Manager is the main interface for managing database servers
type Manager interface {
	Connector
	CreateDatabase(databaseConfig Database) error
	CreateUser(userConfig User) error
}

// databaseManager is the internal implementation of the Manager interface
type databaseManager struct {
	connection Connection
}

// initialize initializes the database manager connection with the provided options.
func (m *databaseManager) initialize(options ...func(*Connection)) {
	for _, option := range options {
		option(&m.connection)
	}
}

// Database represents the configuration for creating a database
type Database struct {
	Name string `json:"name"`
}

// User represents the configuration for creating a user
type User struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

// New creates a new Manager instance based on the provided engine.
func New(engine string, options ...func(*Connection)) (Manager, error) {
	switch engine {
	case "mysql":
		return newMySQLManager(options...), nil
	case "postgres":
		return newPostgresManager(options...), nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", engine)
	}
}
