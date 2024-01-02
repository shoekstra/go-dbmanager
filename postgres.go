package dbmanager

import (
	"fmt"
	"log"
)

type postgresManager struct {
	databaseManager
}

func newPostgresManager(options ...func(*Connection)) Manager {
	manager := &postgresManager{
		databaseManager: databaseManager{
			connection: Connection{
				Database: "postgres",
				Port:     "5432",
			},
		},
	}
	manager.initialize(options...)
	return manager
}

func (m *postgresManager) Connect() error {
	log.Printf("Connecting to %s:%d as %s\n", m.connection.Host, m.connection.Port, m.connection.Username)
	// Additional Postgres specific logic for establishing a connection
	return nil
}

func (m *postgresManager) Disconnect() error {
	fmt.Println("Disconnecting...")
	// Additional Postgres specific logic for disconnecting
	return nil
}

func (m *postgresManager) CreateDatabase(database Database) error {
	log.Printf("Creating database: %s\n", database.Name)
	// Additional Postgres specific logic for creating a database
	return nil
}

func (m *postgresManager) CreateUser(user User) error {
	log.Printf("Creating user: %s\n", user.Name)
	// Additional Postgres specific logic for creating a user
	return nil
}
