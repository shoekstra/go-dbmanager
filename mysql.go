package dbmanager

import (
	"fmt"
	"log"
)

type mysqlManager struct {
	databaseManager
}

func newMySQLManager(options ...func(*Connection)) Manager {
	manager := &mysqlManager{
		databaseManager: databaseManager{
			connection: Connection{
				Port: "3306",
			},
		},
	}
	manager.initialize(options...)
	return manager
}

func (m *mysqlManager) Connect() error {
	log.Printf("Connecting to %s:%s as %s\n", m.connection.Host, m.connection.Port, m.connection.Username)
	// Additional MySQL specific logic for establishing a connection
	return nil
}

func (m *mysqlManager) Disconnect() error {
	fmt.Println("Disconnecting...")
	// Additional MySQL specific logic for disconnecting
	return nil
}

func (m *mysqlManager) CreateDatabase(database Database) error {
	log.Printf("Creating database: %s\n", database.Name)
	// Additional MySQL specific logic for creating a database
	return nil
}

func (m *mysqlManager) CreateUser(user User) error {
	log.Printf("Creating user: %s\n", user.Name)
	// Additional MySQL specific logic for creating a user
	return nil
}
