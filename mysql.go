package dbmanager

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlManager struct {
	databaseManager
}

// newMySQLManager creates a new MySQL manager.
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

// Connect connects to the MySQL server.
func (m *mysqlManager) Connect() error {
	log.Printf("Connecting to %s:%s as %s\n", m.connection.Host, m.connection.Port, m.connection.Username)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", m.connection.Username, m.connection.Password, m.connection.Host, m.connection.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	m.db = db

	return nil
}

// Disconnect disconnects from the MySQL server.
func (m *mysqlManager) Disconnect() error {
	log.Println("Disconnecting...")

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("failed to disconnect from MySQL: %w", err)
	}

	return nil
}

// Manage manages the databases and users based on the provided options.
func (m *mysqlManager) Manage(databases []Database, users []User) error {
	log.Println("Managing databases and users")

	for _, db := range databases {
		if err := m.CreateDatabase(db); err != nil {
			return err
		}
	}

	for _, user := range users {
		if err := m.CreateUser(user); err != nil {
			return err
		}
		if err := m.GrantPermissions(user); err != nil {
			return err
		}
	}

	return nil
}
