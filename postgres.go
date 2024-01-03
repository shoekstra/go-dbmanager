package dbmanager

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type postgresManager struct {
	databaseManager
}

// newPostgresManager creates a new PostgreSQL manager.
func newPostgresManager(options ...func(*Connection)) Manager {
	manager := &postgresManager{
		databaseManager: databaseManager{
			connection: Connection{
				Database: "postgres",
				Port:     "5432",
				SSLMode:  "disable",
			},
		},
	}
	manager.initialize(options...)
	return manager
}

// Connect connects to the PostgreSQL server.
func (m *postgresManager) Connect() error {
	log.Printf("Connecting to %s:%s as %s\n", m.connection.Host, m.connection.Port, m.connection.Username)

	db, err := sql.Open("pgx", m.connectionString(m.connection))
	if err != nil {
		return fmt.Errorf("error connecting to PostgreSQL database: %w", err)
	}

	m.db = db

	if err := m.db.Ping(); err != nil {
		return fmt.Errorf("error pinging PostgreSQL database: %w", err)
	}

	log.Printf("Connected to PostgreSQL database %s on %s", m.connection.Database, m.connection.Host)

	return nil
}

// connectionStrings returns a list of connection strings for the specified database.
func (m *postgresManager) connectionString(connection Connection) string {
	connectionString := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		connection.Host, connection.Port, connection.Username, connection.Database, connection.SSLMode)
	if m.connection.Password != "" {
		connectionString += fmt.Sprintf(" password=%s", m.connection.Password)
	}
	return connectionString
}

// Disconnect disconnects from the PostgreSQL server.
func (m *postgresManager) Disconnect() error {
	log.Println("Disconnecting...")

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("error closing connection to PostgreSQL database: %w", err)
	}

	return nil
}

// Manage manages the databases and users based on the provided options.
func (m *postgresManager) Manage(databases []Database, users []User) error {
	// Create users
	for _, user := range users {
		if err := m.CreateUser(user); err != nil {
			return err
		}
	}

	// Create databases
	for _, database := range databases {
		if err := m.CreateDatabase(database); err != nil {
			return err
		}
	}

	// Grant permissions
	for _, user := range users {
		if err := m.GrantPermissions(user); err != nil {
			return err
		}
	}

	return nil
}
