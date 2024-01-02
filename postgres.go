package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
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
				SSLMode:  "disable",
			},
		},
	}
	manager.initialize(options...)
	return manager
}

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

func (m *postgresManager) Disconnect() error {
	fmt.Println("Disconnecting...")

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("error closing connection to PostgreSQL database: %w", err)
	}

	return nil
}

func (m *postgresManager) CreateDatabase(database Database) error {
	log.Printf("Creating database: %s\n", database.Name)

	// Check if the database already exists
	if exists, err := m.DatabaseExists(database.Name); err != nil {
		return err
	} else if exists {
		log.Printf("Database %s already exists, skipping\n", database.Name)
		return nil
	}

	// Create the database
	query := fmt.Sprintf("CREATE DATABASE %s", database.Name)

	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	log.Printf("Created database %s\n", database.Name)

	return nil
}

// DatabaseExists checks if the specified database exists.
func (m *postgresManager) DatabaseExists(name string) (bool, error) {
	var exists bool
	query := "SELECT 1 FROM pg_database WHERE datname = $1"
	err := m.db.QueryRow(query, name).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}

func (m *postgresManager) CreateUser(user User) error {
	log.Printf("Creating user: %s\n", user.Name)

	// Check if the user already exists
	if exists, err := m.UserExists(user.Name); err != nil {
		return err
	} else if exists {
		log.Printf("User %s already exists, skipping\n", user.Name)
		return nil
	}

	// Create the user
	query := fmt.Sprintf("CREATE USER %s", QuoteIdentifier(user.Name))

	if user.Password != "" {
		query += fmt.Sprintf(" WITH LOGIN PASSWORD '%s'", user.Password)
	}

	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	return nil
}

// UserExists checks if the specified user exists.
func (m *postgresManager) UserExists(name string) (bool, error) {
	var exists bool
	query := "SELECT 1 FROM pg_roles WHERE rolname = $1 LIMIT 1"
	err := m.db.QueryRow(query, strings.ToLower(name)).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
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
