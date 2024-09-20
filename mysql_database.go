package dbmanager

import (
	"database/sql"
	"fmt"
)

// CreateDatabase creates a database based on the provided Database options.
func (m *mysqlManager) CreateDatabase(database Database) error {
	// Create the database if it doesn't exist
	exists, err := m.databaseExists(database.Name)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	if err := m.createDatabase(database); err != nil {
		return err
	}

	return nil
}

// createDatabase creates a new database.
func (m *mysqlManager) createDatabase(database Database) error {
	_, err := m.db.Exec(fmt.Sprintf("CREATE DATABASE %s", database.Name))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// databaseExists checks if a database exists.
func (m *mysqlManager) databaseExists(name string) (bool, error) {
	var dbName string
	err := m.db.QueryRow("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&dbName)
	if err != nil {
		if err == sql.ErrNoRows {
			// No database found, return false without error
			return false, nil
		}
		// Other errors should be returned
		return false, fmt.Errorf("failed to check if database exists: %w", err)
	}

	// If we reach here, it means the user exists
	return true, nil
}
