package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// CreateUser creates and manages a user. It will create the user if it doesn't already exist.
func (m *postgresManager) CreateUser(user User) error {
	// Check if the user already exists
	if exists, err := m.userExists(user.Name); err != nil {
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

	log.Printf("Created user: %s\n", user.Name)

	return nil
}

// userExists checks if the specified user exists.
func (m *postgresManager) userExists(name string) (bool, error) {
	var exists bool
	query := "SELECT 1 FROM pg_roles WHERE rolname = $1 LIMIT 1"
	err := m.db.QueryRow(query, strings.ToLower(name)).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}
