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

	query := "CREATE"

	// If a password is set, we're creating a user, otherwise we're creating a role/group
	if user.Password != "" {
		query += " USER"
	} else {
		query += " ROLE"
	}
	query += fmt.Sprintf(" %s", QuoteIdentifier(user.Name))

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

// updateUser updates the specified user.
func (m *postgresManager) updateUser(user User) error {
	// Check if the user already exists
	if exists, err := m.userExists(user.Name); err != nil {
		return err
	} else if !exists {
		log.Printf("User %s does not exist, skipping\n", user.Name)
		return nil
	}

	// Update the user
	query := fmt.Sprintf("ALTER USER %s", QuoteIdentifier(user.Name))

	if user.Password != "" {
		query += fmt.Sprintf(" WITH LOGIN PASSWORD '%s'", user.Password)
	}

	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	log.Printf("Updated user: %s\n", user.Name)

	return nil
}
