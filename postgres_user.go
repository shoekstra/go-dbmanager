package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
)

// CreateUser creates and manages a user. It will create the user if it doesn't already exist.
func (m *postgresManager) CreateUser(user User) error {
	if exists, err := m.userExists(user.Name); err != nil {
		return err
	} else if !exists {
		if err := m.createUser(user); err != nil {
			return err
		}
		log.Printf("Created user: %s\n", user.Name)
	}

	// We can't read back the user's password, so if one is set, we'll just set it again
	if user.Password != "" {
		if err := m.setPassword(user.Name, user.Password); err != nil {
			return err
		}
	}

	return nil
}

// createUser creates a new user.
func (m *postgresManager) createUser(user User) error {
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
	err := m.db.QueryRow(query, name).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}

// setPassword sets the password for the specified user.
func (m *postgresManager) setPassword(name, password string) error {
	query := fmt.Sprintf("ALTER USER %s WITH LOGIN PASSWORD '%s'", QuoteIdentifier(name), password)
	if _, err := m.db.Exec(query); err != nil {
		return err
	}
	return nil
}
