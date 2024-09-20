package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
)

// CreateUser creates a user based on the provided User options.
func (m *mysqlManager) CreateUser(user User) error {
	// If the user already exists, we'll update it, otherwise we'll create it
	exists, err := m.userExists(user.Name)
	if err != nil {
		return err
	}

	if !exists {
		if err := m.createUser(user); err != nil {
			return err
		}
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
func (m *mysqlManager) createUser(user User) error {
	log.Printf("Creating user: %s\n", user.Name)

	_, err := m.db.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user.Name, user.Password))
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

func (m *mysqlManager) setPassword(name, password string) error {
	log.Printf("Setting password for user: %s\n", name)

	query := fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'", name, password)
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to set password: %w", err)
	}

	return nil
}

func (m *mysqlManager) userExists(name string) (bool, error) {
	var user string
	err := m.db.QueryRow("SELECT User FROM mysql.user WHERE User = ?", name).Scan(&user)
	if err != nil {
		if err == sql.ErrNoRows {
			// No user found, return false without error
			return false, nil
		}
		// Other errors should be returned
		return false, fmt.Errorf("failed to check if user exists: %w", err)
	}

	// If we reach here, it means the user exists
	return true, nil
}
