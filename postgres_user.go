package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// CreateUser creates and manages a user. It will create the user if it doesn't already exist.
func (m *postgresManager) CreateUser(user User) error {

	// If the user already exists, we'll update it, otherwise we'll create it
	exists, err := m.userExists(user.Name)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := m.createUser(user); err != nil {
			return err
		}
	} else {
		if _, err := m.updateUser(user); err != nil {
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
func (m *postgresManager) createUser(user User) (bool, error) {
	query := "CREATE"

	// If the user has a password or explicitly set login to true, we'll create a user, otherwise we'll create a role
	if user.Options.Login || user.Password != "" {
		query += " USER"
	} else {
		query += " ROLE"
	}
	query += fmt.Sprintf(" %s", QuoteIdentifier(user.Name))

	addOption := func(option string) {
		if strings.HasSuffix(query, QuoteIdentifier(user.Name)) {
			query += " WITH"
		}
		query += " " + option
	}

	if user.Password != "" {
		addOption(fmt.Sprintf("LOGIN PASSWORD '%s'", user.Password))
	}

	if user.Options.Superuser {
		addOption("SUPERUSER")
	}

	if user.Options.CreateRole {
		addOption("CREATEROLE")
	}

	if user.Options.CreateDatabase {
		addOption("CREATEDB")
	}

	if user.Options.Inherit {
		addOption("INHERIT")
	}

	if user.Options.Replication {
		addOption("REPLICATION")
	}

	if user.Options.BypassRLS {
		addOption("BYPASSRLS")
	}

	if _, err := m.db.Exec(query); err != nil {
		return false, err
	}

	log.Printf("Created user: %s\n", user.Name)

	return true, nil
}

// getUser returns the user with the specified name.
func (m *postgresManager) getUser(name string) (User, error) {
	var user User
	query := "SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolcanlogin, rolinherit, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = $1"
	err := m.db.QueryRow(query, name).Scan(&user.Name, &user.Options.Superuser, &user.Options.CreateRole, &user.Options.CreateDatabase, &user.Options.Login, &user.Options.Inherit, &user.Options.Replication, &user.Options.BypassRLS)
	if err != nil {
		return User{}, err
	}
	return user, nil
}

// setPassword sets the password for the specified user.
func (m *postgresManager) setPassword(name, password string) error {
	query := fmt.Sprintf("ALTER USER %s WITH LOGIN PASSWORD '%s'", QuoteIdentifier(name), password)
	if _, err := m.db.Exec(query); err != nil {
		return err
	}
	return nil
}

// updateUser updates the specified user.
func (m *postgresManager) updateUser(user User) (bool, error) {
	query := fmt.Sprintf("ALTER USER %s", QuoteIdentifier(user.Name))

	addOption := func(option string) {
		if strings.HasSuffix(query, QuoteIdentifier(user.Name)) {
			query += " WITH"
		}
		query += " " + option
	}

	// Compare with real user
	realUser, err := m.getUser(user.Name)
	if err != nil {
		return false, err
	}

	if user.Options.Superuser != realUser.Options.Superuser {
		if user.Options.Superuser {
			addOption("SUPERUSER")
		} else {
			addOption("NOSUPERUSER")
		}
	}

	if user.Options.CreateRole != realUser.Options.CreateRole {
		if user.Options.CreateRole {
			addOption("CREATEROLE")
		} else {
			addOption("NOCREATEROLE")
		}
	}

	if user.Options.CreateDatabase != realUser.Options.CreateDatabase {
		if user.Options.CreateDatabase {
			addOption("CREATEDB")
		} else {
			addOption("NOCREATEDB")
		}
	}

	if user.Options.Login != realUser.Options.Login {
		if user.Options.Login {
			addOption("LOGIN")
		} else {
			addOption("NOLOGIN")
		}
	}

	if user.Options.Inherit != realUser.Options.Inherit {
		if user.Options.Inherit {
			addOption("INHERIT")
		} else {
			addOption("NOINHERIT")
		}
	}

	if user.Options.Replication != realUser.Options.Replication {
		if user.Options.Replication {
			addOption("REPLICATION")
		} else {
			addOption("NOREPLICATION")
		}
	}

	if user.Options.BypassRLS != realUser.Options.BypassRLS {
		if user.Options.BypassRLS {
			addOption("BYPASSRLS")
		} else {
			addOption("NOBYPASSRLS")
		}
	}

	if _, err := m.db.Exec(query); err != nil {
		return false, err
	}

	log.Printf("Updated user: %s\n", user.Name)

	return true, nil
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
