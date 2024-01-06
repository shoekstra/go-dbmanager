package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// CreateDatabase creates and updates a database. It will create the database if it doesn't already exist
// and apply the default privileges if provided.
func (m *postgresManager) CreateDatabase(database Database) error {
	// Create the database if it doesn't already exist
	if err := m.createDatabase(database); err != nil {
		return err
	}

	// Update the database
	if err := m.updateDatabase(database); err != nil {
		return err
	}

	// Apply default privileges
	if err := m.alterDefaultPrivileges(database.Name, database.DefaultPrivileges); err != nil {
		return err
	}

	return nil
}

// createDatabase creates a new database.
func (m *postgresManager) createDatabase(database Database) error {
	if exists, err := m.databaseExists(database.Name); err != nil {
		return err
	} else if exists {
		log.Printf("Database %s already exists, skipping\n", database.Name)
		return nil
	}

	query := fmt.Sprintf("CREATE DATABASE %s", database.Name)

	// Add owner if provided, if the owner is not provided then the current user will be the owner. If an
	// owner if provided we need to validate the user exists before creating the database.
	if database.Owner != "" {
		if exists, err := m.userExists(database.Owner); err != nil {
			return err
		} else if !exists {
			return fmt.Errorf("owner %s does not exist", database.Owner)
		}

		// RDS wants the user creating the database to be a member of the owner role, so we need to add the
		// our current user to the owner role before creating the database and then remove it after.
		if err := m.addRole(m.connection.Username, database.Owner); err != nil {
			return err
		}
		defer func() {
			if err := m.removeRole(m.connection.Username, database.Owner); err != nil {
				log.Printf("Error removing user %s from role %s: %v\n", m.connection.Username, database.Owner, err)
			}
		}()

		query += fmt.Sprintf(" OWNER %s", QuoteIdentifier(database.Owner))
	}

	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	log.Printf("Created database: %s\n", database.Name)

	return nil
}

// databaseExists checks if the specified database exists.
func (m *postgresManager) databaseExists(name string) (bool, error) {
	var exists bool
	query := "SELECT 1 FROM pg_database WHERE datname = $1"
	err := m.db.QueryRow(query, name).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}

// updateDatabase updates a database.
func (m *postgresManager) updateDatabase(database Database) error {
	// Update owner if provided
	if err := m.updateDatabaseOwner(database); err != nil {
		return err
	}

	// Update default privileges if provided
	if err := m.alterDefaultPrivileges(database.Name, database.DefaultPrivileges); err != nil {
		return err
	}

	return nil
}

// updateDatabaseOwner updates the owner of a database.
func (m *postgresManager) updateDatabaseOwner(database Database) error {
	// If an owner isn't set we won't try to update it
	if database.Owner == "" {
		return nil
	}

	currentOwner, err := m.getDatabaseOwner(database.Name)
	if err != nil {
		return err
	}

	var removeRoleErr error

	if currentOwner != database.Owner {
		// RDS wants the user creating the database to be a member of the owner role, so we need to add the
		// our current user to the owner role before creating the database and then remove it after.
		if err := m.addRole(m.connection.Username, database.Owner); err != nil {
			return err
		}
		defer func() {
			if removeRoleErr = m.removeRole(m.connection.Username, database.Owner); err != nil {
				log.Printf("Error removing user %s from role %s: %v\n", m.connection.Username, database.Owner, err)
			}
		}()

		query := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", database.Name, QuoteIdentifier(database.Owner))
		if _, err := m.db.Exec(query); err != nil {
			return err
		}
		log.Printf("Updated owner of database %s to %s\n", database.Name, database.Owner)
	}

	return removeRoleErr
}

// databaseOwner returns the owner of a database.
func (m *postgresManager) getDatabaseOwner(database string) (string, error) {
	var owner string
	query := fmt.Sprintf("SELECT pg_catalog.pg_get_userbyid(d.datdba) FROM pg_catalog.pg_database d WHERE d.datname = '%s'", database)
	if err := m.db.QueryRow(query).Scan(&owner); err != nil {
		return "", err
	}
	return owner, nil
}

// alterDefaultPrivileges alters the default privileges in a database for a user or role.
//
// This needs to be done in a separate connection to the database where the permissions are being granted
// and after the users or roles mentioned in the "To" field have been created or it will return an error.
func (m *postgresManager) alterDefaultPrivileges(database string, privileges []DefaultPrivilege) error {
	// Create new client using the database where permissions are being granted
	db := &postgresManager{
		databaseManager: databaseManager{
			connection: Connection{
				Host:     m.connection.Host,
				Database: database,
				Port:     m.connection.Port,
				Username: m.connection.Username,
				Password: m.connection.Password,
				SSLMode:  m.connection.SSLMode,
			},
		},
	}

	// Connect to the database
	if err := db.Connect(); err != nil {
		return err
	}
	defer db.Disconnect()

	var removeRoleErr error

	for _, privilege := range privileges {
		// RDS wants the user setting the default privilege to be a member of the role, so we need to add the
		// our current user to the role before settings the default privilege the database and removing it after.
		if privilege.Role != "" || privilege.Role != m.connection.Username {
			if err := m.addRole(m.connection.Username, privilege.Role); err != nil {
				log.Printf("Error adding user %s to role %s: %v\n", m.connection.Username, privilege.Role, err)
			}
			defer func() {
				if removeRoleErr = m.removeRole(m.connection.Username, privilege.Role); removeRoleErr != nil {
					log.Printf("Error removing user %s from role %s: %v\n", m.connection.Username, privilege.Role, removeRoleErr)
				}
			}()
		}

		query := m.alterDefaultPrivilegeQuery(database, privilege)
		log.Printf("Altering default permissions in database %s: %s", database, query)
		if _, err := db.db.Exec(query); err != nil {
			return fmt.Errorf("error altering default privilege: %w", err)
		}
	}

	log.Printf("Applied default privileges for database %s\n", database)

	return removeRoleErr
}

// alterDefaultPrivilege alters the default privileges in a database for a user or role.
func (m *postgresManager) alterDefaultPrivilegeQuery(database string, privilege DefaultPrivilege) string {
	query := "ALTER DEFAULT PRIVILEGES"
	if privilege.Role != "" {
		query += fmt.Sprintf(" FOR ROLE %s", QuoteIdentifier(privilege.Role))
	}
	query += fmt.Sprintf(" IN SCHEMA %s GRANT %s ON %s TO %s", QuoteIdentifier(privilege.Schema), strings.Join(privilege.Grant, ", "), privilege.On, QuoteIdentifier(privilege.To))
	if privilege.WithGrant {
		query += " WITH GRANT OPTION"
	}
	return query
}
