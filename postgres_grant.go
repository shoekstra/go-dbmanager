package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// GrantPermissions grants permissions to a user based on the provided Grant options.
func (m *postgresManager) GrantPermissions(user User) error {
	// Check if the user exists
	if exists, err := m.userExists(user.Name); err != nil {
		return err
	} else if !exists {
		log.Printf("User %s does not exist, skipping\n", user.Name)
		return nil
	}

	// Grant permissions
	for _, grant := range user.Grants {
		log.Printf("Processing grant: %v", grant)

		if err := m.grantPermission(user.Name, grant); err != nil {
			return fmt.Errorf("error granting permissions: %w", err)
		}
	}

	// Add to roles
	for _, role := range user.Roles {
		if err := m.addRole(user.Name, role); err != nil {
			return fmt.Errorf("error adding user to role: %w", err)
		}
	}

	return nil
}

// addRole adds a user to a role.
func (m *postgresManager) addRole(username, role string) error {
	// Check if the user already has the role
	if hasRole, err := m.hasRole(username, role); err != nil {
		return err
	} else if hasRole {
		log.Printf("User %s already has role %s, skipping\n", username, role)
		return nil
	}

	// Add the user to the role
	query := fmt.Sprintf("GRANT %s TO %s", QuoteIdentifier(role), QuoteIdentifier(username))
	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	log.Printf("Added user %s to role %s\n", username, role)

	return nil
}

// hasRole checks if the specified user has the specified role.
func (m *postgresManager) hasRole(user, role string) (bool, error) {
	var exists bool
	query := "SELECT 1 FROM pg_roles r JOIN pg_auth_members m ON r.oid = m.roleid JOIN pg_roles u ON m.member = u.oid WHERE r.rolname = $1 AND u.rolname = $2"
	err := m.db.QueryRow(query, strings.ToLower(role), strings.ToLower(user)).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}

// grantPermission grants a single permission to a user.
func (m *postgresManager) grantPermission(username string, grant Grant) error {
	var query string

	// Create new client using the database where permissions are being granted,
	// we also use this client to check if the user already has the permissions
	db := &postgresManager{
		databaseManager: databaseManager{
			connection: Connection{
				Host:     m.connection.Host,
				Database: grant.Database,
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

	// Construct the grant query based on the provided options
	if grant.Database != "" && grant.Schema == "" {
		if hasPermissions, err := db.hasDatabasePrivilege(username, grant.Database, grant.Privileges); err != nil {
			return err
		} else if hasPermissions {
			log.Printf("User %s already has permissions on database %s, skipping\n", username, grant.Database)
			return nil
		}

		query = m.grantDatabasePermissionQuery(username, grant)
	} else if grant.Database != "" && grant.Schema != "" {
		if grant.Table != "" {
			if hasPermissions, err := db.hasTablePrivilege(username, grant.Schema, grant.Table, grant.Privileges); err != nil {
				return err
			} else if hasPermissions {
				log.Printf("User %s already has permissions on table %s in database %s, skipping\n", username, grant.Table, grant.Database)
				return nil
			}
		} else if grant.Sequence != "" {
			if hasPermissions, err := db.hasSequencePrivilege(username, grant.Schema, grant.Sequence, grant.Privileges); err != nil {
				return err
			} else if hasPermissions {
				log.Printf("User %s already has permissions on sequence %s in database %s, skipping\n", username, grant.Table, grant.Database)
				return nil
			}
		} else {
			if hasPermissions, err := db.hasSchemaPrivilege(username, grant.Schema, grant.Privileges); err != nil {
				return err
			} else if hasPermissions {
				log.Printf("User %s already has permissions on schema %s in database %s, skipping\n", username, grant.Table, grant.Database)
				return nil
			}
		}

		query = m.grantSchemaPermissionQuery(username, grant)
	} else {
		return fmt.Errorf("invalid grant options")
	}

	// Execute the grant query
	if _, err := db.db.Exec(query); err != nil {
		return err
	}

	return nil
}

// hasDatabasePrivilege checks if a user has the specified privileges on a database.
func (m *postgresManager) hasDatabasePrivilege(username, database string, privileges []string) (bool, error) {
	if privileges[0] == "ALL" {
		privileges = []string{"CREATE", "CONNECT", "TEMPORARY", "TEMP"}
	}
	query := fmt.Sprintf("SELECT has_database_privilege('%s', '%s', '%s')",
		username, database, strings.Join(privileges, ", "))
	var hasPermission bool
	if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
		return false, err
	}
	return hasPermission, nil
}

// hasTablePrivilege checks if a user has the specified privileges on a table.
func (m *postgresManager) hasTablePrivilege(username, schema, table string, privileges []string) (bool, error) {
	// We can't check privileges using has_table_privilege if the table is a wildcard
	// because it will return an error, so we'll just return false and let the grantPermission
	// function reapply the permissions.
	if table == "*" {
		return false, nil
	}

	if privileges[0] == "ALL" {
		privileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	}
	query := fmt.Sprintf("SELECT has_table_privilege('%s', '%s.%s', '%s')",
		username, schema, table, strings.Join(privileges, ", "))
	var hasPermission bool
	if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
		return false, err
	}
	return hasPermission, nil
}

// hasSequencePrivilege checks if a user has the specified privileges on a sequence.
func (m *postgresManager) hasSequencePrivilege(username, schema, sequence string, privileges []string) (bool, error) {
	// We can't check privileges using has_sequence_privilege if the sequence is a wildcard
	// because it will return an error, so we'll just return false and let the grantPermission
	// function reapply the permissions.
	if sequence == "*" {
		return false, nil
	}
	if privileges[0] == "ALL" {
		privileges = []string{"SELECT", "UPDATE"}
	}
	query := fmt.Sprintf("SELECT has_sequence_privilege('%s', '%s.%s', '%s')",
		username, schema, sequence, strings.Join(privileges, ", "))
	var hasPermission bool
	if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
		return false, err
	}
	return hasPermission, nil
}

// hasSchemaPrivilege checks if a user has the specified privileges on a schema.
func (m *postgresManager) hasSchemaPrivilege(username, schema string, privileges []string) (bool, error) {
	if privileges[0] == "ALL" {
		privileges = []string{"CREATE", "USAGE"}
	}
	query := fmt.Sprintf("SELECT has_schema_privilege('%s', '%s', '%s')",
		username, schema, strings.Join(privileges, ", "))
	var hasPermission bool
	if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
		return false, err
	}
	return hasPermission, nil
}

// grantDatabasePermission grants a permission on a database to a user.
func (m *postgresManager) grantDatabasePermissionQuery(username string, grant Grant) string {
	log.Printf("Granting %s permission to %s database", username, grant.Database)
	query := fmt.Sprintf("GRANT %s ON DATABASE %s TO %s", strings.Join(grant.Privileges, ", "), QuoteIdentifier(grant.Database), QuoteIdentifier(username))
	// Add WITH GRANT OPTION if GrantOption is true
	if grant.WithGrant {
		query += " WITH GRANT OPTION"
	}
	return query
}

// grantSchemaPermission grants a permission on a schema to a user.
func (m *postgresManager) grantSchemaPermissionQuery(username string, grant Grant) string {
	query := fmt.Sprintf("GRANT %s ON", strings.Join(grant.Privileges, ", "))

	switch {
	case grant.Sequence == "*":
		log.Printf("Granting permissions to all sequences in schema %s", grant.Schema)
		query += fmt.Sprintf(" ALL SEQUENCES IN SCHEMA %s", QuoteIdentifier(grant.Schema))

	case grant.Sequence != "":
		log.Printf("Granting permissions to sequence in schema %s", grant.Schema)
		query += fmt.Sprintf(" SEQUENCE %s.%s", QuoteIdentifier(grant.Schema), QuoteIdentifier(grant.Sequence))

	case grant.Table == "*":
		log.Printf("Granting permissions to all tables in schema %s", grant.Schema)
		query += fmt.Sprintf(" ALL TABLES IN SCHEMA %s", QuoteIdentifier(grant.Schema))

	case grant.Table != "":
		log.Printf("Granting permissions to table in schema %s", grant.Schema)
		query += fmt.Sprintf(" TABLE %s.%s", QuoteIdentifier(grant.Schema), QuoteIdentifier(grant.Table))
	}

	query += fmt.Sprintf(" TO %s", QuoteIdentifier(username))

	if grant.WithGrant {
		query += " WITH GRANT OPTION"
	}

	return query
}
