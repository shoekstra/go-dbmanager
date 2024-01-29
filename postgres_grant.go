package dbmanager

import (
	"database/sql"
	"fmt"
	"log"
	"slices"
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

	// Remove user from roles not specified in the config
	roles, err := m.getRoles(user.Name)
	if err != nil {
		return err
	}

	for _, role := range roles {
		if !slices.Contains(user.Roles, role) {
			if err := m.removeRole(user.Name, role); err != nil {
				return fmt.Errorf("error removing user from role: %w", err)
			}
		}
	}

	return nil
}

// getRoles returns a list of roles for the specified user.
func (m *postgresManager) getRoles(username string) ([]string, error) {
	var roles []string
	query := "SELECT r.rolname FROM pg_roles r JOIN pg_auth_members m ON r.oid = m.roleid JOIN pg_roles u ON m.member = u.oid WHERE u.rolname = $1"
	rows, err := m.db.Query(query, strings.ToLower(username))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}

	return roles, nil
}

// addRole adds a user to a role.
func (m *postgresManager) addRole(username, role string) error {
	// Check if the user is trying to add themselves to the role
	if username == role {
		log.Printf("User %s is trying to add themselves to role %s, skipping\n", username, role)
		return nil
	}

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
func (m *postgresManager) hasRole(username, role string) (bool, error) {
	if username == role {
		return true, nil
	}

	var exists bool
	query := "SELECT 1 FROM pg_roles r JOIN pg_auth_members m ON r.oid = m.roleid JOIN pg_roles u ON m.member = u.oid WHERE r.rolname = $1 AND u.rolname = $2"
	err := m.db.QueryRow(query, strings.ToLower(role), username).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return exists, nil
}

// removeRole removes a user from a role.
func (m *postgresManager) removeRole(username, role string) error {
	// Check if the user is trying to remove themselves from the role
	if username == role {
		log.Printf("User %s is trying to remove themselves from role %s, skipping\n", username, role)
		return nil
	}

	// Check if the user has the role
	if hasRole, err := m.hasRole(username, role); err != nil {
		return err
	} else if !hasRole {
		log.Printf("User %s does not have role %s, skipping\n", username, role)
		return nil
	}

	// Remove the user from the role
	query := fmt.Sprintf("REVOKE %s FROM %s", QuoteIdentifier(role), QuoteIdentifier(username))
	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	log.Printf("Removed user %s from role %s\n", username, role)

	return nil
}

// grantPermission grants a single permission to a user.
func (m *postgresManager) grantPermission(username string, grant Grant) error {
	var query string

	database := grant.Database
	if database == "" {
		database = "postgres"
	}

	// Create new client using the database where permissions are being granted,
	// we also use this client to check if the user already has the permissions
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

	// Construct the grant query based on the provided options
	if grant.Database == "" && grant.Parameter != "" {
		if hasPermissions, err := db.hasParameterPrivilege(username, grant.Parameter, grant.Privileges[0]); err != nil {
			return err
		} else if hasPermissions {
			log.Printf("User %s already has permissions on parameter %s, skipping\n", username, grant.Parameter)
			return nil
		}
		query = m.grantParameterPermissionQuery(username, grant)
	} else if grant.Database != "" && grant.Schema == "" {
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

	for _, privilege := range privileges {
		query := fmt.Sprintf("SELECT has_database_privilege('%s', '%s', '%s')",
			username, database, privilege)
		var hasPermission bool
		if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
			return false, err
		}
		if !hasPermission {
			return false, nil // If any privilege is not granted, return false
		}
	}

	return true, nil // All privileges are granted
}

// hasParameterPrivilege checks if a user has the specified privileges on a parameter.
func (m *postgresManager) hasParameterPrivilege(username, parameter string, privilege string) (bool, error) {
	query := fmt.Sprintf("SELECT has_parameter_privilege('%s', '%s', '%s')",
		username, parameter, privilege)
	var hasPermission bool
	if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
		return false, err
	}
	if !hasPermission {
		return false, nil // If any privilege is not granted, return false
	}

	return true, nil // All privileges are granted
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

	for _, privilege := range privileges {
		query := fmt.Sprintf("SELECT has_table_privilege('%s', '%s.%s', '%s')",
			username, schema, table, privilege)
		var hasPermission bool
		if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
			return false, err
		}
		if !hasPermission {
			return false, nil // If any privilege is not granted, return false
		}
	}

	return true, nil // All privileges are granted
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

	for _, privilege := range privileges {
		query := fmt.Sprintf("SELECT has_sequence_privilege('%s', '%s.%s', '%s')",
			username, schema, sequence, privilege)
		var hasPermission bool
		if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
			return false, err
		}
		if !hasPermission {
			return false, nil // If any privilege is not granted, return false
		}
	}

	return true, nil // All privileges are granted
}

// hasSchemaPrivilege checks if a user has the specified privileges on a schema.
func (m *postgresManager) hasSchemaPrivilege(username, schema string, privileges []string) (bool, error) {
	if privileges[0] == "ALL" {
		privileges = []string{"CREATE", "USAGE"}
	}

	for _, privilege := range privileges {
		query := fmt.Sprintf("SELECT has_schema_privilege('%s', '%s', '%s')",
			username, schema, privilege)
		var hasPermission bool
		if err := m.db.QueryRow(query).Scan(&hasPermission); err != nil {
			return false, err
		}
		if !hasPermission {
			return false, nil // If any privilege is not granted, return false
		}
	}

	return true, nil // All privileges are granted
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

// grantParameterPermission grants a permission on a parameter to a user.
func (m *postgresManager) grantParameterPermissionQuery(username string, grant Grant) string {
	log.Printf("Granting %s permission to %s parameter", username, grant.Parameter)
	query := fmt.Sprintf("GRANT %s ON", strings.Join(grant.Privileges, ", "))
	if grant.Parameter == "*" {
		query += " ALL PARAMETERS"
	} else {
		query += fmt.Sprintf(" PARAMETER %s", QuoteIdentifier(grant.Parameter))
	}
	query += fmt.Sprintf(" TO %s", QuoteIdentifier(username))
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
