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

// newPostgresManager creates a new PostgreSQL manager.
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

// Connect connects to the PostgreSQL server.
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

// connectionStrings returns a list of connection strings for the specified database.
func (m *postgresManager) connectionString(connection Connection) string {
	connectionString := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		connection.Host, connection.Port, connection.Username, connection.Database, connection.SSLMode)
	if m.connection.Password != "" {
		connectionString += fmt.Sprintf(" password=%s", m.connection.Password)
	}
	return connectionString
}

// Disconnect disconnects from the PostgreSQL server.
func (m *postgresManager) Disconnect() error {
	log.Println("Disconnecting...")

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("error closing connection to PostgreSQL database: %w", err)
	}

	return nil
}

// GrantPermissions grants permissions to a user based on the provided Grant options.
func (m *postgresManager) GrantPermissions(username, database string, grants []Grant) error {
	// Check if the user exists
	if exists, err := m.UserExists(username); err != nil {
		return err
	} else if !exists {
		log.Printf("User %s does not exist, skipping\n", username)
		return nil
	}

	// Grant permissions
	for _, grant := range grants {
		log.Printf("Processing grant: %v", grant)

		if err := m.grantPermission(username, grant); err != nil {
			return fmt.Errorf("error granting permissions: %w", err)
		}
	}

	return nil
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

// Manage manages the databases and users based on the provided options.
func (m *postgresManager) Manage(databases []Database, users []User) error {
	// Create users
	for _, user := range users {
		if err := m.CreateUser(user); err != nil {
			return err
		}
	}

	// Create databases
	for _, database := range databases {
		if err := m.CreateDatabase(database); err != nil {
			return err
		}
	}

	// Grant permissions
	for _, user := range users {
		for _, grant := range user.Grants {
			if err := m.GrantPermissions(user.Name, grant.Database, []Grant{grant}); err != nil {
				return err
			}
		}
	}

	return nil
}
