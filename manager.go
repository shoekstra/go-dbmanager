package dbmanager

import (
	"database/sql"
	"fmt"
	"strings"
)

// Manager is the main interface for managing database servers
type Manager interface {
	Connector
	CreateDatabase(databaseConfig Database) error
	CreateUser(userConfig User) error
	GrantPermissions(user User) error
	Manage(databases []Database, users []User) error
}

// databaseManager is the internal implementation of the Manager interface
type databaseManager struct {
	connection Connection
	db         *sql.DB
}

// initialize initializes the database manager connection with the provided options.
func (m *databaseManager) initialize(options ...func(*Connection)) {
	for _, option := range options {
		option(&m.connection)
	}
}

// Database represents the configuration for creating a database
type Database struct {
	Name              string             `json:"name"`
	DefaultPrivileges []DefaultPrivilege `json:"default_privileges"`
	Owner             string             `json:"owner"`
}

// DefaultPrivilege contains the default privileges in a database for a user or role.
type DefaultPrivilege struct {
	Role      string   `json:"role"`
	Schema    string   `json:"schema"`
	Grant     []string `json:"grant"`
	On        string   `json:"on"`
	To        string   `json:"to"`
	WithGrant bool     `json:"with_grant"`
}

// Grant represents a set of permissions granted to a user.
type Grant struct {
	// Optional: Specify the target database
	Database string `json:"database"`

	// Optional: Specify the target schema
	Schema string `json:"schema"`

	// Optional: Specify the target Sequence
	Sequence string `json:"sequence"`

	// Optional: Specify the target table
	Table string `json:"table"`

	// Required: List of privileges (e.g., "ALL", "CONNECT", "USAGE", "SELECT", etc.)
	Privileges []string `json:"privileges"`

	// Optional: Grant option
	WithGrant bool `json:"with_grant"`
}

// User represents the configuration for creating a user
type User struct {
	Name     string   `json:"name"`
	Password string   `json:"password"`
	Grants   []Grant  `json:"grants"`
	Roles    []string `json:"roles"`
}

// New creates a new Manager instance based on the provided engine.
func New(engine string, options ...func(*Connection)) (Manager, error) {
	switch engine {
	case "mysql":
		return newMySQLManager(options...), nil
	case "postgres":
		return newPostgresManager(options...), nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", engine)
	}
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//	tblname := "my_table"
//	data := "my_data"
//	quoted := pq.QuoteIdentifier(tblname)
//	err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
//
// This is a copy of the PostgreSQL libpq function so that we don't need to
// import the entire pq package.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
