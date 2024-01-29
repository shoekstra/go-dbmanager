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

	// Optional: Specify the target parameter (PostgreSQL only)
	Parameter string `json:"parameter"`

	// Required: List of privileges (e.g., "ALL", "CONNECT", "USAGE", "SELECT", etc.)
	Privileges []string `json:"privileges"`

	// Optional: Grant option
	WithGrant bool `json:"with_grant"`
}

// UserOptions represents the configuration for creating a user
type UserOptions struct {
	// Login specifies whether the user is allowed to log in to the database. Applicable to PostgreSQL only.
	Login bool `json:"login"`

	// Superuser specifies whether the user will be a superuser. Applicable to PostgreSQL only.
	Superuser bool `json:"superuser"`

	// CreateDatabase specifies whether the user will be allowed to create databases. Applicable to PostgreSQL only.
	CreateDatabase bool `json:"create_database"`

	// CreateRole specifies whether the user will be allowed to create roles. Applicable to PostgreSQL only.
	CreateRole bool `json:"create_role"`

	// Inherit specifies whether the user will inherit privileges of roles that it is a member of. Applicable to PostgreSQL only.
	Inherit bool `json:"inherit"`

	// Replication specifies whether the user will be allowed to initiate streaming replication. Applicable to PostgreSQL only.
	Replication bool `json:"replication"`

	// BypassRLS specifies whether the user will be allowed to bypass row level security policies. Applicable to PostgreSQL only.
	BypassRLS bool `json:"bypass_rls"`
}

// User represents the configuration for creating a user
type User struct {
	Name     string      `json:"name"`
	Password string      `json:"password"`
	Options  UserOptions `json:"options"`
	Grants   []Grant     `json:"grants"`
	Roles    []string    `json:"roles"`
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
