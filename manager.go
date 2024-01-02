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
	DatabaseExists(databaseName string) (bool, error)
	CreateUser(userConfig User) error
	UserExists(username string) (bool, error)
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
	Name string `json:"name"`
}

// User represents the configuration for creating a user
type User struct {
	Name     string `json:"name"`
	Password string `json:"password"`
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
