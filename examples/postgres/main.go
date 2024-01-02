package main

import (
	"github.com/shoekstra/go-dbmanager"
)

func main() {
	databaseName := "mytestdb"
	username, password := "mytestuser", "password"

	// Example usage with PostgreSQL
	dbm, err := dbmanager.New(
		"postgres",
		dbmanager.WithHost("localhost"),
		dbmanager.WithUsername("postgres"),
		dbmanager.WithPassword("password"),
	)
	if err != nil {
		panic(err)
	}

	// Connect to server
	if err := dbm.Connect(); err != nil {
		panic(err)
	}
	defer dbm.Disconnect()

	// Create user: this always needs to happen first otherwise the default privileges cannot be set
	if err := dbm.CreateUser(dbmanager.User{Name: username, Password: password}); err != nil {
		panic(err)
	}

	defaultPrivileges := []dbmanager.DefaultPrivilege{
		{Role: "postgres", Schema: "public", Grant: []string{"ALL"}, On: "tables", To: username},
		{Role: "postgres", Schema: "public", Grant: []string{"USAGE", "SELECT"}, On: "SEQUENCES", To: username},
	}

	// Create database
	if err := dbm.CreateDatabase(dbmanager.Database{Name: databaseName, DefaultPrivileges: defaultPrivileges}); err != nil {
		panic(err)
	}

	// Assign privileges to user
	grants := []dbmanager.Grant{
		{Database: databaseName, Privileges: []string{"ALL"}},
		{Database: databaseName, Privileges: []string{"USAGE", "SELECT"}, Schema: "public", Sequence: "*"},
		{Database: databaseName, Privileges: []string{"ALL"}, Schema: "public", Table: "*"},
	}
	if err := dbm.GrantPermissions(username, databaseName, grants); err != nil {
		panic(err)
	}
}
