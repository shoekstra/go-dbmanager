package main

import (
	"github.com/shoekstra/go-dbmanager"
)

func main() {
	databaseName := "mytestdb"
	owner, user, password := "myowner", "mytestuser", "password"

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

	// Create users: this always needs to happen first otherwise the default privileges cannot be set
	for _, user := range []string{owner, user} {
		if err := dbm.CreateUser(dbmanager.User{Name: user, Password: password}); err != nil {
			panic(err)
		}
	}

	defaultPrivileges := []dbmanager.DefaultPrivilege{
		{Role: owner, Schema: "public", Grant: []string{"ALL"}, On: "tables", To: user},
		{Role: owner, Schema: "public", Grant: []string{"USAGE", "SELECT"}, On: "SEQUENCES", To: user},
	}

	// Create database
	if err := dbm.CreateDatabase(dbmanager.Database{Name: databaseName, Owner: user, DefaultPrivileges: defaultPrivileges}); err != nil {
		panic(err)
	}

	// Assign privileges to user
	grants := []dbmanager.Grant{
		{Database: databaseName, Privileges: []string{"ALL"}},
		{Database: databaseName, Privileges: []string{"USAGE", "SELECT"}, Schema: "public", Sequence: "*"},
		{Database: databaseName, Privileges: []string{"ALL"}, Schema: "public", Table: "*"},
	}
	if err := dbm.GrantPermissions(dbmanager.User{Name: user, Grants: grants}); err != nil {
		panic(err)
	}
}
