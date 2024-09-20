//go:build postgres

package dbmanager

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var (
	postgresTestManager        Manager
	postgresTestManagerChecker *postgresManager
	postgresResource           *dockertest.Resource

	adminUser, adminPassword string = "postgres", "password"
	username, password       string = "mytestuser", "mypassword"
	database                 string = "mytestdb"
)

func testPostgresQuery(username, password, database, query string) (sql.Result, error) {
	m := &postgresManager{
		databaseManager: databaseManager{
			connection: Connection{
				Host:     "localhost",
				Database: database,
				Port:     postgresResource.GetPort("5432/tcp"),
				Username: username,
				Password: password,
				SSLMode:  "disable",
			},
		},
	}
	m.Connect()
	defer m.Disconnect()

	return m.db.Exec(query)
}

func TestMain(m *testing.M) {
	// Disable log output for tests
	log.SetOutput(io.Discard)

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	postgresResource, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "latest",
		Env: []string{
			"POSTGRES_PASSWORD=password",
			"POSTGRES_USER=postgres",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	postgresResource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 60 * time.Second

	if err := pool.Retry(func() error {
		databaseURL := fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", adminUser, adminPassword, postgresResource.GetHostPort("5432/tcp"))
		log.Println("Connecting to database on URL: ", databaseURL)

		db, err := sql.Open("pgx", databaseURL)
		if err != nil {
			return err
		}

		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// Run the tests
	code := m.Run()

	// Clean up
	if err := pool.Purge(postgresResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestPostgresManager_ConnectIntegration(t *testing.T) {
	postgresTestManager = newPostgresManager(
		WithHost("localhost"),
		WithPort(postgresResource.GetPort("5432/tcp")),
		WithUsername(adminUser),
		WithPassword(adminPassword),
	)
	// Test connection
	assert.NoError(t, postgresTestManager.Connect(), "Error connecting to database")

	// Create an engine specific manager for checking
	postgresTestManagerChecker = postgresTestManager.(*postgresManager)
}

func TestPostgresManager_CreateUserIntegration_Basic(t *testing.T) {
	// Perform the actual operation
	err := postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManagerChecker.userExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")

	// Attempting to create the user again with a different password should not return an error
	password = "newpassword"
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")

	created, err := postgresTestManagerChecker.getUser(username)
	assert.NoError(t, err, "Error getting user options")
	assert.Equal(t, username, created.Name, "User name does not match")
	assert.True(t, created.Options.Login, "User login does not match") // Login shouold be true when a password is set
}

func TestPostgresManager_CreateUserIntegration_BasicDashes(t *testing.T) {
	username := "my-test-user"

	// Perform the actual operation
	err := postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManagerChecker.userExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")
}

func TestPostgresManager_CreateUserIntegration_BasicMixedCase(t *testing.T) {
	username := "MyTestUser"

	// Perform the actual operation
	err := postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManagerChecker.userExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")
}

func TestPostgresManager_CreateUserIntegration_BasicUnderscores(t *testing.T) {
	username := "my_test_user"

	// Perform the actual operation
	err := postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManagerChecker.userExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")
}

func TestPostgresManager_CreateUserIntegration_WithOptions(t *testing.T) {
	username := "mytestuserwithoptions"

	user := User{
		Name: username,
		Options: UserOptions{
			Login:          true,
			Superuser:      true,
			CreateDatabase: true,
			CreateRole:     true,
			Inherit:        true,
			Replication:    true,
			BypassRLS:      true,
		},
	}

	// Perform the actual operation
	err := postgresTestManager.CreateUser(user)
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManagerChecker.userExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	created, err := postgresTestManagerChecker.getUser(username)
	assert.NoError(t, err, "Error getting user options")
	assert.Equal(t, user.Name, created.Name, "User name does not match")
	assert.Equal(t, user.Options.Login, created.Options.Login, "User login does not match")
	assert.Equal(t, user.Options.Superuser, created.Options.Superuser, "User superuser does not match")
	assert.Equal(t, user.Options.CreateDatabase, created.Options.CreateDatabase, "User create database does not match")
	assert.Equal(t, user.Options.CreateRole, created.Options.CreateRole, "User create role does not match")
	assert.Equal(t, user.Options.Inherit, created.Options.Inherit, "User inherit does not match")
	assert.Equal(t, user.Options.Replication, created.Options.Replication, "User replication does not match")
	assert.Equal(t, user.Options.BypassRLS, created.Options.BypassRLS, "User bypass RLS does not match")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user when it already exists")
}

func TestPostgresManager_CreateDatabaseIntegration_Basic(t *testing.T) {
	// Perform the actual operation
	err := postgresTestManager.CreateDatabase(Database{Name: database})
	assert.NoError(t, err, "Error creating database")

	// Check if the database was created successfully
	exists, err := postgresTestManagerChecker.databaseExists(database)
	assert.True(t, exists, "Database not found after CreateDatabase operation")
	assert.NoError(t, err, "Error checking if database exists")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: database})
	assert.NoError(t, err, "Error creating database when it already exists")
}

func TestPostgresManager_CreateDatabaseIntegration_AlterDefaultPrivileges(t *testing.T) {
	// Create database with default privileges should fail because the user does not exist yet
	defaultPrivileges := []DefaultPrivilege{
		{Role: "postgres", Schema: "public", Grant: []string{"ALL"}, On: "tables", To: "username"},
		{Role: "postgres", Schema: "public", Grant: []string{"USAGE", "SELECT"}, On: "SEQUENCES", To: "username"},
	}
	err := postgresTestManager.CreateDatabase(Database{Name: database, DefaultPrivileges: defaultPrivileges})
	assert.Error(t, err, "Creating database with default privileges should have failed if user does not exist")

	// Create database with default privileges again should succeed when user exists
	defaultPrivileges = []DefaultPrivilege{
		{Role: "postgres", Schema: "public", Grant: []string{"ALL"}, On: "tables", To: username},
		{Role: "postgres", Schema: "public", Grant: []string{"USAGE", "SELECT"}, On: "SEQUENCES", To: username},
	}
	err = postgresTestManager.CreateDatabase(Database{Name: database, DefaultPrivileges: defaultPrivileges})
	assert.NoError(t, err, "Error creating database with default privileges")

	// Check if the database was created successfully
	exists, err := postgresTestManagerChecker.databaseExists(database)
	assert.True(t, exists, "Database not found after CreateDatabase operation with default privileges")
	assert.NoError(t, err, "Error checking if database exists")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: database})
	assert.NoError(t, err, "Error creating database with default privileges when it already exists")
}

func TestPostgresManager_CreateDatabaseIntegration_Owner(t *testing.T) {
	owneddb := "owneddb"

	// Create database with owner that doesn't exist should fail
	err := postgresTestManager.CreateDatabase(Database{Name: owneddb, Owner: "username"})
	assert.Error(t, err, "Creating database with owner set should have failed if user does not exist")

	// Create database with owner should succeed when user exists
	err = postgresTestManager.CreateDatabase(Database{Name: owneddb, Owner: username})
	assert.NoError(t, err, "Error creating database with existing owner set")

	// Check if the database was created successfully
	exists, err := postgresTestManagerChecker.databaseExists(owneddb)
	assert.True(t, exists, "Database not found after CreateDatabase operation with owner set")
	assert.NoError(t, err, "Error checking if database exists")
	set, err := postgresTestManagerChecker.getDatabaseOwner(owneddb)
	assert.Equal(t, username, set, "Owner not set after CreateDatabase operation with owner set")
	assert.NoError(t, err, "Error checking if owner is set")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: owneddb})
	assert.NoError(t, err, "Error creating database with owner set when it already exists")
}

func TestPostgresManager_CreateDatabaseIntegration_UpdateOwner(t *testing.T) {
	owneddb := "owneddb"

	// Check current owner
	current, err := postgresTestManagerChecker.getDatabaseOwner(owneddb)
	assert.Equal(t, username, current, "Owner not set after CreateDatabase operation with owner set")
	assert.NoError(t, err, "Error checking if owner is set")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: owneddb, Owner: "postgres"})
	assert.NoError(t, err, "Error updating database with new owner set")

	// Check if the database was updated successfully
	updated, err := postgresTestManagerChecker.getDatabaseOwner(owneddb)
	assert.Equal(t, "postgres", updated, "Owner not set after CreateDatabase operation with owner set")
	assert.NoError(t, err, "Error checking if owner is set")
}

func TestPostgresManager_GrantPermissionsIntegration_Database(t *testing.T) {
	// Test grant options
	grants := []Grant{
		{
			Database:   database,
			Privileges: []string{"ALL"},
		},
	}

	// Perform the actual operation
	err := postgresTestManager.GrantPermissions(User{Name: username, Grants: grants})
	assert.NoError(t, err, "Error granting permissions")
}

func TestPostgresManager_GrantPermissionsIntegration_AllSequences(t *testing.T) {
	// Test grant options
	grants := []Grant{
		{
			Database:   database,
			Privileges: []string{"USAGE", "SELECT"},
			Schema:     "public",
			Sequence:   "*",
		},
	}

	// Perform the actual operation
	err := postgresTestManager.GrantPermissions(User{Name: username, Grants: grants})
	assert.NoError(t, err, "Error granting permissions")
}

func TestPostgresManager_GrantPermissionsIntegration_AllTables(t *testing.T) {
	// Test grant options
	grants := []Grant{
		{
			Database:   database,
			Privileges: []string{"ALL"},
			Schema:     "public",
			Table:      "*",
		},
	}

	// Perform the actual operation
	err := postgresTestManager.GrantPermissions(User{Name: username, Grants: grants})
	assert.NoError(t, err, "Error granting permissions")
}

func TestPostgresManager_GrantPermissionsIntegration_AddRole(t *testing.T) {
	// Create a new role
	role := "myrole"
	err := postgresTestManager.CreateUser(User{Name: role})
	assert.NoError(t, err, "Error creating role")

	// Assign the role to the user
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions")

	// Check if the role was assigned successfully
	set, err := postgresTestManagerChecker.hasRole(username, role)
	assert.NoError(t, err, "Error checking if user has role")
	assert.True(t, set, "User does not have role after GrantPermissions operation")

	// Attempting to assign the role again should not return an error
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions when role is already assigned")
}

func TestPostgresManager_GrantPermissionsIntegration_AddRoleWithDashes(t *testing.T) {
	// Create a new role
	role := "my-role"
	err := postgresTestManager.CreateUser(User{Name: role})
	assert.NoError(t, err, "Error creating role")

	// Assign the role to the user
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions")

	// Check if the role was assigned successfully
	set, err := postgresTestManagerChecker.hasRole(username, role)
	assert.NoError(t, err, "Error checking if user has role")
	assert.True(t, set, "User does not have role after GrantPermissions operation")

	// Attempting to assign the role again should not return an error
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions when role is already assigned")
}

func TestPostgresManager_GrantPermissionsIntegration_AddRoleWithUnderscores(t *testing.T) {
	// Create a new role
	role := "my_role"
	err := postgresTestManager.CreateUser(User{Name: role})
	assert.NoError(t, err, "Error creating role")

	// Assign the role to the user
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions")

	// Check if the role was assigned successfully
	set, err := postgresTestManagerChecker.hasRole(username, role)
	assert.NoError(t, err, "Error checking if user has role")
	assert.True(t, set, "User does not have role after GrantPermissions operation")

	// Attempting to assign the role again should not return an error
	err = postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}})
	assert.NoError(t, err, "Error granting permissions when role is already assigned")
}

func TestPostgresManager_GrantPermissionsIntegration_AddSetParameter(t *testing.T) {
	username := "mytestparameteruser"
	grants := []Grant{{Parameter: "session_replication_role", Privileges: []string{"SET"}}}

	// Create a new user
	err := postgresTestManager.CreateUser(User{Name: username, Password: password})
	assert.NoError(t, err, "Error creating user")

	// Assign permissions to the user
	err = postgresTestManager.GrantPermissions(User{Name: username, Grants: grants})
	assert.NoError(t, err, "Error granting permissions")

	// Check if the role was assigned successfully
	set, err := postgresTestManagerChecker.hasParameterPrivilege(username, "session_replication_role", "SET")
	assert.NoError(t, err, "Error checking if user has parameter set")
	assert.True(t, set, "User does not have session_replication_role parameter after GrantPermissions operation")

	// Attempting to assign the role again should not return an error
	err = postgresTestManager.GrantPermissions(User{Name: username, Grants: grants})
	assert.NoError(t, err, "Error granting permissions when role is already assigned")

	// Attempt to set the parameter
	_, err = testPostgresQuery(username, password, database, "SET session_replication_role = replica;")
	assert.NoError(t, err, "Error setting parameter")
}

func TestPostgresManager_GrantPermissionsIntegration_RemoveRole(t *testing.T) {
	role := "myrole"
	extraRole := "myextrarole"

	err := postgresTestManager.CreateUser(User{Name: role})
	assert.NoError(t, err, "Error creating role")

	err = postgresTestManager.CreateUser(User{Name: extraRole})
	assert.NoError(t, err, "Error creating role")

	// Assign both roles and then remove one
	assert.NoError(t, postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role, extraRole}}), "Error granting permissions")
	assert.NoError(t, postgresTestManager.GrantPermissions(User{Name: username, Roles: []string{role}}), "Error granting permissions")

	// Check if the role was removed successfully
	set, err := postgresTestManagerChecker.hasRole(username, extraRole)
	assert.NoError(t, err, "Error checking if user has role")
	assert.False(t, set, "User still has \"myextrarole\" role after GrantPermissions operation")
}

func TestPostgresManager_ManagerIntegration(t *testing.T) {
	managedUser := "manageduser"
	managedDatabase := "manageddb"
	managedOwner := "managedowner"

	databases := []Database{
		{
			Name:  managedDatabase,
			Owner: managedUser,
			DefaultPrivileges: []DefaultPrivilege{
				{Role: "postgres", Schema: "public", Grant: []string{"ALL"}, On: "tables", To: username},
				{Role: "postgres", Schema: "public", Grant: []string{"USAGE", "SELECT"}, On: "SEQUENCES", To: username},
			},
		},
	}
	userGrants := []Grant{
		{Database: managedDatabase, Privileges: []string{"ALL"}},
		{Database: managedDatabase, Privileges: []string{"USAGE", "SELECT"}, Schema: "public", Sequence: "*"},
		{Database: managedDatabase, Privileges: []string{"ALL"}, Schema: "public", Table: "*"},
	}
	users := []User{
		{Name: managedUser, Password: password, Grants: userGrants},
		{Name: managedOwner, Password: password},
	}

	// Perform the actual operation
	err := postgresTestManager.Manage(databases, users)
	assert.NoError(t, err, "Error managing databases and users")

	// Check if the database was created successfully
	exists, err := postgresTestManagerChecker.databaseExists(managedDatabase)
	assert.True(t, exists, "Database not found after Manage operation")
	assert.NoError(t, err, "Error checking if database exists")

	// Check if the user was created successfully
	exists, err = postgresTestManagerChecker.userExists(managedUser)
	assert.True(t, exists, "User not found after Manage operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Check if the owner was created successfully
	exists, err = postgresTestManagerChecker.userExists(managedOwner)
	assert.True(t, exists, "Owner not found after Manage operation")
	assert.NoError(t, err, "Error checking if owner exists")
}

func TestPostgresManager_DisconnectIntegration(t *testing.T) {
	// Test disconnection
	assert.NoError(t, postgresTestManager.Disconnect(), "Error disconnecting from database")
}
