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
	postgresTestManager Manager
	resource            *dockertest.Resource

	adminUser, adminPassword string = "postgres", "password"
	username, password       string = "mytestuser", "mypassword"
	database                 string = "mytestdb"
)

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
	resource, err = pool.RunWithOptions(&dockertest.RunOptions{
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

	databaseURL := fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", adminUser, adminPassword, resource.GetHostPort("5432/tcp"))

	log.Println("Connecting to database on URL: ", databaseURL)

	resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 60 * time.Second
	if err := pool.Retry(func() error {
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
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestPostgresManager_ConnectIntegration(t *testing.T) {
	postgresTestManager = newPostgresManager(
		WithHost("localhost"),
		WithPort(resource.GetPort("5432/tcp")),
		WithUsername(adminUser),
		WithPassword(adminPassword),
	)
	// Test connection
	assert.NoError(t, postgresTestManager.Connect(), "Error connecting to database")
}

func TestPostgresManager_CreateUserIntegration_Basic(t *testing.T) {
	// Perform the actual operation
	err := postgresTestManager.CreateUser(User{Name: username, Password: "password"})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	exists, err := postgresTestManager.UserExists(username)
	assert.True(t, exists, "User not found after CreateUser operation")
	assert.NoError(t, err, "Error checking if user exists")

	// Attempting to create the user again should not return an error
	err = postgresTestManager.CreateUser(User{Name: username, Password: "password"})
	assert.NoError(t, err, "Error creating user when it already exists")
}

func TestPostgresManager_CreateDatabaseIntegration_Basic(t *testing.T) {
	// Perform the actual operation
	err := postgresTestManager.CreateDatabase(Database{Name: database})
	assert.NoError(t, err, "Error creating database")

	// Check if the database was created successfully
	exists, err := postgresTestManager.DatabaseExists(database)
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
	exists, err := postgresTestManager.DatabaseExists(database)
	assert.True(t, exists, "Database not found after CreateDatabase operation with default privileges")
	assert.NoError(t, err, "Error checking if database exists")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: database})
	assert.NoError(t, err, "Error creating database with default privileges when it already exists")
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
	err := postgresTestManager.GrantPermissions(username, database, grants)
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
	err := postgresTestManager.GrantPermissions(username, database, grants)
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
	err := postgresTestManager.GrantPermissions(username, database, grants)
	assert.NoError(t, err, "Error granting permissions")
}

func TestPostgresManager_DisconnectIntegration(t *testing.T) {
	// Test disconnection
	assert.NoError(t, postgresTestManager.Disconnect(), "Error disconnecting from database")
}
