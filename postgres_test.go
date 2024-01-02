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

var postgresTestManager Manager
var resource *dockertest.Resource
var adminUser, adminPassword string

func TestMain(m *testing.M) {
	// Disable log output for tests
	log.SetOutput(io.Discard)

	adminUser, adminPassword = "postgres", "password"

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

func TestPostgresManager_CreateDatabaseIntegration_Basic(t *testing.T) {
	// Test database name
	databaseName := "mytestdb"

	// Perform the actual operation
	err := postgresTestManager.CreateDatabase(Database{Name: databaseName})
	assert.NoError(t, err, "Error creating database")

	// Check if the database was created successfully
	exists, err := postgresTestManager.DatabaseExists(databaseName)
	assert.True(t, exists, "Database not found after CreateDatabase operation")
	assert.NoError(t, err, "Error checking if database exists")

	// Attempting to create the database again should not return an error
	err = postgresTestManager.CreateDatabase(Database{Name: databaseName})
	assert.NoError(t, err, "Error creating database when it already exists")
}

func TestPostgresManager_CreateUserIntegration_Basic(t *testing.T) {
	// Test user name
	username := "mytestuser"

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

func TestPostgresManager_CreateDatabaseIntegration(t *testing.T) {
	// Test disconnection
	assert.NoError(t, postgresTestManager.Disconnect(), "Error disconnecting from database")
}
