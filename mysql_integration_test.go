//go:build mysql

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
	mysqlTestManager Manager
	mysqlResource    *dockertest.Resource

	mysqlAdminUser, mysqlAdminPassword string = "root", "password"
	mysqlUsername, mysqlPassword       string = "mytestuser", "mypassword"
	mysqlDatabase                      string = "mytestdb"
)

func testMySQLQuery(username, password, database, query string) (sql.Result, error) {
	m := &mysqlManager{
		databaseManager: databaseManager{
			connection: Connection{
				Host:     "localhost",
				Database: database,
				Port:     mysqlResource.GetPort("3306/tcp"),
				Username: username,
				Password: password,
			},
		},
	}
	if err := m.Connect(); err != nil {
		return nil, err
	}
	defer m.Disconnect()

	return m.db.Exec(query)
}

func testMySQLQueryForPermissions(username, database string) ([]string, error) {
	m := &mysqlManager{
		databaseManager: databaseManager{
			connection: Connection{
				Host:     "localhost",
				Database: database,
				Port:     mysqlResource.GetPort("3306/tcp"),
				Username: mysqlAdminUser,
				Password: mysqlAdminPassword,
			},
		},
	}
	if err := m.Connect(); err != nil {
		return nil, err
	}
	defer m.Disconnect()

	// Query the SCHEMA_PRIVILEGES table to get the privileges for the user on the database
	grantee := "'" + username + "'@'%'"
	rows, err := m.db.Query("SELECT PRIVILEGE_TYPE FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES WHERE GRANTEE = ? AND TABLE_SCHEMA = ?", grantee, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Collect all privileges
	var privileges []string
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			return nil, err
		}
		privileges = append(privileges, privilege)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return privileges, nil
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
	mysqlResource, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "latest",
		Env: []string{
			"MYSQL_ROOT_PASSWORD=password",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	mysqlResource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// Sleep for a while to allow the container to start to avoid the "connection.go:49: unexpected EOF" output
	time.Sleep(10 * time.Second)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 60 * time.Second

	if err := pool.Retry(func() error {
		databaseURL := fmt.Sprintf("%s:%s@tcp(%s)/", mysqlAdminUser, mysqlAdminPassword, mysqlResource.GetHostPort("3306/tcp"))
		log.Println("Connecting to database on URL: ", databaseURL)

		db, err := sql.Open("mysql", databaseURL)
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
	if err := pool.Purge(mysqlResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestMySQLManager_ConnectIntegration(t *testing.T) {
	mysqlTestManager = newMySQLManager(
		WithHost("localhost"),
		WithPort(mysqlResource.GetPort("3306/tcp")),
		WithUsername(mysqlAdminUser),
		WithPassword(mysqlAdminPassword),
	)
	// Test connection
	assert.NoError(t, mysqlTestManager.Connect(), "Erroring connecting to database")
}

func TestMySQLManager_CreateUserIntegration_Basic(t *testing.T) {
	err := mysqlTestManager.CreateUser(User{Name: mysqlUsername, Password: mysqlPassword})
	assert.NoError(t, err, "Error creating user")

	// Check if the user was created successfully
	_, err = testMySQLQuery(mysqlUsername, mysqlPassword, "", "SELECT 1")
	assert.NoError(t, err)

	// Attempting to create the user again should not return an error
	err = mysqlTestManager.CreateUser(User{Name: mysqlUsername, Password: mysqlPassword})
	assert.NoError(t, err, "Error creating user when it already exists")

	// Attempting to create the user again with a different password should not return an error
	newPassword := "newpassword"
	err = mysqlTestManager.CreateUser(User{Name: mysqlUsername, Password: newPassword})
	assert.NoError(t, err, "Error creating user when it already exists")

	// Check if the user was updated with the new password
	_, err = testMySQLQuery(mysqlUsername, newPassword, "", "SELECT 1")
	assert.NoError(t, err)
}

func TestMySQLManager_CreateDatabaseIntegration_Basic(t *testing.T) {
	err := mysqlTestManager.CreateDatabase(Database{Name: mysqlDatabase})
	assert.NoError(t, err)

	// Verify the database was created
	_, err = testMySQLQuery(mysqlAdminUser, mysqlAdminPassword, "", fmt.Sprintf("USE %s", mysqlDatabase))
	assert.NoError(t, err)

	// Attempting to create the database again should not return an error
	err = mysqlTestManager.CreateDatabase(Database{Name: mysqlDatabase})
	assert.NoError(t, err, "Error creating database when it already exists")
}

func TestMySQLManager_GrantPermissionsIntegration_Basic(t *testing.T) {
	// Grant permissions to the user
	err := mysqlTestManager.GrantPermissions(User{
		Name: mysqlUsername,
		Grants: []Grant{
			{
				Database:   mysqlDatabase,
				Privileges: []string{"SELECT", "INSERT"},
			},
		},
	})
	assert.NoError(t, err)

	// Verify the granted permissions using INFORMATION_SCHEMA
	permissions, err := testMySQLQueryForPermissions(mysqlUsername, mysqlDatabase)
	assert.NoError(t, err)

	// Check if the expected privileges are present
	expectedPermissions := []string{"SELECT", "INSERT"}
	for _, expected := range expectedPermissions {
		assert.Contains(t, permissions, expected)
	}
}

func TestMySQLManager_GrantPermissionsIntegration_All(t *testing.T) {
	// Grant permissions to the user
	err := mysqlTestManager.GrantPermissions(User{
		Name: mysqlUsername,
		Grants: []Grant{
			{
				Database:   mysqlDatabase,
				Privileges: []string{"ALL"},
			},
		},
	})
	assert.NoError(t, err)

	// Verify the granted permissions using INFORMATION_SCHEMA
	permissions, err := testMySQLQueryForPermissions(mysqlUsername, mysqlDatabase)
	assert.NoError(t, err)

	// Check if the expected privileges are present
	expectedPermissions := []string{
		"ALTER", "ALTER ROUTINE", "CREATE", "CREATE ROUTINE",
		"CREATE TEMPORARY TABLES", "CREATE VIEW", "DELETE", "DROP", "EVENT", "EXECUTE", "INDEX",
		"INSERT", "LOCK TABLES", "REFERENCES", "SELECT", "SHOW VIEW", "TRIGGER", "UPDATE",
	}
	for _, expected := range expectedPermissions {
		assert.Contains(t, permissions, expected)
	}
}
