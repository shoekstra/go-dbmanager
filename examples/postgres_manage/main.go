package main

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/shoekstra/go-dbmanager"
)

type config struct {
	Databases []dbmanager.Database `json:"databases"`
	Users     []dbmanager.User     `json:"users"`
}

func main() {
	// Open our config file
	cfg, err := readConfigFile()
	if err != nil {
		log.Fatal(err)
	}

	// Create a new database manager
	dbm, err := dbmanager.New(
		"postgres",
		dbmanager.WithHost("localhost"),
		dbmanager.WithUsername("postgres"),
		dbmanager.WithPassword("password"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Connect to the database and close the connection when done
	if err := dbm.Connect(); err != nil {
		log.Fatal(err)
	}
	defer dbm.Disconnect()

	// Manage the databases, users and permissions
	if err := dbm.Manage(cfg.Databases, cfg.Users); err != nil {
		log.Fatal(err)
	}
}

func readConfigFile() (*config, error) {
	file, err := os.Open("config.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg config
	byteValue, _ := io.ReadAll(file)
	json.Unmarshal(byteValue, &cfg)

	return &cfg, nil
}
