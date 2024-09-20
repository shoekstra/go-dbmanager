package dbmanager

import (
	"fmt"
	"log"
	"strings"
)

// GrantPermissions grants permissions to a MySQL user based on the provided Grant options.
func (m *mysqlManager) GrantPermissions(user User) error {
	log.Printf("Granting permissions to user: %s\n", user.Name)

	// Check if the user exists
	if exists, err := m.userExists(user.Name); err != nil {
		return err
	} else if !exists {
		log.Printf("User %s does not exist, skipping\n", user.Name)
		return nil
	}

	// Grant permissions based on the grants specified for the user
	for _, grant := range user.Grants {
		log.Printf("Processing grant: %v", grant)

		// Build the base GRANT query
		grantQuery := fmt.Sprintf("GRANT %s ON %s.* TO '%s'@'%%'",
			strings.Join(grant.Privileges, ", "), // Join privileges
			grant.Database,                       // Grant specific database
			user.Name)                            // User

		// Add WITH GRANT OPTION if specified
		if grant.WithGrant {
			grantQuery += " WITH GRANT OPTION"
		}

		// Execute the GRANT query
		_, err := m.db.Exec(grantQuery)
		if err != nil {
			return fmt.Errorf("error granting permissions: %w", err)
		}
	}

	return nil
}
