package dbmanager

// Connector represents a database connection
type Connector interface {
	Connect() error
	Disconnect() error
}

// Connection represents the configuration for establishing a connection
type Connection struct {
	Host     string
	Database string
	Port     string
	Username string
	Password string
	SSLMode  string
	SSL      bool
}

// WithHost sets the host in the connection configuration
func WithHost(host string) func(*Connection) {
	return func(c *Connection) {
		c.Host = host
	}
}

// WithPort sets the port in the connection configuration
func WithPort(port string) func(*Connection) {
	return func(c *Connection) {
		c.Port = port
	}
}

// WithUsername sets the username in the connection configuration
func WithUsername(username string) func(*Connection) {
	return func(c *Connection) {
		c.Username = username
	}
}

// WithPassword sets the password in the connection configuration
func WithPassword(password string) func(*Connection) {
	return func(c *Connection) {
		c.Password = password
	}
}

// WithSSL sets the SSL flag in the connection configuration
func WithSSL(ssl bool) func(*Connection) {
	return func(c *Connection) {
		c.SSL = ssl
	}
}
