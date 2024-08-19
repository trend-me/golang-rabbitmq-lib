package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	*amqp.Connection
	user, host, pwd, port string
}

func (c *Connection) Connect(user, pwd, host, port string) (err error) {
	c.user, c.pwd, c.host, c.port = user, pwd, host, port
	c.Connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", c.user, c.pwd, c.host, c.port))
	return
}

func (c *Connection) Reconnect() (err error) {
	c.Connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", c.user, c.pwd, c.host, c.port))
	return
}

func (c *Connection) Disconnect() error {
	return c.Close()
}

