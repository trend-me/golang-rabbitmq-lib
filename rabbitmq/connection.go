package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	*amqp.Connection
}

func (c *Connection) Connect(user, pwd, host, port string) (err error) {
	c.Connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pwd, host, port))
	return
}

func (c *Connection) Disconnect() error {
	return c.Close()
}
