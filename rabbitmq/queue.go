package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	ContentTypeJson = "JSON"
)

const (
	delayExchangeName = "delay-exchange"
)

const (
	envKeyQueueMaxRetries = "QUEUE_MAX_RETRIES"
	envKeyQueueRetryDelay = "QUEUE_RETRY_DELAY"
)

type Queue struct {
	connection                        *Connection
	channel                           *amqp.Channel
	queue                             *amqp.Queue
	name, contentType, dqlName        string
	createIfNotExists, retryable, dlq bool
}

func (q *Queue) Connect() (err error) {
	if q.channel == nil {
		q.channel, err = q.connection.Channel()
		if err != nil {
			return
		}
	}

	if q.queue == nil && q.createIfNotExists {
		args := amqp.Table{}
		q.dqlName = q.name + "-dlq"
		var queue amqp.Queue
		queue, err = q.channel.QueueDeclare(
			q.name,
			false,
			false,
			false,
			false,
			args,
		)
		if err != nil {
			return
		}

		q.queue = &queue

		if q.dlq {
			_, err = q.channel.QueueDeclare(q.dqlName,
				false,
				false,
				false,
				false,
				nil)
			if err != nil {
				return
			}
		}

	}

	if q.retryable {
		err = q.channel.ExchangeDeclare(
			delayExchangeName,
			"x-delayed-message",
			true,
			false,
			false,
			false,
			map[string]interface{}{
				"x-delayed-type": "direct",
			},
		)
		if err != nil {
			return
		}

		err = q.channel.QueueBind(
			q.name,
			q.name,
			delayExchangeName,
			false,
			nil,
		)
		if err != nil {
			return
		}
	}
	return
}

func (q *Queue) Publish(ctx context.Context, content []byte) (err error) {
	err = q.channel.PublishWithContext(
		ctx,
		"",
		q.name,
		false,
		false,
		amqp.Publishing{
			ContentType: q.contentType,
			Body:        content,
		},
	)
	return err
}

func (q *Queue) Close() (err error) {
	err = q.channel.Close()
	if err != nil {
		return
	}

	err = q.connection.Close()
	if err != nil {
		return
	}

	return
}

func (q *Queue) Consume(ctx context.Context, handler func(delivery amqp.Delivery) error) (err error) {
	messagesCh, err := q.channel.ConsumeWithContext(
		ctx,
		q.name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	forever := make(chan bool)
	go func() {
		for msg := range messagesCh {
			e := handler(msg)
			if e != nil {
				if msg.Headers == nil {
					msg.Headers = map[string]any{}
				}
				retry, _ := msg.Headers["x-retry-count"].(int32)
				fmt.Println(fmt.Sprintf("%s - x-retry-count: %d - ", time.Now().Format(time.RFC3339), retry), e.Error())

				maxConsumerRetries, _ := strconv.Atoi(os.Getenv(envKeyQueueMaxRetries))
				if q.retryable && int(retry) < maxConsumerRetries {
					msg.Headers["x-retry-count"] = retry + 1
					msg.Headers["x-delay"] = os.Getenv(envKeyQueueRetryDelay)
					if err = q.channel.PublishWithContext(
						ctx,
						delayExchangeName,
						q.name,
						false,
						false,
						amqp.Publishing{
							Headers:     msg.Headers,
							ContentType: msg.ContentType,
							Body:        msg.Body,
						},
					); err != nil {
						log.Printf("Failed to re-enqueue message to main queue: %v", err)
					}
				} else if q.dlq {
					if err = q.channel.PublishWithContext(
						ctx,
						"",
						q.dqlName,
						false,
						false,
						amqp.Publishing{
							ContentType: msg.ContentType,
							Body:        msg.Body,
						},
					); err != nil {
						log.Printf("Failed to publish message to DLQ: %v\n", err)
					}
				}

			}
			err = msg.Ack(false)
			if err != nil {
				log.Printf("Failed to ack message: %v\n", err)
			}

		}
	}()

	<-forever
	return
}

func NewQueue(connection *Connection, name, contentType string, createIfNotExists, dlq, retryable bool) *Queue {
	return &Queue{
		connection:        connection,
		channel:           nil,
		queue:             nil,
		name:              name,
		contentType:       contentType,
		createIfNotExists: createIfNotExists,
		dlq:               dlq,
		retryable:         retryable,
	}
}
