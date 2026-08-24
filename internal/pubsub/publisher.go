package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	message := amqp.Publishing{
		ContentType: "application/json",
		Body:        jBytes,
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, message)
	if err != nil {
		return err
	}

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
) (*amqp.Channel, amqp.Queue, error) {
	channelA, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queueA, err := channelA.QueueDeclare(queueName, durable, !durable, !durable, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channelA.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channelA, queueA, nil
}
