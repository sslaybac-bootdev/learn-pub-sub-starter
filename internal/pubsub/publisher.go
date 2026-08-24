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
