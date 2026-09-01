package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	gBytes := bytes.Buffer{}
	encoder := gob.NewEncoder(&gBytes)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	message := amqp.Publishing{
		ContentType: "application/gob",
		Body:        gBytes.Bytes(),
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, message)
	return err
}

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

	exchange_table := amqp.Table{}
	exchange_table["x-dead-letter-exchange"] = "peril_dlx"

	queueA, err := channelA.QueueDeclare(queueName, durable, !durable, !durable,
		false, exchange_table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channelA.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channelA, queueA, nil
}
