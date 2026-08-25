package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ProcessMessages[T any](ch <-chan amqp.Delivery, handler func(T)) {
	for delivery := range ch {
		var content T
		err := json.Unmarshal(delivery.Body, &content)
		if err == nil {
			handler(content)
		}
		delivery.Ack(false)
	}

}
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T),
) error {
	ch_amqp, _, err := DeclareAndBind(conn, exchange, queueName, key, durable)
	if err != nil {
		return err
	}

	delivery_ch, err := ch_amqp.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go ProcessMessages(delivery_ch, handler)
	return nil
}
