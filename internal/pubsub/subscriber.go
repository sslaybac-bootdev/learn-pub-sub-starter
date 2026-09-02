package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func ProcessJson[T any](ch <-chan amqp.Delivery, handler func(T) AckType) {
	for delivery := range ch {
		var content T
		err := json.Unmarshal(delivery.Body, &content)
		if err == nil {
			a_type := handler(content)
			switch a_type {
			case Ack:
				delivery.Ack(false)
				log.Printf("Sending Ack response.")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Printf("Sending NackRequeue response.")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Printf("Sending NackDiscard response.")
			}
		} else {
			delivery.Ack(false)
		}
	}

}

func ProcessGob[T any](ch <-chan amqp.Delivery, handler func(T) AckType) {
	for delivery := range ch {
		var content T
		b := bytes.NewBuffer(delivery.Body)
		decoder := gob.NewDecoder(b)
		err := decoder.Decode(&content)
		if err == nil {
			a_type := handler(content)
			switch a_type {
			case Ack:
				delivery.Ack(false)
				log.Printf("Sending Ack response.")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Printf("Sending NackRequeue response.")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Printf("Sending NackDiscard response.")
			}
		} else {
			delivery.Ack(false)
		}
	}
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T) AckType,
) error {
	ch_amqp, _, err := DeclareAndBind(conn, exchange, queueName, key, durable)
	if err != nil {
		return err
	}

	delivery_ch, err := ch_amqp.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go ProcessGob(delivery_ch, handler)
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T) AckType,
) error {
	ch_amqp, _, err := DeclareAndBind(conn, exchange, queueName, key, durable)
	if err != nil {
		return err
	}

	delivery_ch, err := ch_amqp.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go ProcessJson(delivery_ch, handler)
	return nil
}
