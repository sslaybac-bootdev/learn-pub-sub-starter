package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func publishTogglePause(channelA *amqp.Channel, pausing bool) {
	state := routing.PlayingState{
		IsPaused: pausing,
	}

	pubsub.PublishJSON(channelA, routing.ExchangePerilDirect, routing.PauseKey,
		state)
}

func handlerLogs() func(gl routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Printf("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}
func main() {
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

	connectionString := "amqp://guest:guest@localhost:5672/"
	ampqClient, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer ampqClient.Close()
	fmt.Println("RabbitMQ connection successful")

	channelA, err := ampqClient.Channel()
	if err != nil {
		log.Fatal("Unable to open channel.")
	}

	pubsub.SubscribeGob(ampqClient, "peril_topic", "game_logs", "game_logs.*", true, handlerLogs())
	_, _, err = pubsub.DeclareAndBind(ampqClient,
		"peril_topic", "game_logs", "game_logs.*", true)
	if err != nil {
		log.Fatal(err)
	}

	for closingServer := false; ; {
		input := gamelogic.GetInput()
		switch input[0] {
		case "pause":
			fmt.Printf("Pausing...\n")
			publishTogglePause(channelA, true)
		case "resume":
			fmt.Printf("Resuming...\n")
			publishTogglePause(channelA, false)
		case "quit":
			fmt.Printf("Shutting Down...\n")
			closingServer = true
		default:
			fmt.Printf("Command not recognized.\n")
		}
		if closingServer {
			break
		}
	}

}
