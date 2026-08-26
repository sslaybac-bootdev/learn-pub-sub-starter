package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Printf("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(mv gamelogic.ArmyMove) {
		defer fmt.Printf("> ")
		gs.HandleMove(mv)
	}
}

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	ampqClient, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer ampqClient.Close()
	fmt.Println("RabbitMQ connection successful")

	playerName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, playerName)
	pubsub.DeclareAndBind(ampqClient, routing.ExchangePerilDirect, queueName, routing.PauseKey, false)
	gameState := gamelogic.NewGameState(playerName)
	err = pubsub.SubscribeJSON(ampqClient, routing.ExchangePerilDirect, queueName, routing.PauseKey, false, handlerPause(gameState))
	if err != nil {
		log.Fatal(err)
	}

	move_queue := fmt.Sprintf("army_moves.%s", playerName)
	move_pub_chan, err := ampqClient.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeJSON(ampqClient, routing.ExchangePerilTopic, move_queue, fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), false, handlerMove(gameState))
	if err != nil {
		log.Fatal(err)
	}

	for closingClient := false; ; {
		input := gamelogic.GetInput()
		switch input[0] {
		case "spawn":
			gameState.CommandSpawn(input)
		case "move":
			mv, err := gameState.CommandMove(input)
			if err == nil {
				pubsub.PublishJSON(move_pub_chan, routing.ExchangePerilTopic, move_queue, mv)
			} else {
				fmt.Printf("%v\n", err)
			}

		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Printf("Spamming not allowed yet!")
		case "quit":
			fmt.Printf("Shutting Down...\n")
			closingClient = true
		default:
			fmt.Printf("Command not recognized.\n")
		}

		if closingClient {
			break
		}
	}
}
