package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Printf("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Printf("> ")
		outcome := gs.HandleMove(mv)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			warEvent := gamelogic.RecognitionOfWar{
				Attacker: mv.Player,
				Defender: gs.GetPlayerSnap(),
			}
			key := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username)
			pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, warEvent)
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}

	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Printf("> ")
		outcome, _, _ := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			log.Printf("Error: not a valid war outcome\n")
			return pubsub.NackDiscard
		}

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

	err = pubsub.SubscribeJSON(ampqClient, routing.ExchangePerilTopic, move_queue, fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), false, handlerMove(gameState, move_pub_chan))
	if err != nil {
		log.Fatal(err)
	}

	war_key := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(ampqClient, routing.ExchangePerilTopic, "war", war_key, true, handlerWar(gameState))

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
