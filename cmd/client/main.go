package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

func logWarResult(ch *amqp.Channel, message string, rw gamelogic.RecognitionOfWar, gs *gamelogic.GameState) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    gs.GetUsername(),
	}

	routing_key := fmt.Sprintf("%s.%s", routing.GameLogSlug, rw.Attacker.Username)
	pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing_key, gameLog)
	return nil
}

func logSpam(ch *amqp.Channel, message string, gs *gamelogic.GameState) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    gs.GetUsername(),
	}
	msg_key := fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.Player.Username)
	err := pubsub.PublishGob(ch, "peril_topic", msg_key, gameLog)
	return err //err is either nil or not nil, and he caller will make the determination on how to deal with it.
}

func handlerWar(ch *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Printf("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			result_message := fmt.Sprintf("%s won a war against %s", winner, loser)
			logWarResult(ch, result_message, rw, gs)
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			result_message := fmt.Sprintf("%s won a war against %s", winner, loser)
			logWarResult(ch, result_message, rw, gs)
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			result_message := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			logWarResult(ch, result_message, rw, gs)
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
	war_channel, err := ampqClient.Channel()

	err = pubsub.SubscribeJSON(ampqClient, routing.ExchangePerilTopic, "war", war_key, true, handlerWar(war_channel, gameState))
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
			if len(input) < 2 {
				fmt.Printf("Spamming requires a count.\n")
				continue
			}
			n, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("%s is not a valid integer count.\n", input[1])
				continue
			}
			for range n {
				msg := gamelogic.GetMaliciousLog()
				err := logSpam(move_pub_chan, msg, gameState)
				if err != nil {
					fmt.Printf("Error when spamming.\n")
					continue
				}
			}
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
