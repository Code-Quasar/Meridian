package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Code-Quasar/Meridian/internal/gateway"
	"github.com/Code-Quasar/Meridian/internal/queue"
)

// this is the main function of Server A.
// this contains :
// - an ApiGateway that send solver requests to kafka producer
// - a kafka producer that send requests to kafka
// - a kafka consumer that consume those requests and call methods using gRPC from Server B
// - the results are streams that are send sequentially into a websocket between server A and the client

func main() {

	log.Println("Starting Meridian Server A ...")
	reg := gateway.EstablishTCP(":9000", ":9093", "jobs-small")

	messages := queue.ReadFromQueue("jobs-small", "1234")

	for m := range messages {
		clientID := string(m.Key)
		msgValue := string(m.Value)

		connInfo, exists := reg.GetConnection(clientID)
		if !exists {
			continue
		}

		// Use a non-blocking select to put the message
		select {
		case connInfo.Response <- msgValue:
		default:
			go func(ch chan string, data string) {
				ch <- data
			}(connInfo.Response, msgValue)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("Server A shut down . . .")
}
