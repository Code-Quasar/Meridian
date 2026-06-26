package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Code-Quasar/Meridian/internal/callbacks"
	"github.com/Code-Quasar/Meridian/internal/gateway"
	"github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"
	"github.com/Code-Quasar/Meridian/internal/queue"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
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

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure()) // Adjust address/credentials
	if err != nil {
		log.Fatalf("Failed to connect to Server B via gRPC: %v", err)
	}
	defer conn.Close()

	grpcClient := solver.NewSolverServiceClient(conn)

	queue.ReadFromQueue("jobs-small", "1234", func(msg kafka.Message) {
		callbacks.SendToGRPC(reg, grpcClient, msg)
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("Server A shut down . . .")
}
