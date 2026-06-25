package main

// this is the main function of Server A.
// this contains :
// - an ApiGateway that send solver requests to kafka producer
// - a kafka producer that send requests to kafka
// - a kafka consumer that consume those requests and call methods using gRPC from Server B
// - the results are streams that are send sequentially into a websocket between server A and the client

func main() {

	// Here the implementation of the api gateway and the socket between user and server

	// the api gateway route it to the kafka producer

	// the consumer consume what it could consume and make gRPC requests

	// send results to the socket

}
