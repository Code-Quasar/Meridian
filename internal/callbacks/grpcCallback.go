package callbacks

import (
	"context"

	"github.com/Code-Quasar/Meridian/internal/grpc/client"
	"github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"
	"github.com/Code-Quasar/Meridian/internal/registry"
	"github.com/segmentio/kafka-go"
)

func transformToRequest(msg kafka.Message) *solver.SolveRequest {
	return nil
}

func SendToGRPC(reg *registry.ConnRegistry, grpcClient solver.SolverServiceClient, msg kafka.Message) {

	// change the kafka message to SolveRequest
	clientID := string(msg.Key)

	connInfo, exists := reg.GetConnection(clientID)
	if !exists {
		return
	}

	req := transformToRequest(msg)

	// gRPC Stream Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for event := range client.ClientSolve(ctx, grpcClient, req) {

		connInfo.Response <- event
	}
}
