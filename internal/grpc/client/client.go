package client

import (
	"context"
	"log"

	"github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"
)

func ClientSolve(ctx context.Context, client solver.SolverServiceClient, req *solver.SolveRequest) <-chan *solver.SolveEvent {

	ch := make(chan *solver.SolveEvent)

	stream, err := client.Solve(ctx, req)
	if err != nil {
		log.Fatalf("[Solver] Solve : %v . \n", err)
	}

	go func() {
		defer close(ch)
		for {
			result, err := stream.Recv()
			if err != nil {
				break
			}

			select {
			case ch <- result:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
