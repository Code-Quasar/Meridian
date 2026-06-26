package main

import (
	"log"
	"net"

	pb "github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"
	"google.golang.org/grpc"
)

type SolverServiceServer struct {
	pb.UnimplementedSolverServiceServer
}

func (s *SolverServiceServer) Solve(req *pb.SolveRequest, stream pb.SolverService_SolveServer) error {

	// Optimization implementation

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":9080")
	if err != nil {
		log.Fatalf("Unable to create server: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSolverServiceServer(grpcServer, &SolverServiceServer{})

	log.Println("Server started ...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server failed : %v", err)
	}
}
