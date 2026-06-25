package main

import pb "github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"

type SolverServiceServer struct {
	pb.UnimplementedSolverServiceServer
}

func (s *SolverServiceServer) Solve(req *pb.SolveRequest, stream pb.SolverService_SolveServer) error {
	return nil
}
