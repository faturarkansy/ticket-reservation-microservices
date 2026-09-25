package main

import (
	"fmt"
	"net"

	"seat-service/handler"
	"seat-service/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on port %s: %v", port, err))
	}

	grpcServer := grpc.NewServer()

	// Register gRPC Handler
	seatHandler := handler.NewSeatHandler()
	pb.RegisterSeatServiceServer(grpcServer, seatHandler)

	// Enable gRPC Reflection untuk kemudahan testing (via Postman / Evans CLI)
	reflection.Register(grpcServer)

	fmt.Printf("Seat gRPC Service is running on port %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to serve gRPC: %v", err))
	}
}