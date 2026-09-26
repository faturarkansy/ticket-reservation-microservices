package main

import (
	"fmt"
	"log"
	"net/http"

	"api-gateway/handler"
	"api-gateway/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Inisialisasi Koneksi gRPC Client ke Seat Service (:50051)
	seatServiceAddr := "localhost:50051"
	conn, err := grpc.Dial(seatServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Seat Service at %s: %v", seatServiceAddr, err)
	}
	defer conn.Close()

	seatClient := pb.NewSeatServiceClient(conn)
	seatHandler := handler.NewSeatHandler(seatClient)

	// 2. Setup Route Handlers
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "API Gateway is running"}`))
	})

	// Endpoint API REST untuk Seats
	http.HandleFunc("/api/v1/seats", seatHandler.GetSeats)

	// 3. Jalankan HTTP Server API Gateway
	port := ":8081"
	fmt.Printf("API Gateway is running on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		panic(err)
	}
}