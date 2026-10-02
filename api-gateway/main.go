package main

import (
	"fmt"
	"log"
	"net/http"

	"api-gateway/handler"
	"api-gateway/pb"
	bookingpb "api-gateway/pb/bookingpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Koneksi gRPC ke Seat Service (:50051)
	seatConn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Seat Service: %v", err)
	}
	defer seatConn.Close()
	seatClient := pb.NewSeatServiceClient(seatConn)
	seatHandler := handler.NewSeatHandler(seatClient)

	// 2. Koneksi gRPC ke Booking Service (:50052)
	bookingConn, err := grpc.Dial("127.0.0.1:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Booking Service: %v", err)
	}
	defer bookingConn.Close()
	bookingClient := bookingpb.NewBookingServiceClient(bookingConn)
	bookingHandler := handler.NewBookingHandler(bookingClient)

	// 3. Register HTTP Routes
	http.HandleFunc("/api/v1/seats", seatHandler.GetSeats)
	http.HandleFunc("/api/v1/bookings", bookingHandler.CreateBooking)

	// 4. Start Server
	port := ":8081"
	fmt.Printf("API Gateway is running on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		panic(err)
	}
}