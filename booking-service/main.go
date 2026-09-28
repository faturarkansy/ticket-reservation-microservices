package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"booking-service/event"
	"booking-service/handler"
	"booking-service/pb"
	seatpb "booking-service/pb/seatpb"
	"booking-service/repository"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	// 1. Inisialisasi Database PostgreSQL (booking_db di Port 5433)
	dbConnStr := "host=localhost port=5433 user=booking_user password=booking_password dbname=booking_db sslmode=disable"
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("PostgreSQL connection ping failed: %v", err)
	}
	fmt.Println("Connected to PostgreSQL (booking_db)...")

	// 2. Inisialisasi gRPC Client ke Seat Service (Port :50051)
	seatConn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Seat Service: %v", err)
	}
	defer seatConn.Close()
	seatClient := seatpb.NewSeatServiceClient(seatConn)

	// 3. Inisialisasi RabbitMQ Publisher (Port 5672)
	amqpURL := "amqp://guest:guest@localhost:5672/"
	publisher, err := event.NewEventPublisher(amqpURL, "booking_created_queue")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer publisher.Close()
	fmt.Println("Connected to RabbitMQ Event Broker...")

	// 4. Setup Repository & Handler
	bookingRepo := repository.NewBookingRepository(db)
	bookingHandler := handler.NewBookingHandler(bookingRepo, seatClient, publisher)

	// 5. Setup gRPC Server
	port := ":50052"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on port %s: %v", port, err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBookingServiceServer(grpcServer, bookingHandler)
	reflection.Register(grpcServer)

	fmt.Printf("Booking gRPC Service is running on port %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to serve gRPC: %v", err))
	}
}