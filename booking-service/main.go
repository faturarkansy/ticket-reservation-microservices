package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

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
	// 1. Koneksi Database PostgreSQL (Port 5433 & User booking_user)
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = "booking_user"
	}
	dbPass := os.Getenv("DB_PASSWORD")
	if dbPass == "" {
		dbPass = "booking_password"
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "booking_db"
	}
	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5433"
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("DB ping failed: %v", err)
	}
	log.Println("Connected to PostgreSQL (booking_db)!")

	// 2. Koneksi ke gRPC Seat Service Client
	seatServiceHost := os.Getenv("SEAT_SERVICE_HOST")
	if seatServiceHost == "" {
		seatServiceHost = "localhost:50051"
	}

	seatConn, err := grpc.NewClient(seatServiceHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Seat Service: %v", err)
	}
	defer seatConn.Close()
	seatClient := seatpb.NewSeatServiceClient(seatConn)

	// 3. Setup RabbitMQ Event Publisher
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@127.0.0.1:5672/"
	}

	exchangeName := os.Getenv("RABBITMQ_EXCHANGE")
	if exchangeName == "" {
		exchangeName = "booking_events"
	}

	publisher, err := event.NewEventPublisher(rabbitmqURL, exchangeName)
	if err != nil {
		log.Fatalf("Failed to initialize publisher: %v", err)
	}
	defer publisher.Close()

	// 4. Inisialisasi Repository & Handler
	repo := repository.NewBookingRepository(db)
	bookingHandler := handler.NewBookingHandler(repo, seatClient, publisher)

	// 🟢 4.5. Setup RabbitMQ Payment Consumer (Diletakkan di sini)
	paymentConsumer, err := event.NewPaymentConsumer(rabbitmqURL, repo)
	if err != nil {
		log.Fatalf("Failed to start Payment Consumer: %v", err)
	}
	paymentConsumer.ListenPaymentEvents()

	// 5. Start gRPC Server
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50052"
	}

	lis, err := net.Listen("tcp", "0.0.0.0:"+grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()

	// Register Booking Handler
	pb.RegisterBookingServiceServer(grpcServer, bookingHandler)

	// Register gRPC Reflection
	reflection.Register(grpcServer)

	log.Printf("Booking Service (gRPC) running on port %s...", grpcPort)

	for serviceName := range grpcServer.GetServiceInfo() {
		log.Printf("Registered gRPC Service: %s", serviceName)
	}

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}