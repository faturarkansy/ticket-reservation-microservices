package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"seat-service/handler"
	"seat-service/pb"
	"seat-service/repository"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// 1. Inisialisasi Koneksi PostgreSQL (Port 5431 untuk seat_db)
	dbConnStr := "host=localhost port=5431 user=seat_user password=seat_password dbname=seat_db sslmode=disable"
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("PostgreSQL connection ping failed: %v", err)
	}
	fmt.Println("Connected to PostgreSQL (seat_db)...")

	// 2. Inisialisasi Koneksi Redis (Port 6380)
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
	})
	fmt.Println("Connected to Redis...")

	// 3. Setup Repository & Handler
	seatRepo := repository.NewSeatRepository(db)
	seatHandler := handler.NewSeatHandler(seatRepo, redisClient)

	// 4. Setup gRPC Server
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on port %s: %v", port, err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSeatServiceServer(grpcServer, seatHandler)
	reflection.Register(grpcServer)

	fmt.Printf("Seat gRPC Service is running on port %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to serve gRPC: %v", err))
	}
}