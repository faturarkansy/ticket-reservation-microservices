package handler

import (
	"context"
	"fmt"
	"time"

	"seat-service/pb"
	"seat-service/repository"

	"github.com/redis/go-redis/v9"
)

type SeatHandler struct {
	pb.UnimplementedSeatServiceServer
	repo        repository.SeatRepository
	redisClient *redis.Client
}

func NewSeatHandler(repo repository.SeatRepository, redisClient *redis.Client) *SeatHandler {
	return &SeatHandler{
		repo:        repo,
		redisClient: redisClient,
	}
}

func (h *SeatHandler) GetSeats(ctx context.Context, req *pb.GetSeatsRequest) (*pb.GetSeatsResponse, error) {
	seats, err := h.repo.GetSeatsByEventID(ctx, req.GetEventId())
	if err != nil {
		return nil, err
	}

	return &pb.GetSeatsResponse{
		Seats: seats,
	}, nil
}

func (h *SeatHandler) ReserveSeat(ctx context.Context, req *pb.ReserveSeatRequest) (*pb.ReserveSeatResponse, error) {
	lockKey := fmt.Sprintf("lock:seat:%s", req.GetSeatId())
	
	// 1. Coba dapatkan Redis Lock (SETNX - Set if Not Exists) selama 10 detik
	acquired, err := h.redisClient.SetNX(ctx, lockKey, req.GetUserId(), 10*time.Second).Result()
	if err != nil {
		return &pb.ReserveSeatResponse{
			Success: false,
			Message: "Error acquiring lock: " + err.Error(),
		}, nil
	}

	if !acquired {
		return &pb.ReserveSeatResponse{
			Success: false,
			Message: "Seat is currently being reserved by another user. Please try again.",
		}, nil
	}

	// Release lock setelah transaksi selesai (atau ganti sesuai flow rilis saat pembayaran/expired)
	defer h.redisClient.Del(ctx, lockKey)

	// 2. Cek status kursi di database
	seat, err := h.repo.GetSeatByID(ctx, req.GetSeatId())
	if err != nil {
		return &pb.ReserveSeatResponse{
			Success: false,
			Message: "Seat not found",
		}, nil
	}

	if seat.Status != "AVAILABLE" {
		return &pb.ReserveSeatResponse{
			Success: false,
			Message: "Seat is no longer available",
		}, nil
	}

	// 3. Update status kursi menjadi RESERVED
	err = h.repo.UpdateSeatStatus(ctx, req.GetSeatId(), "RESERVED")
	if err != nil {
		return &pb.ReserveSeatResponse{
			Success: false,
			Message: "Failed to reserve seat",
		}, nil
	}

	return &pb.ReserveSeatResponse{
		Success: true,
		Message: "Seat reserved successfully!",
	}, nil
}

func (h *SeatHandler) ReleaseSeat(ctx context.Context, req *pb.ReleaseSeatRequest) (*pb.ReleaseSeatResponse, error) {
	err := h.repo.UpdateSeatStatus(ctx, req.GetSeatId(), "AVAILABLE")
	if err != nil {
		return &pb.ReleaseSeatResponse{
			Success: false,
			Message: "Failed to release seat",
		}, nil
	}

	return &pb.ReleaseSeatResponse{
		Success: true,
		Message: "Seat released successfully",
	}, nil
}