package handler

import (
	"context"
	"fmt"
	"seat-service/pb"
)

type SeatHandler struct {
	pb.UnimplementedSeatServiceServer
}

func NewSeatHandler() *SeatHandler {
	return &SeatHandler{}
}

func (h *SeatHandler) GetSeats(ctx context.Context, req *pb.GetSeatsRequest) (*pb.GetSeatsResponse, error) {
	fmt.Printf("Fetching seats for event_id: %s\n", req.GetEventId())

	// Sample mock data response
	seats := []*pb.Seat{
		{
			Id:         "seat-1",
			EventId:    req.GetEventId(),
			SeatNumber: "A1",
			Status:     "AVAILABLE",
			Price:      150000,
		},
		{
			Id:         "seat-2",
			EventId:    req.GetEventId(),
			SeatNumber: "A2",
			Status:     "AVAILABLE",
			Price:      150000,
		},
	}

	return &pb.GetSeatsResponse{
		Seats: seats,
	}, nil
}

func (h *SeatHandler) ReserveSeat(ctx context.Context, req *pb.ReserveSeatRequest) (*pb.ReserveSeatResponse, error) {
	fmt.Printf("Reserving seat %s for user %s\n", req.GetSeatId(), req.GetUserId())

	return &pb.ReserveSeatResponse{
		Success: true,
		Message: "Seat reserved successfully",
	}, nil
}

func (h *SeatHandler) ReleaseSeat(ctx context.Context, req *pb.ReleaseSeatRequest) (*pb.ReleaseSeatResponse, error) {
	fmt.Printf("Releasing seat %s\n", req.GetSeatId())

	return &pb.ReleaseSeatResponse{
		Success: true,
		Message: "Seat released successfully",
	}, nil
}