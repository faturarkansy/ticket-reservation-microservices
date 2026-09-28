package handler

import (
	"context"
	"fmt"

	"booking-service/event"
	"booking-service/pb"
	seatpb "booking-service/pb/seatpb"
	"booking-service/repository"

	"github.com/google/uuid"
)

type BookingHandler struct {
	pb.UnimplementedBookingServiceServer
	repo       repository.BookingRepository
	seatClient seatpb.SeatServiceClient
	publisher  *event.EventPublisher
}

func NewBookingHandler(repo repository.BookingRepository, seatClient seatpb.SeatServiceClient, publisher *event.EventPublisher) *BookingHandler {
	return &BookingHandler{
		repo:       repo,
		seatClient: seatClient,
		publisher:  publisher,
	}
}

func (h *BookingHandler) CreateBooking(ctx context.Context, req *pb.CreateBookingRequest) (*pb.CreateBookingResponse, error) {
	// 1. Panggil gRPC Seat Service untuk mereservasi kursi (termasuk Redis Lock)
	reserveResp, err := h.seatClient.ReserveSeat(ctx, &seatpb.ReserveSeatRequest{
		SeatId: req.GetSeatId(),
		UserId: req.GetUserId(),
	})
	if err != nil {
		return &pb.CreateBookingResponse{
			Status:  "FAILED",
			Message: "Failed to communicate with Seat Service: " + err.Error(),
		}, nil
	}

	if !reserveResp.GetSuccess() {
		return &pb.CreateBookingResponse{
			Status:  "FAILED",
			Message: reserveResp.GetMessage(),
		}, nil
	}

	// 2. Buat record transaksi di database Booking
	bookingID := "bk-" + uuid.New().String()[:8]
	defaultPrice := 150000.00 // simulasi harga tiket

	booking := &pb.GetBookingResponse{
		BookingId:  bookingID,
		UserId:     req.GetUserId(),
		EventId:    req.GetEventId(),
		SeatId:     req.GetSeatId(),
		Status:     "PENDING",
		TotalPrice: defaultPrice,
	}

	if err := h.repo.CreateBooking(ctx, booking); err != nil {
		return &pb.CreateBookingResponse{
			Status:  "FAILED",
			Message: "Failed to save booking record: " + err.Error(),
		}, nil
	}

	// 3. Terbitkan Asynchronous Event ke RabbitMQ
	eventPayload := event.BookingCreatedEvent{
		BookingID:  bookingID,
		UserID:     req.GetUserId(),
		EventID:    req.GetEventId(),
		SeatID:     req.GetSeatId(),
		TotalPrice: defaultPrice,
	}
	
	if err := h.publisher.PublishBookingCreated(ctx, eventPayload); err != nil {
		fmt.Printf("Warning: Failed to publish event: %v\n", err)
	}

	return &pb.CreateBookingResponse{
		BookingId:  bookingID,
		Status:     "PENDING",
		TotalPrice: defaultPrice,
		Message:    "Booking initiated successfully and lock acquired!",
	}, nil
}

func (h *BookingHandler) GetBooking(ctx context.Context, req *pb.GetBookingRequest) (*pb.GetBookingResponse, error) {
	return h.repo.GetBookingByID(ctx, req.GetBookingId())
}