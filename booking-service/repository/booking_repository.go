package repository

import (
	"context"
	"database/sql"
	"fmt"

	"booking-service/pb"

	_ "github.com/lib/pq"
)

type BookingRepository interface {
	CreateBooking(ctx context.Context, booking *pb.GetBookingResponse) error
	GetBookingByID(ctx context.Context, bookingID string) (*pb.GetBookingResponse, error)
	// 🟢 1. Tambahkan kontrak method baru di interface
	UpdateBookingStatus(ctx context.Context, bookingID string, status string) error
}

type bookingRepository struct {
	db *sql.DB
}

func NewBookingRepository(db *sql.DB) BookingRepository {
	return &bookingRepository{db: db}
}

func (r *bookingRepository) CreateBooking(ctx context.Context, booking *pb.GetBookingResponse) error {
	query := `
		INSERT INTO bookings (id, user_id, event_id, seat_id, status, total_price)
		VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := r.db.ExecContext(ctx, query,
		booking.BookingId,
		booking.UserId,
		booking.EventId,
		booking.SeatId,
		booking.Status,
		booking.TotalPrice,
	)
	if err != nil {
		return fmt.Errorf("failed to insert booking: %w", err)
	}

	return nil
}

func (r *bookingRepository) GetBookingByID(ctx context.Context, bookingID string) (*pb.GetBookingResponse, error) {
	query := `SELECT id, user_id, event_id, seat_id, status, total_price FROM bookings WHERE id = $1`
	row := r.db.QueryRowContext(ctx, query, bookingID)

	var b pb.GetBookingResponse
	if err := row.Scan(&b.BookingId, &b.UserId, &b.EventId, &b.SeatId, &b.Status, &b.TotalPrice); err != nil {
		return nil, err
	}

	return &b, nil
}

// 🟢 2. Implementasi fungsi UpdateBookingStatus di struct bookingRepository
func (r *bookingRepository) UpdateBookingStatus(ctx context.Context, bookingID string, status string) error {
	query := `UPDATE bookings SET status = $1, updated_at = NOW() WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, status, bookingID)
	if err != nil {
		return fmt.Errorf("failed to update booking status: %w", err)
	}
	return nil
}