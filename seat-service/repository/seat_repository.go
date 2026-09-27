package repository

import (
	"context"
	"database/sql"
	"fmt"
	"seat-service/pb"

	_ "github.com/lib/pq"
)

type SeatRepository interface {
	GetSeatsByEventID(ctx context.Context, eventID string) ([]*pb.Seat, error)
	GetSeatByID(ctx context.Context, seatID string) (*pb.Seat, error)
	UpdateSeatStatus(ctx context.Context, seatID string, status string) error
}

type seatRepository struct {
	db *sql.DB
}

func NewSeatRepository(db *sql.DB) SeatRepository {
	return &seatRepository{db: db}
}

func (r *seatRepository) GetSeatsByEventID(ctx context.Context, eventID string) ([]*pb.Seat, error) {
	query := `SELECT id, event_id, seat_number, status, price FROM seats WHERE event_id = $1`
	rows, err := r.db.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("failed to query seats: %w", err)
	}
	defer rows.Close()

	var seats []*pb.Seat
	for rows.Next() {
		var seat pb.Seat
		if err := rows.Scan(&seat.Id, &seat.EventId, &seat.SeatNumber, &seat.Status, &seat.Price); err != nil {
			return nil, fmt.Errorf("failed to scan seat: %w", err)
		}
		seats = append(seats, &seat)
	}

	return seats, nil
}

func (r *seatRepository) GetSeatByID(ctx context.Context, seatID string) (*pb.Seat, error) {
	query := `SELECT id, event_id, seat_number, status, price FROM seats WHERE id = $1`
	row := r.db.QueryRowContext(ctx, query, seatID)

	var seat pb.Seat
	if err := row.Scan(&seat.Id, &seat.EventId, &seat.SeatNumber, &seat.Status, &seat.Price); err != nil {
		return nil, err
	}

	return &seat, nil
}

func (r *seatRepository) UpdateSeatStatus(ctx context.Context, seatID string, status string) error {
	query := `UPDATE seats SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, status, seatID)
	if err != nil {
		return fmt.Errorf("failed to update seat status: %w", err)
	}
	return nil
}