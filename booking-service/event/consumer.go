package event

import (
	"context"
	"encoding/json"
	"log"

	"booking-service/repository"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PaymentEvent struct generik untuk menangani event payment (SUCCESS / FAILED)
type PaymentEvent struct {
	PaymentID string  `json:"payment_id"`
	BookingID string  `json:"booking_id"`
	SeatID    string  `json:"seat_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"` // "SUCCESS" atau "FAILED"
}

type PaymentConsumer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	repo repository.BookingRepository
}

func NewPaymentConsumer(amqpURL string, repo repository.BookingRepository) (*PaymentConsumer, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	exchangeName := "booking_events"
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"booking_payment_queue", // Queue khusus booking-service
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 🟢 Bind queue ke routing key 'payment.completed'
	err = ch.QueueBind(
		q.Name,
		"payment.completed",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 🟢 Bind queue ke routing key 'payment.failed' (Untuk Saga Compensation)
	err = ch.QueueBind(
		q.Name,
		"payment.failed",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &PaymentConsumer{
		conn: conn,
		ch:   ch,
		repo: repo,
	}, nil
}

func (c *PaymentConsumer) ListenPaymentEvents() {
	msgs, err := c.ch.Consume(
		"booking_payment_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register payment consumer: %v", err)
	}

	log.Println(" [*] Booking Service Payment Consumer listening for 'payment.*' events...")

	go func() {
		for d := range msgs {
			var event PaymentEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshaling payment event: %v", err)
				continue
			}

			log.Printf("📥 [PAYMENT EVENT RECEIVED]: BookingID: %s | Status: %s", event.BookingID, event.Status)

			// 🟢 Logika Kompensasi Saga berdasarkan status pembayaran
			var targetStatus string
			if event.Status == "SUCCESS" {
				targetStatus = "PAID"
			} else if event.Status == "FAILED" {
				targetStatus = "CANCELLED"
			}

			if targetStatus != "" {
				err := c.repo.UpdateBookingStatus(context.Background(), event.BookingID, targetStatus)
				if err != nil {
					log.Printf("❌ Failed to update booking status for BookingID %s: %v", event.BookingID, err)
				} else {
					log.Printf("✅ BookingID: %s status updated to %s", event.BookingID, targetStatus)
				}
			}
		}
	}()
}