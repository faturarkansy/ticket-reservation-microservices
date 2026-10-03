package event

import (
	"context"
	"encoding/json"
	"log"

	"booking-service/repository"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PaymentCompletedEvent struct {
	PaymentID string  `json:"payment_id"`
	BookingID string  `json:"booking_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
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
		"booking_payment_queue", // Queue khusus booking-service untuk mendengarkan event payment
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Bind queue ke routing key 'payment.completed'
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

	log.Println(" [*] Booking Service Payment Consumer listening for 'payment.completed' events...")

	go func() {
		for d := range msgs {
			var event PaymentCompletedEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshaling payment event: %v", err)
				continue
			}

			log.Printf("📥 [PAYMENT RECEIVED]: Updating BookingID: %s to status: %s", event.BookingID, event.Status)

			// Update status di database booking
			if event.Status == "SUCCESS" {
				err := c.repo.UpdateBookingStatus(context.Background(), event.BookingID, "PAID")
				if err != nil {
					log.Printf("❌ Failed to update booking status: %v", err)
				} else {
					log.Printf("✅ BookingID: %s successfully updated to PAID", event.BookingID)
				}
			}
		}
	}()
}