package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PaymentEvent payload untuk RabbitMQ (Mendukung SUCCESS dan FAILED)
type PaymentEvent struct {
	PaymentID string  `json:"payment_id"`
	BookingID string  `json:"booking_id"`
	SeatID    string  `json:"seat_id"` // 🟢 Ditambahkan untuk pelepasan kursi di seat-service
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"` // "SUCCESS" atau "FAILED"
}

type PaymentPublisher struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	exchangeName string
}

func NewPaymentPublisher(amqpURL, exchangeName string) (*PaymentPublisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // "booking_events"
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

	return &PaymentPublisher{conn: conn, ch: ch, exchangeName: exchangeName}, nil
}

// PublishPaymentEvent fleksibel mengirim dengan routingKey "payment.completed" atau "payment.failed"
func (p *PaymentPublisher) PublishPaymentEvent(ctx context.Context, event PaymentEvent, routingKey string) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = p.ch.PublishWithContext(
		ctx,
		p.exchangeName,
		routingKey, // 🟢 Routing key dinamis
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	log.Printf("💳 [PAYMENT EVENT PUBLISHED - %s]: PaymentID: %s | BookingID: %s | SeatID: %s | Status: %s",
		routingKey, event.PaymentID, event.BookingID, event.SeatID, event.Status)
	return nil
}

func main() {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@127.0.0.1:5672/"
	}

	publisher, err := NewPaymentPublisher(rabbitmqURL, "booking_events")
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ Publisher: %v", err)
	}
	defer publisher.conn.Close()
	defer publisher.ch.Close()

	log.Println("💳 Payment Service is starting on port :50053...")

	// 🟢 Simulasi skenario pembayaran GAGAL untuk pengujian Saga Compensation Logic Hari Ke-9
	simulatePaymentFailureProcess(publisher)
}

func simulatePaymentFailureProcess(pub *PaymentPublisher) {
	ctx := context.Background()

	log.Println(" [*] Payment Service Ready. Simulating payment failure scenario...")
	
	time.Sleep(2 * time.Second)
	
	paymentEvent := PaymentEvent{
		PaymentID: "pay-" + uuid.New().String()[:8],
		BookingID: "bk-4e30230f", // ID booking target pengujian
		SeatID:    "seat-2",       // ID seat yang akan dibatalkan
		UserID:    "usr-100",
		Amount:    150000.00,
		Status:    "FAILED",
	}

	// Publish event dengan routing key "payment.failed"
	if err := pub.PublishPaymentEvent(ctx, paymentEvent, "payment.failed"); err != nil {
		log.Printf("Failed to publish payment failed event: %v", err)
	}
}