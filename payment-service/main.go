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

// PaymentCompletedEvent payload untuk RabbitMQ
type PaymentCompletedEvent struct {
	PaymentID string  `json:"payment_id"`
	BookingID string  `json:"booking_id"`
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

func (p *PaymentPublisher) PublishPaymentCompleted(ctx context.Context, event PaymentCompletedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = p.ch.PublishWithContext(
		ctx,
		p.exchangeName,
		"payment.completed", // Routing Key khusus event pembayaran
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

	log.Printf("💳 [PAYMENT EVENT PUBLISHED]: PaymentID: %s | BookingID: %s | Status: %s", event.PaymentID, event.BookingID, event.Status)
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

	// Simulasi pemrosesan transaksi pembayaran
	// Untuk pengujian awal Hari Ke-8: Jalankan simulasi pembayaran otomatis
	simualtePaymentProcess(publisher)
}

func simualtePaymentProcess(pub *PaymentPublisher) {
	ctx := context.Background()

	log.Println(" [*] Payment Service Ready. Ready to process transaction requests...")
	
	// Simulasi event pembayaran berhasil untuk pengujian
	time.Sleep(2 * time.Second)
	
	paymentEvent := PaymentCompletedEvent{
		PaymentID: "pay-" + uuid.New().String()[:8],
		BookingID: "bk-4e30230f", // ID dari booking hari ke-7
		UserID:    "usr-100",
		Amount:    150000.00,
		Status:    "SUCCESS",
	}

	if err := pub.PublishPaymentCompleted(ctx, paymentEvent); err != nil {
		log.Printf("Failed to publish payment event: %v", err)
	}
}