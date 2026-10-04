package event

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"seat-service/repository"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type PaymentEvent struct {
	PaymentID string  `json:"payment_id"`
	BookingID string  `json:"booking_id"`
	SeatID    string  `json:"seat_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"` // "SUCCESS" atau "FAILED"
}

type SeatPaymentConsumer struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	seatRepo    repository.SeatRepository
	redisClient *redis.Client // 🟢 Ditambahkan untuk penghapusan lock Redis
}

func NewSeatPaymentConsumer(amqpURL string, seatRepo repository.SeatRepository, redisClient *redis.Client) (*SeatPaymentConsumer, error) {
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
		"seat_payment_queue", // Queue khusus seat-service
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Bind queue ke event payment.failed untuk melepaskan kursi
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

	return &SeatPaymentConsumer{
		conn:        conn,
		ch:          ch,
		seatRepo:    seatRepo,
		redisClient: redisClient,
	}, nil
}

func (c *SeatPaymentConsumer) ListenPaymentEvents() {
	msgs, err := c.ch.Consume(
		"seat_payment_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register seat payment consumer: %v", err)
	}

	log.Println(" [*] Seat Service Payment Consumer listening for 'payment.failed' events...")

	go func() {
		for d := range msgs {
			var event PaymentEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshaling payment event in seat-service: %v", err)
				continue
			}

			if event.Status == "FAILED" && event.SeatID != "" {
				log.Printf("📥 [SAGA COMPENSATION]: Releasing SeatID: %s due to failed payment...", event.SeatID)

				ctx := context.Background()
				// Step 1: Update status kursi di PostgreSQL kembali menjadi AVAILABLE
				err := c.seatRepo.UpdateSeatStatus(ctx, event.SeatID, "AVAILABLE")
				if err != nil {
					log.Printf("❌ Failed to update seat status to AVAILABLE: %v", err)
				} else {
					log.Printf("✅ SeatID: %s successfully updated to AVAILABLE in DB", event.SeatID)
				}

				// Step 2: Hapus lock di Redis langsung menggunakan Redis client
				lockKey := fmt.Sprintf("seat_lock:%s", event.SeatID)
				err = c.redisClient.Del(ctx, lockKey).Err()
				if err != nil {
					log.Printf("⚠️ Failed or key not found when releasing Redis lock: %v", err)
				} else {
					log.Printf("🔓 Lock released for SeatID: %s in Redis (Key: %s)", event.SeatID, lockKey)
				}
			}
		}
	}()
}