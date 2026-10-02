package event

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BookingCreatedEvent struct {
	BookingID  string  `json:"booking_id"`
	UserID     string  `json:"user_id"`
	EventID    string  `json:"event_id"`
	SeatID     string  `json:"seat_id"`
	TotalPrice float64 `json:"total_price"`
}

type EventConsumer struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
}

func NewEventConsumer(amqpURL string, queueName string) (*EventConsumer, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &EventConsumer{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

func (c *EventConsumer) StartConsuming() error {
	msgs, err := c.ch.Consume(
		c.queueName, // queue
		"",          // consumer tag
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var event BookingCreatedEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshaling event payload: %v", err)
				continue
			}

			// Simulasi Pemrosesan Notifikasi (Email / WA / E-Ticket)
			c.processNotification(event)
		}
	}()

	log.Printf(" [*] Notification Service listening for messages on queue: %s. To exit press CTRL+C", c.queueName)
	<-forever

	return nil
}

func (c *EventConsumer) processNotification(event BookingCreatedEvent) {
	log.Println("------------------------------------------------------------")
	log.Printf("📩 [NOTIFICATION SERVICE] New Event Received!")
	log.Printf("   ├─ Booking ID  : %s", event.BookingID)
	log.Printf("   ├─ User ID     : %s", event.UserID)
	log.Printf("   ├─ Event ID    : %s", event.EventID)
	log.Printf("   ├─ Seat ID     : %s", event.SeatID)
	log.Printf("   └─ Total Price : Rp %.2f", event.TotalPrice)
	log.Printf("✅ [ACTION] Simulation: Email/WhatsApp confirmation ticket sent to User %s", event.UserID)
	log.Println("------------------------------------------------------------")
}

func (c *EventConsumer) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}