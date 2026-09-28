package event

import (
	"context"
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

type EventPublisher struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	queueName string
}

func NewEventPublisher(amqpURL string, queueName string) (*EventPublisher, error) {
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

	return &EventPublisher{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

func (p *EventPublisher) PublishBookingCreated(ctx context.Context, event BookingCreatedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.ch.PublishWithContext(
		ctx,
		"",          // exchange
		p.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("Published BookingCreatedEvent: %s to queue %s", event.BookingID, p.queueName)
	return nil
}

func (p *EventPublisher) Close() {
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}