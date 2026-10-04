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
	Status     string  `json:"status"`
}

type EventPublisher struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	exchangeName string
}

func NewEventPublisher(amqpURL string, exchangeName string) (*EventPublisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Declare Topic Exchange 'booking_events'
	err = ch.ExchangeDeclare(
		exchangeName, // name ("booking_events")
		"topic",        // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &EventPublisher{
		conn:         conn,
		ch:           ch,
		exchangeName: exchangeName,
	}, nil
}

func (p *EventPublisher) PublishBookingCreated(ctx context.Context, event BookingCreatedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Publish ke Exchange 'booking_events' dengan Routing Key 'booking.created'
	err = p.ch.PublishWithContext(
		ctx,
		p.exchangeName,    //  Exchange 'booking_events'
		"booking.created", //  Routing Key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("🚀 Published BookingCreatedEvent: %s to exchange %s", event.BookingID, p.exchangeName)
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