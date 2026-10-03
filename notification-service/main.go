package main

import (
	"encoding/json"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BookingEvent struct {
	BookingID  string  `json:"booking_id"`
	UserID     string  `json:"user_id"`
	EventID    string  `json:"event_id"`
	SeatID     string  `json:"seat_id"`
	TotalPrice float64 `json:"total_price"`
	Status     string  `json:"status"`
}

func main() {
	// 1. Ambil URL RabbitMQ dari Env atau Default (Gunakan 127.0.0.1 agar pasti IPv4)
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@127.0.0.1:5672/" // 🟢 Diubah ke 127.0.0.1
	}

	// 2. Koneksi ke RabbitMQ
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// 3. Pastikan Exchange 'booking_events' Ada
	exchangeName := "booking_events"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	// 4. Deklarasikan Queue Khusus Notification Service
	q, err := ch.QueueDeclare(
		"notification_queue", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// 5. Bind Queue ke Exchange dengan Routing Key wildcard '#'
	err = ch.QueueBind(
		q.Name,       // queue name
		"#",          // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}

	// 6. Mulai Mengonsumsi Pesan (Consume)
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	log.Println(" [*] Notification Service is running on 127.0.0.1:5672. Waiting for booking events...")

	// 7. Loop Terus-Menerus Menunggu Pesan
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("📩 [RAW EVENT RECEIVED]: %s", string(d.Body))

			var event BookingEvent
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Printf("Error unmarshaling event JSON: %v", err)
				continue
			}

			sendNotification(event)
		}
	}()

	<-forever
}

func sendNotification(event BookingEvent) {
	log.Printf("==================================================")
	log.Printf("[NOTIFICATION SENT] User ID: %s", event.UserID)
	log.Printf("Booking ID : %s", event.BookingID)
	log.Printf("Event ID   : %s | Seat ID: %s", event.EventID, event.SeatID)
	log.Printf("Total Price: Rp %.2f", event.TotalPrice)
	log.Printf("Status     : %s", event.Status)
	log.Printf("Message    : Status pemesanan tiket Anda saat ini PENDING. Silakan selesaikan pembayaran!")
	log.Printf("==================================================")
}