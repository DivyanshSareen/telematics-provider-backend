package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
)

type VehicleLocation struct {
    VehicleId string  `json:"vehicleId"`
    DriverId  string  `json:"driverId"`
    Latitude  float64 `json:"latitude"`
    Longitude float64 `json:"longitude"`
    Timestamp string  `json:"timestamp"`
}

type Consumer struct {
    ready chan bool
    db    *sql.DB
}

func NewConsumer(db *sql.DB) *Consumer {
    return &Consumer{
        ready: make(chan bool),
        db:    db,
    }
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
    close(consumer.ready)
    return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for {
        select {
        case message := <-claim.Messages():
            if message == nil {
                return nil
            }

            log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", 
                string(message.Value), message.Timestamp, message.Topic)

            // Parse the JSON message
            var location VehicleLocation
            if err := json.Unmarshal(message.Value, &location); err != nil {
                log.Printf("Error unmarshaling message: %v", err)
                session.MarkMessage(message, "")
                continue
            }

            // Validate that vehicle and driver exist before inserting
            if !consumer.validateReferences(location.VehicleId, location.DriverId) {
                log.Printf("Invalid vehicle_id or driver_id: %s, %s", location.VehicleId, location.DriverId)
                session.MarkMessage(message, "")
                continue
            }

            // Insert into TimescaleDB
            if err := consumer.insertVehicleLocation(location); err != nil {
                log.Printf("Error inserting into database: %v", err)
                // Don't mark message as processed if DB insert fails
                continue
            }

            session.MarkMessage(message, "")

        case <-session.Context().Done():
            return nil
        }
    }
}

func (consumer *Consumer) validateReferences(vehicleId, driverId string) bool {
    // Check if vehicle exists
    var vehicleCount int
    err := consumer.db.QueryRow("SELECT COUNT(*) FROM vehicles WHERE vehicle_id = $1", vehicleId).Scan(&vehicleCount)
    if err != nil || vehicleCount == 0 {
        log.Printf("Vehicle %s not found in database", vehicleId)
        return false
    }

    // Check if driver exists
    var driverCount int
    err = consumer.db.QueryRow("SELECT COUNT(*) FROM drivers WHERE driver_id = $1", driverId).Scan(&driverCount)
    if err != nil || driverCount == 0 {
        log.Printf("Driver %s not found in database", driverId)
        return false
    }

    return true
}

func (consumer *Consumer) insertVehicleLocation(location VehicleLocation) error {
    // Parse timestamp - handle multiple formats
    var timestamp time.Time
    var err error

    if location.Timestamp != "" {
        // Try RFC3339 first
        timestamp, err = time.Parse(time.RFC3339, location.Timestamp)
        if err != nil {
            // Try other common formats
            formats := []string{
                "2006-01-02T15:04:05Z07:00",
                "2006-01-02T15:04:05Z",
                "2006-01-02 15:04:05",
            }
            
            for _, format := range formats {
                if timestamp, err = time.Parse(format, location.Timestamp); err == nil {
                    break
                }
            }
            
            if err != nil {
                log.Printf("Error parsing timestamp %s: %v", location.Timestamp, err)
                timestamp = time.Now() // Use current time as fallback
            }
        }
    } else {
        timestamp = time.Now()
    }

    query := `
        INSERT INTO vehicle_locations (time, vehicle_id, driver_id, latitude, longitude)
        VALUES ($1, $2, $3, $4, $5)
    `

    _, err = consumer.db.Exec(query, timestamp, location.VehicleId, location.DriverId, location.Latitude, location.Longitude)
    if err != nil {
        return err
    }

    log.Printf("Inserted vehicle location: %s (driver: %s) at (%f, %f) at %v", 
        location.VehicleId, location.DriverId, location.Latitude, location.Longitude, timestamp)
    
    return nil
}

func main() {
    kafkaBroker := os.Getenv("KAFKA_BROKER")
    if kafkaBroker == "" {
        kafkaBroker = "localhost:9092"
    }

    databaseURL := os.Getenv("DATABASE_URL")
    if databaseURL == "" {
        databaseURL = "postgres://postgres:password@localhost:5432/gpsdb?sslmode=disable"
    }

    // Connect to database
    db, err := sql.Open("postgres", databaseURL)
    if err != nil {
        log.Fatalln("Failed to connect to database:", err)
    }
    defer db.Close()

    // Configure connection pool
    db.SetMaxOpenConns(25)
    db.SetMaxIdleConns(25)
    db.SetConnMaxLifetime(5 * time.Minute)

    // Test database connection
    if err := db.Ping(); err != nil {
        log.Fatalln("Failed to ping database:", err)
    }

    log.Println("Connected to database successfully")

    // Kafka consumer configuration
    config := sarama.NewConfig()
    config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
    config.Consumer.Offsets.Initial = sarama.OffsetOldest
    config.Consumer.Group.Session.Timeout = 10 * time.Second
    config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

    consumer := NewConsumer(db)

    ctx, cancel := context.WithCancel(context.Background())
    client, err := sarama.NewConsumerGroup([]string{kafkaBroker}, "gps-consumer-group", config)
    if err != nil {
        log.Panicf("Error creating consumer group client: %v", err)
    }

    wg := &sync.WaitGroup{}
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            if err := client.Consume(ctx, []string{"gps-topic"}, consumer); err != nil {
                log.Panicf("Error from consumer: %v", err)
            }
            // Check if context was cancelled, signaling that the consumer should stop
            if ctx.Err() != nil {
                return
            }
            consumer.ready = make(chan bool)
        }
    }()

    <-consumer.ready
    log.Println("Sarama consumer up and running!...")

    sigterm := make(chan os.Signal, 1)
    signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
    select {
    case <-ctx.Done():
        log.Println("terminating: context cancelled")
    case <-sigterm:
        log.Println("terminating: via signal")
    }
    cancel()
    wg.Wait()

    if err = client.Close(); err != nil {
        log.Panicf("Error closing client: %v", err)
    }
}
