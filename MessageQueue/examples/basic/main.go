// Package main demonstrates basic usage of the GoMQ message queue.
//
// This example shows how to:
// - Create and configure a broker
// - Create a queue
// - Publish messages with various options
// - Subscribe and consume messages
// - Acknowledge messages
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/messagequeue/internal/broker"
	"github.com/messagequeue/internal/message"
	"github.com/messagequeue/internal/queue"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("GoMQ Basic Example")
	log.Println("==================")

	// Create a broker with default configuration
	cfg := broker.DefaultConfig()
	cfg.DefaultTTL = time.Hour
	cfg.EnableDLQ = true

	b := broker.New(cfg)

	// Start the broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer b.Shutdown(ctx)

	log.Println("Broker started successfully")

	// Create a queue
	q, err := b.CreateQueue("orders",
		broker.WithQueueMaxSize(10000),
		broker.WithQueueTTL(time.Hour),
		broker.WithQueueMaxRetries(3),
		broker.WithQueueAckTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	log.Println("Queue 'orders' created")

	// WaitGroup to coordinate goroutines
	var wg sync.WaitGroup

	// Start a consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, q)
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Publish some messages
	publishMessages(ctx, q)

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Print queue statistics
	printStats(q)

	// Demonstrate priority queue
	log.Println("\n--- Priority Queue Demo ---")
	demonstratePriority(ctx, b)

	// Demonstrate delayed messages
	log.Println("\n--- Delayed Messages Demo ---")
	demonstrateDelayedMessages(ctx, b)

	// Wait for interrupt signal
	log.Println("\nPress Ctrl+C to exit...")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Shutting down...")
	case <-time.After(5 * time.Second):
		log.Println("Example complete")
	}

	cancel()
	wg.Wait()
}

// runConsumer runs a message consumer.
func runConsumer(ctx context.Context, q *queue.Queue) {
	// Subscribe to the queue
	consumer, err := q.Subscribe("order-processor")
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
		return
	}
	defer consumer.Close()

	log.Println("Consumer started, waiting for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer shutting down")
			return
		case msg, ok := <-consumer.Messages():
			if !ok {
				log.Println("Consumer channel closed")
				return
			}
			log.Printf("Received message: %s - Payload: %s", msg.ID()[:8], msg.PayloadString())

			// Acknowledge the message
			if err := msg.Ack(); err != nil {
				log.Printf("Failed to ack message: %v", err)
			}
		}
	}
}

// publishMessages publishes sample messages to the queue.
func publishMessages(ctx context.Context, q *queue.Queue) {
	log.Println("Publishing messages...")

	// Simple message
	msg1 := message.New([]byte(`{"order_id": "ORD-001", "amount": 99.99}`))
	msg1.SetHeader("content-type", "application/json")
	msg1.SetHeader("correlation-id", "corr-123")

	if err := q.Publish(ctx, msg1); err != nil {
		log.Printf("Failed to publish message 1: %v", err)
	} else {
		log.Printf("Published message: %s", msg1.ID()[:8])
	}

	// High priority message
	msg2 := message.New([]byte(`{"order_id": "ORD-002", "amount": 1999.99, "priority": "urgent"}`))
	msg2.SetHeader("content-type", "application/json")
	msg2.SetPriority(message.PriorityHigh)

	if err := q.Publish(ctx, msg2); err != nil {
		log.Printf("Failed to publish message 2: %v", err)
	} else {
		log.Printf("Published high-priority message: %s", msg2.ID()[:8])
	}

	// Message with TTL
	msg3 := message.New([]byte(`{"order_id": "ORD-003", "flash_sale": true}`))
	msg3.SetTTL(5 * time.Minute)
	msg3.SetHeader("content-type", "application/json")

	if err := q.Publish(ctx, msg3); err != nil {
		log.Printf("Failed to publish message 3: %v", err)
	} else {
		log.Printf("Published message with TTL: %s (expires: %s)", msg3.ID()[:8], msg3.ExpiresAt().Format(time.RFC3339))
	}

	// Message using builder pattern
	msg4 := message.NewBuilder().
		WithStringPayload(`{"order_id": "ORD-004", "status": "pending"}`).
		WithHeader("content-type", "application/json").
		WithHeader("source", "web-app").
		WithPriority(message.PriorityNormal).
		WithTTL(time.Hour).
		Build()

	if err := q.Publish(ctx, msg4); err != nil {
		log.Printf("Failed to publish message 4: %v", err)
	} else {
		log.Printf("Published message (builder): %s", msg4.ID()[:8])
	}

	// Batch of messages
	log.Println("Publishing batch of messages...")
	for i := 5; i <= 10; i++ {
		msg := message.New([]byte(fmt.Sprintf(`{"order_id": "ORD-%03d", "batch": true}`, i)))
		msg.SetHeader("content-type", "application/json")
		msg.SetHeader("batch-id", "batch-001")

		if err := q.Publish(ctx, msg); err != nil {
			log.Printf("Failed to publish batch message %d: %v", i, err)
		}
	}
	log.Println("Batch published successfully")
}

// printStats prints queue statistics.
func printStats(q *queue.Queue) {
	stats := q.Stats()
	log.Println("\n--- Queue Statistics ---")
	log.Printf("Name:          %s", stats.Name)
	log.Printf("Depth:         %d", stats.Depth)
	log.Printf("Enqueue Count: %d", stats.EnqueueCount)
	log.Printf("Dequeue Count: %d", stats.DequeueCount)
	log.Printf("Ack Count:     %d", stats.AckCount)
	log.Printf("Nack Count:    %d", stats.NackCount)
	log.Printf("Consumer Count: %d", stats.ConsumerCount)
}

// demonstratePriority shows priority-based message ordering.
func demonstratePriority(ctx context.Context, b *broker.Broker) {
	q, err := b.CreateQueue("priority-demo", broker.WithQueuePriority(true))
	if err != nil {
		log.Printf("Failed to create priority queue: %v", err)
		return
	}

	// Publish messages with different priorities
	priorities := []struct {
		name     string
		priority int
	}{
		{"Low Priority Task", message.PriorityLow},
		{"Normal Priority Task", message.PriorityNormal},
		{"High Priority Task", message.PriorityHigh},
		{"Another Low Task", message.PriorityLow},
		{"Critical Task", message.PriorityHigh},
	}

	for _, p := range priorities {
		msg := message.New([]byte(p.name))
		msg.SetPriority(p.priority)
		q.Publish(ctx, msg)
		log.Printf("Published: %s (priority: %d)", p.name, p.priority)
	}

	log.Printf("Queue depth: %d", q.Depth())
	log.Println("Messages will be delivered in priority order (high -> normal -> low)")
}

// demonstrateDelayedMessages shows delayed message delivery.
func demonstrateDelayedMessages(ctx context.Context, b *broker.Broker) {
	q, err := b.CreateQueue("delayed-demo")
	if err != nil {
		log.Printf("Failed to create delayed queue: %v", err)
		return
	}

	// Publish a delayed message
	msg := message.New([]byte("This message is delayed by 3 seconds"))
	msg.SetDelay(3 * time.Second)

	log.Printf("Publishing delayed message at %s", time.Now().Format(time.RFC3339))
	log.Printf("Message will be available at %s", msg.DeliverAt().Format(time.RFC3339))

	if err := q.Publish(ctx, msg); err != nil {
		log.Printf("Failed to publish delayed message: %v", err)
		return
	}

	log.Printf("Message published, will be delivered in 3 seconds")
	log.Printf("Is message ready now? %v", msg.IsReady())

	// Wait and check again
	time.Sleep(4 * time.Second)
	log.Printf("Is message ready after delay? %v", msg.IsReady())
}
