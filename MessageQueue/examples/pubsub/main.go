// Package main demonstrates the publish-subscribe pattern using GoMQ topics.
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
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("GoMQ Pub/Sub Example")
	log.Println("====================")

	// Create and start the broker
	cfg := broker.DefaultConfig()
	b := broker.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer b.Shutdown(ctx)

	// Create a topic for events
	topicName := "system-events"
	topic, err := b.CreateTopic(topicName, broker.WithTopicBufferSize(100))
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("Created topic: %s", topicName)

	// Create multiple subscribers
	numSubscribers := 3
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start subscribers
	for i := 1; i <= numSubscribers; i++ {
		subscriberID := fmt.Sprintf("subscriber-%d", i)
		subscriber, err := topic.Subscribe(subscriberID)
		if err != nil {
			log.Fatalf("Failed to subscribe %s: %v", subscriberID, err)
		}
		log.Printf("Created subscriber: %s", subscriberID)

		wg.Add(1)
		go func(id string, msgCh <-chan *message.Message) {
			defer wg.Done()
			processMessages(id, msgCh, stopCh)
		}(subscriberID, subscriber.Messages())
	}

	// Create a filtered subscriber that only receives high-priority messages
	filteredSubscriber, err := topic.SubscribeWithFilter("priority-subscriber", func(msg *message.Message) bool {
		return msg.Priority() >= message.PriorityHigh
	})
	if err != nil {
		log.Fatalf("Failed to create filtered subscriber: %v", err)
	}
	log.Printf("Created filtered subscriber: priority-subscriber (high priority only)")

	wg.Add(1)
	go func() {
		defer wg.Done()
		processMessages("priority-subscriber", filteredSubscriber.Messages(), stopCh)
	}()

	// Start publisher
	wg.Add(1)
	go func() {
		defer wg.Done()
		publishEvents(ctx, topic, stopCh)
	}()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Run for a while or until interrupted
	select {
	case <-sigCh:
		log.Println("\nReceived shutdown signal")
	case <-time.After(10 * time.Second):
		log.Println("\nDemo complete")
	}

	// Shutdown
	close(stopCh)
	wg.Wait()

	// Print statistics
	stats := topic.Stats()
	log.Println("\nTopic Statistics:")
	log.Printf("  Name:            %s", stats.Name)
	log.Printf("  Published:       %d", stats.PublishCount)
	log.Printf("  Delivered:       %d", stats.DeliveryCount)
	log.Printf("  Subscribers:     %d", stats.SubscriberCount)
}

// publishEvents publishes sample events to the topic.
func publishEvents(ctx context.Context, topic interface {
	Publish(ctx context.Context, msg *message.Message) error
	Name() string
}, stopCh <-chan struct{}) {
	events := []struct {
		eventType string
		priority  int
		data      string
	}{
		{"user.login", message.PriorityNormal, `{"user_id": "123", "ip": "192.168.1.1"}`},
		{"user.logout", message.PriorityLow, `{"user_id": "123"}`},
		{"order.created", message.PriorityHigh, `{"order_id": "ORD-001", "amount": 99.99}`},
		{"system.alert", message.PriorityHigh, `{"level": "warning", "message": "High CPU usage"}`},
		{"user.signup", message.PriorityNormal, `{"user_id": "456", "email": "new@example.com"}`},
		{"payment.processed", message.PriorityHigh, `{"payment_id": "PAY-001", "status": "success"}`},
		{"inventory.low", message.PriorityNormal, `{"product_id": "PROD-123", "quantity": 5}`},
		{"system.critical", message.PriorityHigh, `{"level": "critical", "message": "Database connection lost"}`},
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	eventIndex := 0

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if eventIndex >= len(events) {
				eventIndex = 0
			}

			event := events[eventIndex]
			eventIndex++

			// Create message using builder pattern
			msg := message.NewBuilder().
				WithStringPayload(event.data).
				WithHeader("event-type", event.eventType).
				WithHeader("source", "pubsub-example").
				WithPriority(event.priority).
				WithTTL(5 * time.Minute).
				Build()

			if err := topic.Publish(ctx, msg); err != nil {
				log.Printf("Failed to publish event: %v", err)
				continue
			}

			priorityStr := "NORMAL"
			if event.priority >= message.PriorityHigh {
				priorityStr = "HIGH"
			} else if event.priority <= message.PriorityLow {
				priorityStr = "LOW"
			}

			log.Printf("[PUBLISH] Event: %-20s Priority: %-6s ID: %s",
				event.eventType, priorityStr, msg.ID()[:8])
		}
	}
}

// processMessages processes messages received by a subscriber.
func processMessages(subscriberID string, msgCh <-chan *message.Message, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case msg, ok := <-msgCh:
			if !ok {
				return
			}

			eventType := msg.Header("event-type")
			log.Printf("[RECEIVE] %s <- Event: %-20s ID: %s",
				subscriberID, eventType, msg.ID()[:8])

			// Simulate processing time
			time.Sleep(50 * time.Millisecond)
		}
	}
}
