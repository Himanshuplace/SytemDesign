// Package queue provides tests for the Queue implementation.
package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/messagequeue/internal/message"
)

func TestNew(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	if q == nil {
		t.Fatal("New() returned nil")
	}

	if q.Name() != "test-queue" {
		t.Errorf("Name() = %q, want %q", q.Name(), "test-queue")
	}

	if q.IsClosed() {
		t.Error("New queue should not be closed")
	}

	if q.Depth() != 0 {
		t.Errorf("Depth() = %d, want 0", q.Depth())
	}
}

func TestQueue_Publish(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()
	msg := message.New([]byte("test payload"))

	err := q.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if q.Depth() != 1 {
		t.Errorf("Depth() = %d, want 1", q.Depth())
	}

	stats := q.Stats()
	if stats.EnqueueCount != 1 {
		t.Errorf("EnqueueCount = %d, want 1", stats.EnqueueCount)
	}
}

func TestQueue_Publish_NilMessage(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()
	err := q.Publish(ctx, nil)

	if err != ErrMessageNil {
		t.Errorf("Publish(nil) error = %v, want %v", err, ErrMessageNil)
	}
}

func TestQueue_Publish_ClosedQueue(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	q.Close()

	ctx := context.Background()
	msg := message.New([]byte("test"))
	err := q.Publish(ctx, msg)

	if err != ErrQueueClosed {
		t.Errorf("Publish() on closed queue error = %v, want %v", err, ErrQueueClosed)
	}
}

func TestQueue_Publish_QueueFull(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.MaxSize = 2
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()

	// Fill the queue
	q.Publish(ctx, message.New([]byte("msg1")))
	q.Publish(ctx, message.New([]byte("msg2")))

	// This should fail
	err := q.Publish(ctx, message.New([]byte("msg3")))
	if err != ErrQueueFull {
		t.Errorf("Publish() to full queue error = %v, want %v", err, ErrQueueFull)
	}
}

func TestQueue_PublishBatch(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()
	msgs := []*message.Message{
		message.New([]byte("msg1")),
		message.New([]byte("msg2")),
		message.New([]byte("msg3")),
	}

	err := q.PublishBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}

	if q.Depth() != 3 {
		t.Errorf("Depth() = %d, want 3", q.Depth())
	}
}

func TestQueue_Subscribe(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	consumer, err := q.Subscribe("consumer-1")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	if consumer == nil {
		t.Fatal("Subscribe() returned nil consumer")
	}

	if consumer.ID() != "consumer-1" {
		t.Errorf("Consumer ID = %q, want %q", consumer.ID(), "consumer-1")
	}

	if q.ConsumerCount() != 1 {
		t.Errorf("ConsumerCount() = %d, want 1", q.ConsumerCount())
	}
}

func TestQueue_Subscribe_Duplicate(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	_, err := q.Subscribe("consumer-1")
	if err != nil {
		t.Fatalf("First Subscribe() error = %v", err)
	}

	_, err = q.Subscribe("consumer-1")
	if err != ErrConsumerExists {
		t.Errorf("Duplicate Subscribe() error = %v, want %v", err, ErrConsumerExists)
	}
}

func TestQueue_Subscribe_ClosedQueue(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	q.Close()

	_, err := q.Subscribe("consumer-1")
	if err != ErrQueueClosed {
		t.Errorf("Subscribe() on closed queue error = %v, want %v", err, ErrQueueClosed)
	}
}

func TestQueue_PublishAndConsume(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	// Create consumer first
	consumer, err := q.Subscribe("consumer-1")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	// Publish message
	ctx := context.Background()
	msg := message.New([]byte("test payload"))
	err = q.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// Receive message
	select {
	case received := <-consumer.Messages():
		if received == nil {
			t.Fatal("Received nil message")
		}
		if string(received.Payload()) != "test payload" {
			t.Errorf("Payload = %q, want %q", string(received.Payload()), "test payload")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestQueue_Ack(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	// Publish and receive
	ctx := context.Background()
	msg := message.New([]byte("test"))
	q.Publish(ctx, msg)

	select {
	case received := <-consumer.Messages():
		// Acknowledge the message
		err := received.Ack()
		if err != nil {
			t.Errorf("Ack() error = %v", err)
		}

		stats := q.Stats()
		if stats.AckCount != 1 {
			t.Errorf("AckCount = %d, want 1", stats.AckCount)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestQueue_Nack_Requeue(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	cfg.MaxRetries = 5
	q := New(cfg)
	defer q.Close()

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	// Publish and receive
	ctx := context.Background()
	msg := message.New([]byte("test"))
	q.Publish(ctx, msg)

	// First receive
	select {
	case received := <-consumer.Messages():
		// Nack with requeue
		err := received.Nack(true)
		if err != nil {
			t.Errorf("Nack() error = %v", err)
		}

		stats := q.Stats()
		if stats.NackCount != 1 {
			t.Errorf("NackCount = %d, want 1", stats.NackCount)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for first message")
	}

	// Message should be requeued and delivered again
	select {
	case received := <-consumer.Messages():
		if received == nil {
			t.Fatal("Requeued message not received")
		}
		received.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for requeued message")
	}
}

func TestQueue_Priority(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.EnablePriority = true
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	ctx := context.Background()

	// Publish messages with different priorities (low first, then high)
	lowPriority := message.New([]byte("low"))
	lowPriority.SetPriority(message.PriorityLow)

	normalPriority := message.New([]byte("normal"))
	normalPriority.SetPriority(message.PriorityNormal)

	highPriority := message.New([]byte("high"))
	highPriority.SetPriority(message.PriorityHigh)

	// Publish in order: low, normal, high
	q.Publish(ctx, lowPriority)
	q.Publish(ctx, normalPriority)
	q.Publish(ctx, highPriority)

	// Give time for dispatch
	time.Sleep(100 * time.Millisecond)

	// Should receive in priority order: high, normal, low
	expected := []string{"high", "normal", "low"}
	for i, exp := range expected {
		select {
		case msg := <-consumer.Messages():
			if string(msg.Payload()) != exp {
				t.Errorf("Message %d = %q, want %q", i, string(msg.Payload()), exp)
			}
			msg.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}
}

func TestQueue_MessageWithTTL(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.DefaultTTL = 0 // No default TTL
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()

	// Create message with short TTL
	msg := message.New([]byte("expires soon"))
	msg.SetTTL(50 * time.Millisecond)

	err := q.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// Wait for message to expire
	time.Sleep(100 * time.Millisecond)

	// Create consumer after expiration
	consumer, _ := q.Subscribe("consumer-1")

	// Should not receive expired message
	select {
	case <-consumer.Messages():
		t.Error("Should not receive expired message")
	case <-time.After(500 * time.Millisecond):
		// Expected timeout
	}
}

func TestQueue_DelayedMessage(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	ctx := context.Background()

	// Create delayed message
	msg := message.New([]byte("delayed"))
	msg.SetDelay(500 * time.Millisecond)

	publishTime := time.Now()
	q.Publish(ctx, msg)

	// Should receive message after delay
	select {
	case received := <-consumer.Messages():
		elapsed := time.Since(publishTime)
		if elapsed < 400*time.Millisecond {
			t.Errorf("Message delivered too early: %v", elapsed)
		}
		if string(received.Payload()) != "delayed" {
			t.Errorf("Payload = %q, want %q", string(received.Payload()), "delayed")
		}
		received.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for delayed message")
	}
}

func TestQueue_Deduplication(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()

	// Publish message with dedup ID
	msg1 := message.New([]byte("original"))
	msg1.SetDeduplicationID("dedup-123")

	err := q.Publish(ctx, msg1)
	if err != nil {
		t.Fatalf("First Publish() error = %v", err)
	}

	// Publish duplicate
	msg2 := message.New([]byte("duplicate"))
	msg2.SetDeduplicationID("dedup-123")

	err = q.Publish(ctx, msg2)
	if err != nil {
		t.Fatalf("Duplicate Publish() error = %v", err)
	}

	// Should only have one message
	if q.Depth() != 1 {
		t.Errorf("Depth() = %d, want 1 (duplicate should be ignored)", q.Depth())
	}
}

func TestQueue_Purge(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()

	// Publish messages
	for i := 0; i < 5; i++ {
		q.Publish(ctx, message.New([]byte("msg")))
	}

	if q.Depth() != 5 {
		t.Errorf("Depth before purge = %d, want 5", q.Depth())
	}

	err := q.Purge()
	if err != nil {
		t.Fatalf("Purge() error = %v", err)
	}

	if q.Depth() != 0 {
		t.Errorf("Depth after purge = %d, want 0", q.Depth())
	}
}

func TestQueue_Close(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	q := New(cfg)

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	err := q.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if !q.IsClosed() {
		t.Error("Queue should be closed")
	}

	if !consumer.IsClosed() {
		t.Error("Consumer should be closed when queue closes")
	}

	// Double close should return error
	err = q.Close()
	if err != ErrQueueClosed {
		t.Errorf("Second Close() error = %v, want %v", err, ErrQueueClosed)
	}
}

func TestQueue_Stats(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()

	// Create consumer
	consumer, _ := q.Subscribe("consumer-1")

	// Publish messages
	for i := 0; i < 3; i++ {
		q.Publish(ctx, message.New([]byte("msg")))
	}

	// Consume and ack one
	select {
	case msg := <-consumer.Messages():
		msg.Ack()
	case <-time.After(time.Second):
		t.Fatal("Timeout")
	}

	stats := q.Stats()

	if stats.Name != "test-queue" {
		t.Errorf("Name = %q, want %q", stats.Name, "test-queue")
	}

	if stats.EnqueueCount != 3 {
		t.Errorf("EnqueueCount = %d, want 3", stats.EnqueueCount)
	}

	if stats.AckCount != 1 {
		t.Errorf("AckCount = %d, want 1", stats.AckCount)
	}

	if stats.ConsumerCount != 1 {
		t.Errorf("ConsumerCount = %d, want 1", stats.ConsumerCount)
	}

	if stats.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestQueue_MultipleConsumers_RoundRobin(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 1
	q := New(cfg)
	defer q.Close()

	// Create multiple consumers
	consumer1, _ := q.Subscribe("consumer-1")
	consumer2, _ := q.Subscribe("consumer-2")

	ctx := context.Background()

	// Publish messages
	for i := 0; i < 4; i++ {
		q.Publish(ctx, message.New([]byte("msg")))
	}

	// Give time for dispatch
	time.Sleep(100 * time.Millisecond)

	// Count messages per consumer
	count1, count2 := 0, 0

	for i := 0; i < 4; i++ {
		select {
		case msg := <-consumer1.Messages():
			count1++
			msg.Ack()
		case msg := <-consumer2.Messages():
			count2++
			msg.Ack()
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Messages should be distributed
	if count1 == 0 || count2 == 0 {
		t.Errorf("Messages not distributed: consumer1=%d, consumer2=%d", count1, count2)
	}
}

func TestQueue_ConsumerClose(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	consumer, _ := q.Subscribe("consumer-1")

	if q.ConsumerCount() != 1 {
		t.Errorf("ConsumerCount = %d, want 1", q.ConsumerCount())
	}

	err := consumer.Close()
	if err != nil {
		t.Errorf("Consumer.Close() error = %v", err)
	}

	if !consumer.IsClosed() {
		t.Error("Consumer should be closed")
	}

	if q.ConsumerCount() != 0 {
		t.Errorf("ConsumerCount after close = %d, want 0", q.ConsumerCount())
	}
}

func TestQueue_UnackedCount(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	cfg.AckTimeout = time.Hour // Long timeout to prevent auto-requeue
	q := New(cfg)
	defer q.Close()

	consumer, _ := q.Subscribe("consumer-1")

	ctx := context.Background()
	q.Publish(ctx, message.New([]byte("msg1")))
	q.Publish(ctx, message.New([]byte("msg2")))

	// Receive but don't ack
	select {
	case <-consumer.Messages():
	case <-time.After(time.Second):
		t.Fatal("Timeout")
	}
	select {
	case <-consumer.Messages():
	case <-time.After(time.Second):
		t.Fatal("Timeout")
	}

	if q.UnackedCount() != 2 {
		t.Errorf("UnackedCount = %d, want 2", q.UnackedCount())
	}
}

func TestQueue_Concurrent(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.MaxSize = 10000
	cfg.PrefetchCount = 100
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent publishers
	numPublishers := 10
	msgsPerPublisher := 100

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerPublisher; j++ {
				msg := message.New([]byte("msg"))
				q.Publish(ctx, msg)
			}
		}(i)
	}

	// Concurrent consumers
	consumer, _ := q.Subscribe("consumer-1")
	received := int32(0)
	done := make(chan struct{})

	go func() {
		for msg := range consumer.Messages() {
			msg.Ack()
			received++
			if received >= int32(numPublishers*msgsPerPublisher) {
				close(done)
				return
			}
		}
	}()

	wg.Wait()

	// Wait for all messages to be consumed
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout: received %d/%d messages", received, numPublishers*msgsPerPublisher)
	}

	stats := q.Stats()
	if stats.EnqueueCount != int64(numPublishers*msgsPerPublisher) {
		t.Errorf("EnqueueCount = %d, want %d", stats.EnqueueCount, numPublishers*msgsPerPublisher)
	}
}

func TestConsumer_Stats(t *testing.T) {
	cfg := DefaultConfig("test-queue")
	cfg.PrefetchCount = 10
	q := New(cfg)
	defer q.Close()

	consumer, _ := q.Subscribe("consumer-1")

	ctx := context.Background()
	q.Publish(ctx, message.New([]byte("msg")))

	select {
	case msg := <-consumer.Messages():
		msg.Ack()
	case <-time.After(time.Second):
		t.Fatal("Timeout")
	}

	delivered, acked, nacked := consumer.Stats()

	if delivered != 1 {
		t.Errorf("Delivered = %d, want 1", delivered)
	}
	if acked != 1 {
		t.Errorf("Acked = %d, want 1", acked)
	}
	if nacked != 0 {
		t.Errorf("Nacked = %d, want 0", nacked)
	}
}

func TestPriorityQueue_Ordering(t *testing.T) {
	pq := &priorityQueue{}

	// Add messages with different priorities
	msg1 := message.New([]byte("low"))
	msg1.SetPriority(1)

	msg2 := message.New([]byte("high"))
	msg2.SetPriority(10)

	msg3 := message.New([]byte("medium"))
	msg3.SetPriority(5)

	pq.Push(msg1)
	pq.Push(msg2)
	pq.Push(msg3)

	if pq.Len() != 3 {
		t.Errorf("Len() = %d, want 3", pq.Len())
	}

	// Pop should return highest priority first
	priorities := []int{10, 5, 1}
	for i, expected := range priorities {
		msg := pq.Pop().(*message.Message)
		if msg.Priority() != expected {
			t.Errorf("Pop %d: priority = %d, want %d", i, msg.Priority(), expected)
		}
	}
}

// Benchmarks

func BenchmarkQueue_Publish(b *testing.B) {
	cfg := DefaultConfig("bench-queue")
	cfg.MaxSize = b.N + 1000
	q := New(cfg)
	defer q.Close()

	ctx := context.Background()
	msg := message.New([]byte("benchmark payload"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Publish(ctx, msg.Clone())
	}
}

func BenchmarkQueue_PublishAndConsume(b *testing.B) {
	cfg := DefaultConfig("bench-queue")
	cfg.MaxSize = b.N + 1000
	cfg.PrefetchCount = 1000
	q := New(cfg)
	defer q.Close()

	consumer, _ := q.Subscribe("consumer-1")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := message.New([]byte("benchmark payload"))
		q.Publish(ctx, msg)

		select {
		case received := <-consumer.Messages():
			received.Ack()
		case <-time.After(time.Second):
			b.Fatal("Timeout")
		}
	}
}

func BenchmarkQueue_Subscribe(b *testing.B) {
	cfg := DefaultConfig("bench-queue")
	q := New(cfg)
	defer q.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumer, _ := q.Subscribe("consumer-" + string(rune(i)))
		consumer.Close()
	}
}
