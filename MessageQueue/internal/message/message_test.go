// Package message provides tests for the message types and functions.
package message

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	payload := []byte("test payload")
	msg := New(payload)

	if msg == nil {
		t.Fatal("New() returned nil")
	}

	if msg.ID() == "" {
		t.Error("Message ID should not be empty")
	}

	if string(msg.Payload()) != "test payload" {
		t.Errorf("Payload = %q, want %q", string(msg.Payload()), "test payload")
	}

	if msg.State() != StatePending {
		t.Errorf("State = %v, want %v", msg.State(), StatePending)
	}

	if msg.Priority() != PriorityNormal {
		t.Errorf("Priority = %d, want %d", msg.Priority(), PriorityNormal)
	}

	if msg.Timestamp().IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestNewWithID(t *testing.T) {
	customID := "custom-id-123"
	msg := NewWithID(customID, []byte("payload"))

	if msg.ID() != customID {
		t.Errorf("ID = %q, want %q", msg.ID(), customID)
	}
}

func TestMessage_Headers(t *testing.T) {
	msg := New([]byte("test"))

	// Set headers
	msg.SetHeader("content-type", "application/json")
	msg.SetHeader("correlation-id", "corr-123")

	// Get single header
	if got := msg.Header("content-type"); got != "application/json" {
		t.Errorf("Header(content-type) = %q, want %q", got, "application/json")
	}

	// Get non-existent header
	if got := msg.Header("non-existent"); got != "" {
		t.Errorf("Header(non-existent) = %q, want empty string", got)
	}

	// Get all headers
	headers := msg.Headers()
	if len(headers) != 2 {
		t.Errorf("Headers() returned %d headers, want 2", len(headers))
	}

	// Verify headers copy doesn't affect original
	headers["new-key"] = "new-value"
	if msg.Header("new-key") != "" {
		t.Error("Modifying returned headers should not affect message")
	}
}

func TestMessage_Priority(t *testing.T) {
	tests := []struct {
		name     string
		priority int
		want     int
	}{
		{"normal priority", PriorityNormal, PriorityNormal},
		{"high priority", PriorityHigh, PriorityHigh},
		{"low priority", PriorityLow, PriorityLow},
		{"negative clamped to 0", -5, 0},
		{"over 10 clamped to 10", 15, 10},
		{"exact boundary 0", 0, 0},
		{"exact boundary 10", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := New([]byte("test"))
			msg.SetPriority(tt.priority)

			if got := msg.Priority(); got != tt.want {
				t.Errorf("Priority() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMessage_TTL(t *testing.T) {
	t.Run("no TTL set", func(t *testing.T) {
		msg := New([]byte("test"))

		if msg.TTL() != 0 {
			t.Errorf("TTL() = %v, want 0", msg.TTL())
		}

		if msg.IsExpired() {
			t.Error("Message without TTL should not be expired")
		}
	})

	t.Run("with TTL not expired", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetTTL(time.Hour)

		if msg.TTL() != time.Hour {
			t.Errorf("TTL() = %v, want %v", msg.TTL(), time.Hour)
		}

		if msg.ExpiresAt().IsZero() {
			t.Error("ExpiresAt should be set")
		}

		if msg.IsExpired() {
			t.Error("Message should not be expired yet")
		}
	})

	t.Run("with TTL expired", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetTTL(time.Millisecond)

		time.Sleep(5 * time.Millisecond)

		if !msg.IsExpired() {
			t.Error("Message should be expired")
		}
	})
}

func TestMessage_Delay(t *testing.T) {
	t.Run("no delay", func(t *testing.T) {
		msg := New([]byte("test"))

		if !msg.IsReady() {
			t.Error("Message without delay should be ready")
		}
	})

	t.Run("with delay not ready", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetDelay(time.Hour)

		if msg.IsReady() {
			t.Error("Message with future delay should not be ready")
		}

		if msg.Delay() != time.Hour {
			t.Errorf("Delay() = %v, want %v", msg.Delay(), time.Hour)
		}
	})

	t.Run("with delay ready", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetDelay(time.Millisecond)

		time.Sleep(5 * time.Millisecond)

		if !msg.IsReady() {
			t.Error("Message should be ready after delay")
		}
	})
}

func TestMessage_DeduplicationID(t *testing.T) {
	msg := New([]byte("test"))

	if msg.DeduplicationID() != "" {
		t.Error("DeduplicationID should be empty by default")
	}

	msg.SetDeduplicationID("dedup-123")

	if got := msg.DeduplicationID(); got != "dedup-123" {
		t.Errorf("DeduplicationID() = %q, want %q", got, "dedup-123")
	}
}

func TestMessage_State(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StatePending, "pending"},
		{StateDelivered, "delivered"},
		{StateAcknowledged, "acknowledged"},
		{StateRejected, "rejected"},
		{StateExpired, "expired"},
		{StateDeadLettered, "dead_lettered"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("State.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMessage_DeliveryCount(t *testing.T) {
	msg := New([]byte("test"))

	if msg.DeliveryCount() != 0 {
		t.Errorf("DeliveryCount() = %d, want 0", msg.DeliveryCount())
	}

	count := msg.IncrementDeliveryCount()
	if count != 1 {
		t.Errorf("IncrementDeliveryCount() = %d, want 1", count)
	}

	if msg.DeliveryCount() != 1 {
		t.Errorf("DeliveryCount() = %d, want 1", msg.DeliveryCount())
	}
}

func TestMessage_MarkDelivered(t *testing.T) {
	msg := New([]byte("test"))
	initialCount := msg.DeliveryCount()

	msg.MarkDelivered()

	if msg.State() != StateDelivered {
		t.Errorf("State = %v, want %v", msg.State(), StateDelivered)
	}

	if msg.DeliveryCount() != initialCount+1 {
		t.Errorf("DeliveryCount = %d, want %d", msg.DeliveryCount(), initialCount+1)
	}

	if msg.LastDelivered().IsZero() {
		t.Error("LastDelivered should be set")
	}
}

func TestMessage_Ack(t *testing.T) {
	t.Run("successful ack", func(t *testing.T) {
		msg := New([]byte("test"))
		ackCalled := false

		msg.SetAckFunc(func() error {
			ackCalled = true
			return nil
		})

		err := msg.Ack()
		if err != nil {
			t.Errorf("Ack() error = %v", err)
		}

		if !ackCalled {
			t.Error("Ack callback should have been called")
		}

		if msg.State() != StateAcknowledged {
			t.Errorf("State = %v, want %v", msg.State(), StateAcknowledged)
		}
	})

	t.Run("double ack", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.Ack()

		err := msg.Ack()
		if err != ErrAlreadyAcknowledged {
			t.Errorf("Second Ack() error = %v, want %v", err, ErrAlreadyAcknowledged)
		}
	})

	t.Run("ack expired message", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetTTL(time.Millisecond)
		time.Sleep(5 * time.Millisecond)

		err := msg.Ack()
		if err != ErrMessageExpired {
			t.Errorf("Ack() error = %v, want %v", err, ErrMessageExpired)
		}
	})
}

func TestMessage_Nack(t *testing.T) {
	t.Run("nack with requeue", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetState(StateDelivered)

		nackCalled := false
		msg.SetNackFunc(func(requeue bool) error {
			nackCalled = true
			if !requeue {
				t.Error("requeue should be true")
			}
			return nil
		})

		err := msg.Nack(true)
		if err != nil {
			t.Errorf("Nack() error = %v", err)
		}

		if !nackCalled {
			t.Error("Nack callback should have been called")
		}

		if msg.State() != StatePending {
			t.Errorf("State = %v, want %v", msg.State(), StatePending)
		}
	})

	t.Run("nack without requeue", func(t *testing.T) {
		msg := New([]byte("test"))
		msg.SetState(StateDelivered)

		err := msg.Nack(false)
		if err != nil {
			t.Errorf("Nack() error = %v", err)
		}

		if msg.State() != StateRejected {
			t.Errorf("State = %v, want %v", msg.State(), StateRejected)
		}
	})
}

func TestMessage_Reject(t *testing.T) {
	msg := New([]byte("test"))
	msg.SetState(StateDelivered)

	err := msg.Reject()
	if err != nil {
		t.Errorf("Reject() error = %v", err)
	}

	if msg.State() != StateRejected {
		t.Errorf("State = %v, want %v", msg.State(), StateRejected)
	}
}

func TestMessage_Requeue(t *testing.T) {
	msg := New([]byte("test"))
	msg.SetState(StateDelivered)

	err := msg.Requeue()
	if err != nil {
		t.Errorf("Requeue() error = %v", err)
	}

	if msg.State() != StatePending {
		t.Errorf("State = %v, want %v", msg.State(), StatePending)
	}
}

func TestMessage_QueueAndTopicName(t *testing.T) {
	msg := New([]byte("test"))

	msg.SetQueueName("my-queue")
	if got := msg.QueueName(); got != "my-queue" {
		t.Errorf("QueueName() = %q, want %q", got, "my-queue")
	}

	msg.SetTopicName("my-topic")
	if got := msg.TopicName(); got != "my-topic" {
		t.Errorf("TopicName() = %q, want %q", got, "my-topic")
	}
}

func TestMessage_Clone(t *testing.T) {
	original := New([]byte("original payload"))
	original.SetHeader("key", "value")
	original.SetPriority(PriorityHigh)
	original.SetTTL(time.Hour)
	original.SetDeduplicationID("dedup-123")

	clone := original.Clone()

	// Verify clone has different ID
	if clone.ID() == original.ID() {
		t.Error("Clone should have different ID")
	}

	// Verify payload is copied
	if string(clone.Payload()) != "original payload" {
		t.Error("Clone should have same payload")
	}

	// Verify modifying clone doesn't affect original
	clone.SetHeader("new-key", "new-value")
	if original.Header("new-key") != "" {
		t.Error("Modifying clone should not affect original")
	}

	// Verify properties are copied
	if clone.Priority() != original.Priority() {
		t.Error("Clone should have same priority")
	}

	if clone.TTL() != original.TTL() {
		t.Error("Clone should have same TTL")
	}

	if clone.DeduplicationID() != original.DeduplicationID() {
		t.Error("Clone should have same deduplication ID")
	}

	// Clone should be in pending state
	if clone.State() != StatePending {
		t.Errorf("Clone state = %v, want %v", clone.State(), StatePending)
	}
}

func TestMessage_JSON(t *testing.T) {
	original := New([]byte("test payload"))
	original.SetHeader("content-type", "application/json")
	original.SetPriority(PriorityHigh)
	original.SetTTL(time.Hour)
	original.SetQueueName("test-queue")

	// Marshal
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	// Unmarshal
	var restored Message
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	// Verify
	if restored.ID() != original.ID() {
		t.Errorf("ID = %q, want %q", restored.ID(), original.ID())
	}

	if string(restored.Payload()) != string(original.Payload()) {
		t.Errorf("Payload = %q, want %q", string(restored.Payload()), string(original.Payload()))
	}

	if restored.Header("content-type") != "application/json" {
		t.Errorf("Header = %q, want %q", restored.Header("content-type"), "application/json")
	}

	if restored.Priority() != PriorityHigh {
		t.Errorf("Priority = %d, want %d", restored.Priority(), PriorityHigh)
	}

	if restored.QueueName() != "test-queue" {
		t.Errorf("QueueName = %q, want %q", restored.QueueName(), "test-queue")
	}
}

func TestMessage_PayloadString(t *testing.T) {
	msg := New([]byte("hello world"))

	if got := msg.PayloadString(); got != "hello world" {
		t.Errorf("PayloadString() = %q, want %q", got, "hello world")
	}
}

func TestBuilder(t *testing.T) {
	msg := NewBuilder().
		WithID("custom-id").
		WithStringPayload("test payload").
		WithHeader("content-type", "text/plain").
		WithHeader("custom-header", "custom-value").
		WithPriority(PriorityHigh).
		WithTTL(time.Hour).
		WithDelay(time.Minute).
		WithDeduplicationID("dedup-id").
		Build()

	if msg.ID() != "custom-id" {
		t.Errorf("ID = %q, want %q", msg.ID(), "custom-id")
	}

	if msg.PayloadString() != "test payload" {
		t.Errorf("Payload = %q, want %q", msg.PayloadString(), "test payload")
	}

	if msg.Header("content-type") != "text/plain" {
		t.Errorf("Header(content-type) = %q, want %q", msg.Header("content-type"), "text/plain")
	}

	if msg.Priority() != PriorityHigh {
		t.Errorf("Priority = %d, want %d", msg.Priority(), PriorityHigh)
	}

	if msg.TTL() != time.Hour {
		t.Errorf("TTL = %v, want %v", msg.TTL(), time.Hour)
	}

	if msg.Delay() != time.Minute {
		t.Errorf("Delay = %v, want %v", msg.Delay(), time.Minute)
	}

	if msg.DeduplicationID() != "dedup-id" {
		t.Errorf("DeduplicationID = %q, want %q", msg.DeduplicationID(), "dedup-id")
	}
}

func TestBuilder_WithJSONPayload(t *testing.T) {
	type testData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	data := testData{Name: "test", Value: 42}
	msg := NewBuilder().WithJSONPayload(data).Build()

	if msg.Header("content-type") != "application/json" {
		t.Errorf("content-type = %q, want %q", msg.Header("content-type"), "application/json")
	}

	var restored testData
	if err := json.Unmarshal(msg.Payload(), &restored); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}

	if restored.Name != data.Name || restored.Value != data.Value {
		t.Errorf("Restored data = %+v, want %+v", restored, data)
	}
}

func TestMessage_Concurrency(t *testing.T) {
	msg := New([]byte("test"))
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg.SetHeader("key", "value")
			msg.SetPriority(i % 10)
			msg.IncrementDeliveryCount()
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = msg.ID()
			_ = msg.Header("key")
			_ = msg.Priority()
			_ = msg.DeliveryCount()
			_ = msg.State()
			_ = msg.Headers()
		}()
	}

	wg.Wait()

	// Just ensure no race conditions occurred (test passes if no panic)
	if msg.DeliveryCount() != 100 {
		t.Errorf("DeliveryCount = %d, want 100", msg.DeliveryCount())
	}
}

func TestMessage_FluentAPI(t *testing.T) {
	// Test that setters return the message for chaining
	msg := New([]byte("test")).
		SetHeader("key", "value").
		SetPriority(PriorityHigh).
		SetTTL(time.Hour).
		SetDelay(time.Minute).
		SetDeduplicationID("dedup")

	if msg == nil {
		t.Fatal("Fluent API returned nil")
	}

	if msg.Header("key") != "value" {
		t.Error("Header not set correctly via fluent API")
	}

	if msg.Priority() != PriorityHigh {
		t.Error("Priority not set correctly via fluent API")
	}
}

// Benchmarks

func BenchmarkNew(b *testing.B) {
	payload := []byte("benchmark payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = New(payload)
	}
}

func BenchmarkMessage_SetHeader(b *testing.B) {
	msg := New([]byte("test"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.SetHeader("key", "value")
	}
}

func BenchmarkMessage_Clone(b *testing.B) {
	msg := New([]byte("test payload for cloning"))
	msg.SetHeader("key1", "value1")
	msg.SetHeader("key2", "value2")
	msg.SetPriority(PriorityHigh)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = msg.Clone()
	}
}

func BenchmarkMessage_JSON(b *testing.B) {
	msg := New([]byte("test payload"))
	msg.SetHeader("content-type", "application/json")
	msg.SetPriority(PriorityHigh)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(msg)
		var restored Message
		json.Unmarshal(data, &restored)
	}
}

func BenchmarkBuilder(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewBuilder().
			WithStringPayload("test payload").
			WithHeader("key", "value").
			WithPriority(PriorityHigh).
			WithTTL(time.Hour).
			Build()
	}
}
