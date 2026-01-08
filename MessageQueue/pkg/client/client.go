// Package client provides a client SDK for connecting to the GoMQ message queue server.
package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Common errors returned by client operations.
var (
	ErrNotConnected   = errors.New("client is not connected")
	ErrAlreadyClosed  = errors.New("client is already closed")
	ErrTimeout        = errors.New("operation timed out")
	ErrAuthFailed     = errors.New("authentication failed")
	ErrServerError    = errors.New("server error")
	ErrInvalidMessage = errors.New("invalid message format")
)

// Config holds client configuration.
type Config struct {
	// Address is the server address (e.g., "localhost:5672")
	Address string

	// ConnectTimeout is the timeout for establishing a connection
	ConnectTimeout time.Duration

	// ReadTimeout is the timeout for read operations
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for write operations
	WriteTimeout time.Duration

	// ReconnectInterval is the interval between reconnection attempts
	ReconnectInterval time.Duration

	// MaxReconnectAttempts is the maximum number of reconnection attempts (0 = unlimited)
	MaxReconnectAttempts int

	// AuthToken is the authentication token (if server requires auth)
	AuthToken string

	// BufferSize is the size of the message receive buffer
	BufferSize int
}

// DefaultConfig returns a default client configuration.
func DefaultConfig() Config {
	return Config{
		Address:              "localhost:5672",
		ConnectTimeout:       10 * time.Second,
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         10 * time.Second,
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
		BufferSize:           100,
	}
}

// Message represents a received message.
type Message struct {
	ID        string            `json:"id"`
	Source    string            `json:"source"` // "queue" or "topic"
	Name      string            `json:"name"`   // queue or topic name
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers"`
	Priority  int               `json:"priority"`
	Timestamp time.Time         `json:"timestamp"`

	// Client reference for acknowledgment
	client    *Client
	queueName string
}

// Ack acknowledges the message.
func (m *Message) Ack() error {
	if m.client == nil || m.Source != "queue" {
		return nil
	}
	return m.client.Ack(m.queueName, m.ID)
}

// Nack negatively acknowledges the message.
func (m *Message) Nack(requeue bool) error {
	if m.client == nil || m.Source != "queue" {
		return nil
	}
	return m.client.Nack(m.queueName, m.ID, requeue)
}

// PayloadString returns the payload as a string.
func (m *Message) PayloadString() string {
	return string(m.Payload)
}

// UnmarshalPayload unmarshals the payload into the provided value.
func (m *Message) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(m.Payload, v)
}

// PublishOptions holds options for publishing messages.
type PublishOptions struct {
	Headers  map[string]string
	Priority int
	TTL      time.Duration
	Delay    time.Duration
}

// Client is a client for connecting to the GoMQ server.
type Client struct {
	mu sync.RWMutex

	config Config
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	// Message receiving
	messages chan *Message
	stopCh   chan struct{}

	// State
	connected atomic.Bool
	closed    atomic.Bool

	// Subscriptions tracking
	subscriptions map[string]bool
	subLock       sync.RWMutex

	// Request/response handling
	responseCh chan string
	responseMu sync.Mutex

	// Reconnection
	reconnecting atomic.Bool

	wg sync.WaitGroup
}

// Connect creates a new client and connects to the server.
func Connect(address string) (*Client, error) {
	cfg := DefaultConfig()
	cfg.Address = address
	return ConnectWithConfig(cfg)
}

// ConnectWithConfig creates a new client with the given configuration.
func ConnectWithConfig(cfg Config) (*Client, error) {
	c := &Client{
		config:        cfg,
		messages:      make(chan *Message, cfg.BufferSize),
		stopCh:        make(chan struct{}),
		subscriptions: make(map[string]bool),
		responseCh:    make(chan string, 1),
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	// Start message receiver
	c.wg.Add(1)
	go c.receiveLoop()

	return c, nil
}

// connect establishes a connection to the server.
func (c *Client) connect() error {
	dialer := net.Dialer{
		Timeout: c.config.ConnectTimeout,
	}

	conn, err := dialer.Dial("tcp", c.config.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.config.Address, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.mu.Unlock()

	c.connected.Store(true)

	// Authenticate if token is provided
	if c.config.AuthToken != "" {
		if err := c.authenticate(); err != nil {
			c.conn.Close()
			c.connected.Store(false)
			return err
		}
	}

	return nil
}

// authenticate sends the authentication token to the server.
func (c *Client) authenticate() error {
	resp, err := c.sendCommand("AUTH " + c.config.AuthToken)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR") {
		return ErrAuthFailed
	}
	return nil
}

// receiveLoop continuously reads messages from the server.
func (c *Client) receiveLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if !c.connected.Load() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c.mu.RLock()
		reader := c.reader
		c.mu.RUnlock()

		if reader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Set read deadline
		if c.config.ReadTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if c.closed.Load() {
				return
			}
			// Connection lost, attempt reconnect
			c.handleDisconnect()
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse response
		if strings.HasPrefix(line, "MESSAGE ") {
			// This is a message delivery
			c.handleMessage(strings.TrimPrefix(line, "MESSAGE "))
		} else {
			// This is a response to a command
			select {
			case c.responseCh <- line:
			default:
				// Response channel full, discard
			}
		}
	}
}

// handleMessage parses and handles an incoming message.
func (c *Client) handleMessage(data string) {
	var msgData struct {
		ID        string            `json:"id"`
		Source    string            `json:"source"`
		Name      string            `json:"name"`
		Payload   string            `json:"payload"`
		Headers   map[string]string `json:"headers"`
		Priority  int               `json:"priority"`
		Timestamp string            `json:"timestamp"`
	}

	if err := json.Unmarshal([]byte(data), &msgData); err != nil {
		return
	}

	timestamp, _ := time.Parse(time.RFC3339, msgData.Timestamp)

	msg := &Message{
		ID:        msgData.ID,
		Source:    msgData.Source,
		Name:      msgData.Name,
		Payload:   []byte(msgData.Payload),
		Headers:   msgData.Headers,
		Priority:  msgData.Priority,
		Timestamp: timestamp,
		client:    c,
		queueName: msgData.Name,
	}

	select {
	case c.messages <- msg:
	default:
		// Buffer full, message dropped
	}
}

// handleDisconnect handles a connection loss.
func (c *Client) handleDisconnect() {
	if c.closed.Load() || c.reconnecting.Load() {
		return
	}

	c.connected.Store(false)
	c.reconnecting.Store(true)
	defer c.reconnecting.Store(false)

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock()

	// Attempt reconnection
	attempts := 0
	for {
		if c.closed.Load() {
			return
		}

		if c.config.MaxReconnectAttempts > 0 && attempts >= c.config.MaxReconnectAttempts {
			return
		}

		attempts++
		time.Sleep(c.config.ReconnectInterval)

		if err := c.connect(); err != nil {
			continue
		}

		// Resubscribe to all subscriptions
		c.resubscribe()
		return
	}
}

// resubscribe resubscribes to all previous subscriptions after reconnect.
func (c *Client) resubscribe() {
	c.subLock.RLock()
	subs := make(map[string]bool)
	for k, v := range c.subscriptions {
		subs[k] = v
	}
	c.subLock.RUnlock()

	for sub := range subs {
		parts := strings.SplitN(sub, ":", 2)
		if len(parts) == 2 {
			c.sendCommand(fmt.Sprintf("SUBSCRIBE %s %s", parts[0], parts[1]))
		}
	}
}

// sendCommand sends a command and waits for a response.
func (c *Client) sendCommand(cmd string) (string, error) {
	if !c.connected.Load() {
		return "", ErrNotConnected
	}

	c.responseMu.Lock()
	defer c.responseMu.Unlock()

	// Drain any old responses
	select {
	case <-c.responseCh:
	default:
	}

	c.mu.Lock()
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}
	_, err := c.writer.WriteString(cmd + "\n")
	if err != nil {
		c.mu.Unlock()
		return "", err
	}
	err = c.writer.Flush()
	c.mu.Unlock()

	if err != nil {
		return "", err
	}

	// Wait for response with timeout
	select {
	case resp := <-c.responseCh:
		return resp, nil
	case <-time.After(c.config.ReadTimeout):
		return "", ErrTimeout
	case <-c.stopCh:
		return "", ErrAlreadyClosed
	}
}

// parseResponse parses a server response.
func parseResponse(resp string) (string, string, error) {
	parts := strings.SplitN(resp, " ", 2)
	code := parts[0]
	data := ""
	if len(parts) > 1 {
		data = parts[1]
	}

	if code == "ERROR" {
		return "", data, fmt.Errorf("%w: %s", ErrServerError, data)
	}

	return code, data, nil
}

// Messages returns the channel for receiving messages.
func (c *Client) Messages() <-chan *Message {
	return c.messages
}

// Ping sends a ping to the server.
func (c *Client) Ping() error {
	resp, err := c.sendCommand("PING")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "PONG") {
		return errors.New("unexpected response to ping")
	}
	return nil
}

// CreateQueue creates a new queue.
func (c *Client) CreateQueue(name string) error {
	resp, err := c.sendCommand("CREATE_QUEUE " + name)
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// DeleteQueue deletes a queue.
func (c *Client) DeleteQueue(name string) error {
	resp, err := c.sendCommand("DELETE_QUEUE " + name)
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// CreateTopic creates a new topic.
func (c *Client) CreateTopic(name string) error {
	resp, err := c.sendCommand("CREATE_TOPIC " + name)
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(name string) error {
	resp, err := c.sendCommand("DELETE_TOPIC " + name)
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// Publish publishes a message to a queue.
func (c *Client) Publish(ctx context.Context, queueName string, payload []byte) error {
	return c.PublishWithOptions(ctx, queueName, payload, PublishOptions{})
}

// PublishWithOptions publishes a message with options to a queue.
func (c *Client) PublishWithOptions(ctx context.Context, queueName string, payload []byte, opts PublishOptions) error {
	return c.publish(ctx, "queue", queueName, payload, opts)
}

// PublishToTopic publishes a message to a topic.
func (c *Client) PublishToTopic(ctx context.Context, topicName string, payload []byte) error {
	return c.PublishToTopicWithOptions(ctx, topicName, payload, PublishOptions{})
}

// PublishToTopicWithOptions publishes a message with options to a topic.
func (c *Client) PublishToTopicWithOptions(ctx context.Context, topicName string, payload []byte, opts PublishOptions) error {
	return c.publish(ctx, "topic", topicName, payload, opts)
}

// publish is the internal publish implementation.
func (c *Client) publish(ctx context.Context, targetType, targetName string, payload []byte, opts PublishOptions) error {
	msgData := map[string]interface{}{
		"payload": string(payload),
	}
	if opts.Headers != nil {
		msgData["headers"] = opts.Headers
	}
	if opts.Priority > 0 {
		msgData["priority"] = opts.Priority
	}
	if opts.TTL > 0 {
		msgData["ttl"] = opts.TTL.String()
	}
	if opts.Delay > 0 {
		msgData["delay"] = opts.Delay.String()
	}

	jsonData, err := json.Marshal(msgData)
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("PUBLISH %s %s %s", targetType, targetName, string(jsonData))
	resp, err := c.sendCommand(cmd)
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// Subscribe subscribes to a queue.
func (c *Client) Subscribe(ctx context.Context, queueName string) error {
	resp, err := c.sendCommand(fmt.Sprintf("SUBSCRIBE queue %s", queueName))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	if err == nil {
		c.subLock.Lock()
		c.subscriptions["queue:"+queueName] = true
		c.subLock.Unlock()
	}
	return err
}

// Unsubscribe unsubscribes from a queue.
func (c *Client) Unsubscribe(ctx context.Context, queueName string) error {
	resp, err := c.sendCommand(fmt.Sprintf("UNSUBSCRIBE queue %s", queueName))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	if err == nil {
		c.subLock.Lock()
		delete(c.subscriptions, "queue:"+queueName)
		c.subLock.Unlock()
	}
	return err
}

// SubscribeToTopic subscribes to a topic.
func (c *Client) SubscribeToTopic(ctx context.Context, topicName string) error {
	resp, err := c.sendCommand(fmt.Sprintf("SUBSCRIBE topic %s", topicName))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	if err == nil {
		c.subLock.Lock()
		c.subscriptions["topic:"+topicName] = true
		c.subLock.Unlock()
	}
	return err
}

// UnsubscribeFromTopic unsubscribes from a topic.
func (c *Client) UnsubscribeFromTopic(ctx context.Context, topicName string) error {
	resp, err := c.sendCommand(fmt.Sprintf("UNSUBSCRIBE topic %s", topicName))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	if err == nil {
		c.subLock.Lock()
		delete(c.subscriptions, "topic:"+topicName)
		c.subLock.Unlock()
	}
	return err
}

// Ack acknowledges a message.
func (c *Client) Ack(queueName, msgID string) error {
	resp, err := c.sendCommand(fmt.Sprintf("ACK %s %s", queueName, msgID))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// Nack negatively acknowledges a message.
func (c *Client) Nack(queueName, msgID string, requeue bool) error {
	requeueStr := "true"
	if !requeue {
		requeueStr = "false"
	}
	resp, err := c.sendCommand(fmt.Sprintf("NACK %s %s %s", queueName, msgID, requeueStr))
	if err != nil {
		return err
	}
	_, _, err = parseResponse(resp)
	return err
}

// ListQueues returns a list of all queues.
func (c *Client) ListQueues() ([]string, error) {
	resp, err := c.sendCommand("LIST_QUEUES")
	if err != nil {
		return nil, err
	}
	_, data, err := parseResponse(resp)
	if err != nil {
		return nil, err
	}

	var queues []string
	if err := json.Unmarshal([]byte(data), &queues); err != nil {
		return nil, err
	}
	return queues, nil
}

// ListTopics returns a list of all topics.
func (c *Client) ListTopics() ([]string, error) {
	resp, err := c.sendCommand("LIST_TOPICS")
	if err != nil {
		return nil, err
	}
	_, data, err := parseResponse(resp)
	if err != nil {
		return nil, err
	}

	var topics []string
	if err := json.Unmarshal([]byte(data), &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

// Stats returns server statistics.
func (c *Client) Stats() (map[string]interface{}, error) {
	resp, err := c.sendCommand("STATS")
	if err != nil {
		return nil, err
	}
	_, data, err := parseResponse(resp)
	if err != nil {
		return nil, err
	}

	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(data), &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

// IsConnected returns whether the client is connected.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return ErrAlreadyClosed
	}

	close(c.stopCh)

	c.mu.Lock()
	if c.conn != nil {
		// Send quit command
		c.writer.WriteString("QUIT\n")
		c.writer.Flush()
		c.conn.Close()
	}
	c.mu.Unlock()

	c.wg.Wait()
	close(c.messages)

	return nil
}
