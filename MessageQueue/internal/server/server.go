// Package server provides a TCP server for remote access to the message queue broker.
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messagequeue/internal/message"
)

// Common errors returned by server operations.
var (
	ErrServerClosed     = errors.New("server is closed")
	ErrInvalidCommand   = errors.New("invalid command")
	ErrMissingParameter = errors.New("missing required parameter")
	ErrSessionClosed    = errors.New("session is closed")
	ErrUnauthorized     = errors.New("unauthorized")
)

// Command types for the protocol.
const (
	CmdPublish     = "PUBLISH"
	CmdSubscribe   = "SUBSCRIBE"
	CmdUnsubscribe = "UNSUBSCRIBE"
	CmdAck         = "ACK"
	CmdNack        = "NACK"
	CmdCreateQueue = "CREATE_QUEUE"
	CmdDeleteQueue = "DELETE_QUEUE"
	CmdCreateTopic = "CREATE_TOPIC"
	CmdDeleteTopic = "DELETE_TOPIC"
	CmdListQueues  = "LIST_QUEUES"
	CmdListTopics  = "LIST_TOPICS"
	CmdStats       = "STATS"
	CmdPing        = "PING"
	CmdQuit        = "QUIT"
)

// Response codes.
const (
	RespOK    = "OK"
	RespError = "ERROR"
	RespMsg   = "MESSAGE"
	RespPong  = "PONG"
)

// Broker defines the interface for the message broker.
type Broker interface {
	CreateQueue(name string) error
	DeleteQueue(name string) error
	PublishToQueue(ctx context.Context, queueName string, msg *message.Message) error
	SubscribeToQueue(queueName, consumerID string) (<-chan *message.Message, error)
	UnsubscribeFromQueue(queueName, consumerID string) error
	AckMessage(queueName, msgID string) error
	NackMessage(queueName, msgID string, requeue bool) error

	CreateTopic(name string) error
	DeleteTopic(name string) error
	PublishToTopic(ctx context.Context, topicName string, msg *message.Message) error
	SubscribeToTopic(topicName, subscriberID string) (<-chan *message.Message, error)
	UnsubscribeFromTopic(topicName, subscriberID string) error

	ListQueues() []string
	ListTopics() []string
	Stats() map[string]interface{}
}

// Config holds server configuration.
type Config struct {
	// Address is the TCP address to listen on (e.g., ":5672")
	Address string

	// ReadTimeout is the timeout for reading from clients
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for writing to clients
	WriteTimeout time.Duration

	// MaxConnections is the maximum number of concurrent connections (0 = unlimited)
	MaxConnections int

	// MaxMessageSize is the maximum message size in bytes
	MaxMessageSize int

	// EnableAuth enables authentication
	EnableAuth bool

	// AuthToken is the token required for authentication (if enabled)
	AuthToken string
}

// DefaultConfig returns a default server configuration.
func DefaultConfig() Config {
	return Config{
		Address:        ":5672",
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxConnections: 1000,
		MaxMessageSize: 1024 * 1024, // 1MB
		EnableAuth:     false,
	}
}

// Server is a TCP server for the message queue.
type Server struct {
	mu sync.RWMutex

	config   Config
	broker   Broker
	listener net.Listener

	// Session management
	sessions    map[string]*Session
	sessionLock sync.RWMutex

	// Stats
	totalConnections int64
	activeConns      int64

	// Lifecycle
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// New creates a new Server.
func New(broker Broker, cfg Config) *Server {
	return &Server{
		config:   cfg,
		broker:   broker,
		sessions: make(map[string]*Session),
		closeCh:  make(chan struct{}),
	}
}

// ListenAndServe starts the server and listens for connections.
func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	return s.serve()
}

// Serve accepts connections on the provided listener.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	return s.serve()
}

// serve is the main accept loop.
func (s *Server) serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return ErrServerClosed
			}
			// Check for temporary errors
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return fmt.Errorf("accept error: %w", err)
		}

		// Check connection limit
		if s.config.MaxConnections > 0 && int(atomic.LoadInt64(&s.activeConns)) >= s.config.MaxConnections {
			conn.Close()
			continue
		}

		atomic.AddInt64(&s.totalConnections, 1)
		atomic.AddInt64(&s.activeConns, 1)

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.activeConns, -1)
		s.wg.Done()
	}()

	// Create session
	session := newSession(conn, s)
	s.addSession(session)
	defer s.removeSession(session.id)

	// Handle commands
	session.serve()
}

// addSession adds a session to the server.
func (s *Server) addSession(session *Session) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()
	s.sessions[session.id] = session
}

// removeSession removes a session from the server.
func (s *Server) removeSession(sessionID string) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()
	delete(s.sessions, sessionID)
}

// getSession returns a session by ID.
func (s *Server) getSession(sessionID string) (*Session, bool) {
	s.sessionLock.RLock()
	defer s.sessionLock.RUnlock()
	session, exists := s.sessions[sessionID]
	return session, exists
}

// ActiveConnections returns the number of active connections.
func (s *Server) ActiveConnections() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

// TotalConnections returns the total number of connections received.
func (s *Server) TotalConnections() int64 {
	return atomic.LoadInt64(&s.totalConnections)
}

// Address returns the server's listen address.
func (s *Server) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.Address
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return ErrServerClosed
	}

	close(s.closeCh)

	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()

	// Close all sessions
	s.sessionLock.Lock()
	for _, session := range s.sessions {
		session.close()
	}
	s.sessionLock.Unlock()

	// Wait for all handlers to complete
	s.wg.Wait()

	return nil
}

// IsClosed returns whether the server is closed.
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// Session represents a client session.
type Session struct {
	mu sync.RWMutex

	id            string
	conn          net.Conn
	server        *Server
	reader        *bufio.Reader
	writer        *bufio.Writer
	authenticated bool

	// Subscriptions
	queueSubs map[string]<-chan *message.Message
	topicSubs map[string]<-chan *message.Message

	// Message forwarding
	stopCh chan struct{}
	closed atomic.Bool
}

// newSession creates a new session.
func newSession(conn net.Conn, server *Server) *Session {
	return &Session{
		id:        generateSessionID(),
		conn:      conn,
		server:    server,
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		queueSubs: make(map[string]<-chan *message.Message),
		topicSubs: make(map[string]<-chan *message.Message),
		stopCh:    make(chan struct{}),
	}
}

// serve handles incoming commands for the session.
func (s *Session) serve() {
	defer s.cleanup()

	// Start message forwarder
	go s.forwardMessages()

	for {
		// Set read deadline
		if s.server.config.ReadTimeout > 0 {
			s.conn.SetReadDeadline(time.Now().Add(s.server.config.ReadTimeout))
		}

		// Read command line
		line, err := s.reader.ReadString('\n')
		if err != nil {
			if err != io.EOF && !s.closed.Load() {
				s.sendError("read error: " + err.Error())
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse and handle command
		if err := s.handleCommand(line); err != nil {
			if err == ErrSessionClosed {
				return
			}
			s.sendError(err.Error())
		}
	}
}

// handleCommand parses and executes a command.
func (s *Session) handleCommand(line string) error {
	parts := strings.SplitN(line, " ", 2)
	cmd := strings.ToUpper(parts[0])
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}

	// Check authentication for protected commands
	if s.server.config.EnableAuth && !s.authenticated && cmd != "AUTH" && cmd != CmdPing && cmd != CmdQuit {
		return ErrUnauthorized
	}

	switch cmd {
	case "AUTH":
		return s.handleAuth(args)
	case CmdPing:
		return s.handlePing()
	case CmdQuit:
		return s.handleQuit()
	case CmdPublish:
		return s.handlePublish(args)
	case CmdSubscribe:
		return s.handleSubscribe(args)
	case CmdUnsubscribe:
		return s.handleUnsubscribe(args)
	case CmdAck:
		return s.handleAck(args)
	case CmdNack:
		return s.handleNack(args)
	case CmdCreateQueue:
		return s.handleCreateQueue(args)
	case CmdDeleteQueue:
		return s.handleDeleteQueue(args)
	case CmdCreateTopic:
		return s.handleCreateTopic(args)
	case CmdDeleteTopic:
		return s.handleDeleteTopic(args)
	case CmdListQueues:
		return s.handleListQueues()
	case CmdListTopics:
		return s.handleListTopics()
	case CmdStats:
		return s.handleStats()
	default:
		return ErrInvalidCommand
	}
}

// handleAuth handles the AUTH command.
func (s *Session) handleAuth(args string) error {
	if !s.server.config.EnableAuth {
		s.authenticated = true
		return s.sendOK("authentication not required")
	}

	if args == s.server.config.AuthToken {
		s.authenticated = true
		return s.sendOK("authenticated")
	}

	return ErrUnauthorized
}

// handlePing handles the PING command.
func (s *Session) handlePing() error {
	return s.send(RespPong, "")
}

// handleQuit handles the QUIT command.
func (s *Session) handleQuit() error {
	s.sendOK("goodbye")
	return ErrSessionClosed
}

// handlePublish handles the PUBLISH command.
// Format: PUBLISH queue|topic <name> <json_payload>
func (s *Session) handlePublish(args string) error {
	parts := strings.SplitN(args, " ", 3)
	if len(parts) < 3 {
		return ErrMissingParameter
	}

	targetType := strings.ToLower(parts[0])
	targetName := parts[1]
	payload := parts[2]

	// Parse message from JSON
	var msgData struct {
		Payload  string            `json:"payload"`
		Headers  map[string]string `json:"headers,omitempty"`
		Priority int               `json:"priority,omitempty"`
		TTL      string            `json:"ttl,omitempty"`
		Delay    string            `json:"delay,omitempty"`
	}

	if err := json.Unmarshal([]byte(payload), &msgData); err != nil {
		// Treat as plain text payload
		msgData.Payload = payload
	}

	msg := message.New([]byte(msgData.Payload))
	for k, v := range msgData.Headers {
		msg.SetHeader(k, v)
	}
	if msgData.Priority > 0 {
		msg.SetPriority(msgData.Priority)
	}
	if msgData.TTL != "" {
		if ttl, err := time.ParseDuration(msgData.TTL); err == nil {
			msg.SetTTL(ttl)
		}
	}
	if msgData.Delay != "" {
		if delay, err := time.ParseDuration(msgData.Delay); err == nil {
			msg.SetDelay(delay)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch targetType {
	case "queue":
		if err := s.server.broker.PublishToQueue(ctx, targetName, msg); err != nil {
			return err
		}
	case "topic":
		if err := s.server.broker.PublishToTopic(ctx, targetName, msg); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid target type: %s (use 'queue' or 'topic')", targetType)
	}

	return s.sendOK(msg.ID())
}

// handleSubscribe handles the SUBSCRIBE command.
// Format: SUBSCRIBE queue|topic <name>
func (s *Session) handleSubscribe(args string) error {
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		return ErrMissingParameter
	}

	targetType := strings.ToLower(parts[0])
	targetName := parts[1]

	s.mu.Lock()
	defer s.mu.Unlock()

	switch targetType {
	case "queue":
		if _, exists := s.queueSubs[targetName]; exists {
			return errors.New("already subscribed to queue")
		}
		ch, err := s.server.broker.SubscribeToQueue(targetName, s.id)
		if err != nil {
			return err
		}
		s.queueSubs[targetName] = ch

	case "topic":
		if _, exists := s.topicSubs[targetName]; exists {
			return errors.New("already subscribed to topic")
		}
		ch, err := s.server.broker.SubscribeToTopic(targetName, s.id)
		if err != nil {
			return err
		}
		s.topicSubs[targetName] = ch

	default:
		return fmt.Errorf("invalid target type: %s", targetType)
	}

	return s.sendOK("subscribed")
}

// handleUnsubscribe handles the UNSUBSCRIBE command.
// Format: UNSUBSCRIBE queue|topic <name>
func (s *Session) handleUnsubscribe(args string) error {
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		return ErrMissingParameter
	}

	targetType := strings.ToLower(parts[0])
	targetName := parts[1]

	s.mu.Lock()
	defer s.mu.Unlock()

	switch targetType {
	case "queue":
		if _, exists := s.queueSubs[targetName]; !exists {
			return errors.New("not subscribed to queue")
		}
		if err := s.server.broker.UnsubscribeFromQueue(targetName, s.id); err != nil {
			return err
		}
		delete(s.queueSubs, targetName)

	case "topic":
		if _, exists := s.topicSubs[targetName]; !exists {
			return errors.New("not subscribed to topic")
		}
		if err := s.server.broker.UnsubscribeFromTopic(targetName, s.id); err != nil {
			return err
		}
		delete(s.topicSubs, targetName)

	default:
		return fmt.Errorf("invalid target type: %s", targetType)
	}

	return s.sendOK("unsubscribed")
}

// handleAck handles the ACK command.
// Format: ACK <queue_name> <message_id>
func (s *Session) handleAck(args string) error {
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		return ErrMissingParameter
	}

	queueName := parts[0]
	msgID := parts[1]

	if err := s.server.broker.AckMessage(queueName, msgID); err != nil {
		return err
	}

	return s.sendOK("acknowledged")
}

// handleNack handles the NACK command.
// Format: NACK <queue_name> <message_id> [requeue]
func (s *Session) handleNack(args string) error {
	parts := strings.SplitN(args, " ", 3)
	if len(parts) < 2 {
		return ErrMissingParameter
	}

	queueName := parts[0]
	msgID := parts[1]
	requeue := len(parts) < 3 || strings.ToLower(parts[2]) == "true"

	if err := s.server.broker.NackMessage(queueName, msgID, requeue); err != nil {
		return err
	}

	return s.sendOK("nacked")
}

// handleCreateQueue handles the CREATE_QUEUE command.
func (s *Session) handleCreateQueue(args string) error {
	if args == "" {
		return ErrMissingParameter
	}

	if err := s.server.broker.CreateQueue(args); err != nil {
		return err
	}

	return s.sendOK("queue created")
}

// handleDeleteQueue handles the DELETE_QUEUE command.
func (s *Session) handleDeleteQueue(args string) error {
	if args == "" {
		return ErrMissingParameter
	}

	if err := s.server.broker.DeleteQueue(args); err != nil {
		return err
	}

	return s.sendOK("queue deleted")
}

// handleCreateTopic handles the CREATE_TOPIC command.
func (s *Session) handleCreateTopic(args string) error {
	if args == "" {
		return ErrMissingParameter
	}

	if err := s.server.broker.CreateTopic(args); err != nil {
		return err
	}

	return s.sendOK("topic created")
}

// handleDeleteTopic handles the DELETE_TOPIC command.
func (s *Session) handleDeleteTopic(args string) error {
	if args == "" {
		return ErrMissingParameter
	}

	if err := s.server.broker.DeleteTopic(args); err != nil {
		return err
	}

	return s.sendOK("topic deleted")
}

// handleListQueues handles the LIST_QUEUES command.
func (s *Session) handleListQueues() error {
	queues := s.server.broker.ListQueues()
	data, _ := json.Marshal(queues)
	return s.sendOK(string(data))
}

// handleListTopics handles the LIST_TOPICS command.
func (s *Session) handleListTopics() error {
	topics := s.server.broker.ListTopics()
	data, _ := json.Marshal(topics)
	return s.sendOK(string(data))
}

// handleStats handles the STATS command.
func (s *Session) handleStats() error {
	stats := s.server.broker.Stats()
	data, _ := json.Marshal(stats)
	return s.sendOK(string(data))
}

// forwardMessages forwards subscribed messages to the client.
func (s *Session) forwardMessages() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.mu.RLock()
		for queueName, ch := range s.queueSubs {
			select {
			case msg, ok := <-ch:
				if ok && msg != nil {
					s.sendMessage("queue", queueName, msg)
				}
			default:
			}
		}
		for topicName, ch := range s.topicSubs {
			select {
			case msg, ok := <-ch:
				if ok && msg != nil {
					s.sendMessage("topic", topicName, msg)
				}
			default:
			}
		}
		s.mu.RUnlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// sendMessage sends a message to the client.
func (s *Session) sendMessage(sourceType, sourceName string, msg *message.Message) error {
	data := map[string]interface{}{
		"id":        msg.ID(),
		"source":    sourceType,
		"name":      sourceName,
		"payload":   string(msg.Payload()),
		"headers":   msg.Headers(),
		"priority":  msg.Priority(),
		"timestamp": msg.Timestamp().Format(time.RFC3339),
	}
	jsonData, _ := json.Marshal(data)
	return s.send(RespMsg, string(jsonData))
}

// sendOK sends a success response.
func (s *Session) sendOK(data string) error {
	return s.send(RespOK, data)
}

// sendError sends an error response.
func (s *Session) sendError(msg string) error {
	return s.send(RespError, msg)
}

// send sends a response to the client.
func (s *Session) send(code, data string) error {
	if s.server.config.WriteTimeout > 0 {
		s.conn.SetWriteDeadline(time.Now().Add(s.server.config.WriteTimeout))
	}

	var response string
	if data != "" {
		response = fmt.Sprintf("%s %s\n", code, data)
	} else {
		response = fmt.Sprintf("%s\n", code)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.writer.WriteString(response); err != nil {
		return err
	}
	return s.writer.Flush()
}

// close closes the session.
func (s *Session) close() {
	if s.closed.Swap(true) {
		return
	}
	close(s.stopCh)
}

// cleanup cleans up session resources.
func (s *Session) cleanup() {
	s.close()

	// Unsubscribe from all subscriptions
	s.mu.Lock()
	for queueName := range s.queueSubs {
		s.server.broker.UnsubscribeFromQueue(queueName, s.id)
	}
	for topicName := range s.topicSubs {
		s.server.broker.UnsubscribeFromTopic(topicName, s.id)
	}
	s.queueSubs = nil
	s.topicSubs = nil
	s.mu.Unlock()
}

// generateSessionID generates a unique session ID.
func generateSessionID() string {
	return fmt.Sprintf("session-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&sessionCounter, 1))
}

var sessionCounter int64
