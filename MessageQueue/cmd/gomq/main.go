// Package main provides the entry point for the GoMQ message queue server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/messagequeue/internal/broker"
)

// Version information (set via ldflags during build).
var (
	version   = "dev"
	buildTime = "unknown"
)

// Command-line flags.
var (
	listenAddr      = flag.String("addr", ":5672", "Server listen address")
	maxQueues       = flag.Int("max-queues", 1000, "Maximum number of queues")
	maxTopics       = flag.Int("max-topics", 1000, "Maximum number of topics")
	defaultTTL      = flag.Duration("default-ttl", 24*time.Hour, "Default message TTL")
	shutdownTimeout = flag.Duration("shutdown-timeout", 30*time.Second, "Graceful shutdown timeout")
	enableDLQ       = flag.Bool("enable-dlq", true, "Enable dead letter queues")
	enableMetrics   = flag.Bool("enable-metrics", true, "Enable metrics collection")
	showVersion     = flag.Bool("version", false, "Show version information")
)

func main() {
	flag.Parse()

	// Show version and exit if requested
	if *showVersion {
		fmt.Printf("GoMQ Message Queue Server\n")
		fmt.Printf("  Version:    %s\n", version)
		fmt.Printf("  Build Time: %s\n", buildTime)
		os.Exit(0)
	}

	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("Starting GoMQ Message Queue Server v%s", version)

	// Create broker configuration
	cfg := broker.Config{
		DefaultTTL:      *defaultTTL,
		MaxQueues:       *maxQueues,
		MaxTopics:       *maxTopics,
		EnableMetrics:   *enableMetrics,
		MetricsInterval: 10 * time.Second,
		EnableDLQ:       *enableDLQ,
		DLQSuffix:       "-dlq",
		ShutdownTimeout: *shutdownTimeout,
	}

	// Create and start the broker
	b := broker.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	log.Printf("Broker started successfully")
	log.Printf("  Max Queues:   %d", cfg.MaxQueues)
	log.Printf("  Max Topics:   %d", cfg.MaxTopics)
	log.Printf("  Default TTL:  %s", cfg.DefaultTTL)
	log.Printf("  DLQ Enabled:  %v", cfg.EnableDLQ)
	log.Printf("  Metrics:      %v", cfg.EnableMetrics)

	// Create some default queues and topics for demonstration
	if _, err := b.CreateQueue("default"); err != nil {
		log.Printf("Warning: Failed to create default queue: %v", err)
	}
	if _, err := b.CreateTopic("events"); err != nil {
		log.Printf("Warning: Failed to create events topic: %v", err)
	}

	log.Printf("Server ready to accept connections")
	log.Printf("Note: TCP server is available via the server package")
	log.Printf("      This binary demonstrates embedded broker usage")

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer shutdownCancel()

	// Shutdown broker
	if err := b.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during shutdown: %v", err)
		os.Exit(1)
	}

	// Print final statistics
	stats := b.Stats()
	log.Printf("Final Statistics:")
	log.Printf("  Uptime:            %s", stats.Uptime)
	log.Printf("  Total Published:   %d", stats.TotalPublished)
	log.Printf("  Total Consumed:    %d", stats.TotalConsumed)
	log.Printf("  Total Dead Letters: %d", stats.TotalDeadLettered)
	log.Printf("  Queues:            %d", stats.QueueCount)
	log.Printf("  Topics:            %d", stats.TopicCount)

	log.Printf("Shutdown complete")
}
