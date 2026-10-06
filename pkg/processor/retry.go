package processor

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// StreamEvent represents an incoming unprocessable stream event payload.
type StreamEvent struct {
	ID        string
	Partition string
	Payload   []byte
	Timestamp time.Time
}

// RetryConfig defines exponential backoff retry parameters.
type RetryConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	MaxRetries     int
}

// DefaultRetryConfig returns standard retry backoff configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		MaxRetries:     3,
	}
}

// HandlerFunc processes a stream event.
type HandlerFunc func(ctx context.Context, event StreamEvent) error

// DLQHandlerFunc handles events that have exceeded max retries.
type DLQHandlerFunc func(ctx context.Context, event StreamEvent, cause error) error

// RetryProcessor manages retries with exponential backoff and DLQ routing.
type RetryProcessor struct {
	config     RetryConfig
	handler    HandlerFunc
	dlqHandler DLQHandlerFunc
	mu         sync.RWMutex

	counterRetries *prometheus.CounterVec
	counterDLQ     *prometheus.CounterVec
}

// NewRetryProcessor initializes RetryProcessor with Prometheus metric counters.
func NewRetryProcessor(config RetryConfig, handler HandlerFunc, dlqHandler DLQHandlerFunc, reg prometheus.Registerer) *RetryProcessor {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	p := &RetryProcessor{
		config:     config,
		handler:    handler,
		dlqHandler: dlqHandler,

		counterRetries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "stream_processor_retry_attempts_total",
				Help: "Total retry attempts performed for stream events",
			},
			[]string{"partition", "status"},
		),
		counterDLQ: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "stream_processor_dlq_events_total",
				Help: "Total stream events routed to the Dead Letter Queue",
			},
			[]string{"partition", "reason"},
		),
	}

	_ = reg.Register(p.counterRetries)
	_ = reg.Register(p.counterDLQ)

	return p
}

// Process attempts to process an event with exponential backoff retries, routing to DLQ on exhaustion.
func (r *RetryProcessor) Process(ctx context.Context, event StreamEvent) error {
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := r.handler(ctx, event)
		if err == nil {
			if attempt > 0 {
				r.counterRetries.WithLabelValues(event.Partition, "success").Inc()
			}
			return nil
		}

		lastErr = err

		if attempt < r.config.MaxRetries {
			r.counterRetries.WithLabelValues(event.Partition, "retry").Inc()

			// Calculate exponential backoff: initial * (factor ^ attempt)
			backoff := float64(r.config.InitialBackoff) * math.Pow(r.config.BackoffFactor, float64(attempt))
			duration := time.Duration(backoff)
			if duration > r.config.MaxBackoff {
				duration = r.config.MaxBackoff
			}

			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Max retries exceeded -> Route event to Dead-Letter Queue (DLQ)
	r.counterDLQ.WithLabelValues(event.Partition, "max_retries_exceeded").Inc()

	if r.dlqHandler != nil {
		dlqErr := r.dlqHandler(ctx, event, lastErr)
		if dlqErr != nil {
			return fmt.Errorf("dlq routing failed for event %s: %w (original error: %v)", event.ID, dlqErr, lastErr)
		}
	}

	return fmt.Errorf("event %s failed after %d retries and routed to DLQ: %w", event.ID, r.config.MaxRetries, lastErr)
}
