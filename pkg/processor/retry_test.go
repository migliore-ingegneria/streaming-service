package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryProcessor_SuccessOnFirstTry(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := DefaultRetryConfig()

	var attempts int32
	handler := func(ctx context.Context, evt StreamEvent) error {
		atomic.AddInt32(&attempts, 1)
		return nil
	}

	var dlqCalled bool
	dlqHandler := func(ctx context.Context, evt StreamEvent, cause error) error {
		dlqCalled = true
		return nil
	}

	proc := NewRetryProcessor(cfg, handler, dlqHandler, reg)

	evt := StreamEvent{
		ID:        "evt-1",
		Partition: "p-0",
		Payload:   []byte("test-data"),
		Timestamp: time.Now(),
	}

	err := proc.Process(context.Background(), evt)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
	assert.False(t, dlqCalled)
}

func TestRetryProcessor_SuccessAfterRetry(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := RetryConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		BackoffFactor:  2.0,
		MaxRetries:     3,
	}

	var attempts int32
	handler := func(ctx context.Context, evt StreamEvent) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return errors.New("temporary processing failure")
		}
		return nil
	}

	var dlqCalled bool
	dlqHandler := func(ctx context.Context, evt StreamEvent, cause error) error {
		dlqCalled = true
		return nil
	}

	proc := NewRetryProcessor(cfg, handler, dlqHandler, reg)

	evt := StreamEvent{
		ID:        "evt-2",
		Partition: "p-1",
		Payload:   []byte("test-data"),
		Timestamp: time.Now(),
	}

	err := proc.Process(context.Background(), evt)
	require.NoError(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	assert.False(t, dlqCalled)
}

func TestRetryProcessor_MaxRetriesExceeded_DLQRouting(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := RetryConfig{
		InitialBackoff: 2 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		MaxRetries:     2,
	}

	mockErr := errors.New("persistent upstream error")

	var attempts int32
	handler := func(ctx context.Context, evt StreamEvent) error {
		atomic.AddInt32(&attempts, 1)
		return mockErr
	}

	var dlqEvt StreamEvent
	var dlqCause error
	dlqHandler := func(ctx context.Context, evt StreamEvent, cause error) error {
		dlqEvt = evt
		dlqCause = cause
		return nil
	}

	proc := NewRetryProcessor(cfg, handler, dlqHandler, reg)

	evt := StreamEvent{
		ID:        "evt-3",
		Partition: "p-2",
		Payload:   []byte("corrupt-data"),
		Timestamp: time.Now(),
	}

	err := proc.Process(context.Background(), evt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "routed to DLQ")

	// Total calls = 1 initial + 2 retries = 3 attempts
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	assert.Equal(t, "evt-3", dlqEvt.ID)
	assert.Equal(t, mockErr, dlqCause)
}

func TestRetryProcessor_ContextCancellation(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := RetryConfig{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
		MaxRetries:     5,
	}

	handler := func(ctx context.Context, evt StreamEvent) error {
		return errors.New("transient error")
	}

	proc := NewRetryProcessor(cfg, handler, nil, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	evt := StreamEvent{
		ID:        "evt-4",
		Partition: "p-0",
		Payload:   []byte("data"),
		Timestamp: time.Now(),
	}

	err := proc.Process(ctx, evt)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}
