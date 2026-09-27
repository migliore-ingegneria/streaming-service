package aggregator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestSlidingWindowAggregator_IngestAndEvaluate(t *testing.T) {
	cfg := SlidingWindowConfig{
		WindowSize:        5 * time.Minute,
		SlideInterval:     1 * time.Minute,
		LatenessTolerance: 10 * time.Second,
	}

	agg := NewSlidingWindowAggregator(cfg, prometheus.NewRegistry())
	now := time.Now()

	// Ingest valid events for partition "video_stream_1"
	events := []float64{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0}
	for i, val := range events {
		err := agg.Ingest(Event{
			Partition: "video_stream_1",
			Value:     val,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("unexpected error ingesting event %d: %v", i, err)
		}
	}

	// Evaluate window at reference time now + 10s
	refTime := now.Add(10 * time.Second)
	results := agg.EvaluateWindow(refTime)

	res, ok := results["video_stream_1"]
	if !ok {
		t.Fatalf("expected results for partition video_stream_1")
	}

	if res.Count != 10 {
		t.Errorf("expected count 10, got %d", res.Count)
	}
	if res.Sum != 550.0 {
		t.Errorf("expected sum 550.0, got %f", res.Sum)
	}
	if res.Min != 10.0 {
		t.Errorf("expected min 10.0, got %f", res.Min)
	}
	if res.Max != 100.0 {
		t.Errorf("expected max 100.0, got %f", res.Max)
	}
	if res.P95 != 100.0 {
		t.Errorf("expected p95 100.0, got %f", res.P95)
	}
}

func TestSlidingWindowAggregator_LateEventRejection(t *testing.T) {
	cfg := SlidingWindowConfig{
		WindowSize:        5 * time.Minute,
		SlideInterval:     1 * time.Minute,
		LatenessTolerance: 10 * time.Second,
	}

	agg := NewSlidingWindowAggregator(cfg, prometheus.NewRegistry())
	now := time.Now()

	// Advance watermark to now + 30s
	err := agg.Ingest(Event{
		Partition: "audio_stream_1",
		Value:     50.0,
		Timestamp: now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try ingesting an event that is 20s behind watermark (beyond 10s lateness tolerance)
	lateEvent := Event{
		Partition: "audio_stream_1",
		Value:     15.0,
		Timestamp: now,
	}

	err = agg.Ingest(lateEvent)
	if err == nil {
		t.Errorf("expected late event to be rejected beyond 10s watermark tolerance")
	}
}
