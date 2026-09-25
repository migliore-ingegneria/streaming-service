package aggregator

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Event represents an incoming telemetry data point with an event timestamp and latency value.
type Event struct {
	Partition string
	Value     float64
	Timestamp time.Time
}

// WindowResult holds computed metrics for a single partition window.
type WindowResult struct {
	Partition string
	WindowEnd time.Time
	Sum       float64
	Count     int64
	Min       float64
	Max       float64
	P95       float64
}

// SlidingWindowConfig configures window length, slide interval, and lateness tolerance watermark.
type SlidingWindowConfig struct {
	WindowSize        time.Duration
	SlideInterval     time.Duration
	LatenessTolerance time.Duration
}

// DefaultConfig returns default 5-minute window with 1-minute slide and 10s lateness watermark.
func DefaultConfig() SlidingWindowConfig {
	return SlidingWindowConfig{
		WindowSize:        5 * time.Minute,
		SlideInterval:     1 * time.Minute,
		LatenessTolerance: 10 * time.Second,
	}
}

// SlidingWindowAggregator manages event-time sliding windows per partition.
type SlidingWindowAggregator struct {
	config    SlidingWindowConfig
	mu        sync.RWMutex
	events    map[string][]Event
	watermark time.Time

	// Prometheus Gauge Vectors
	gaugeSum   *prometheus.GaugeVec
	gaugeCount *prometheus.GaugeVec
	gaugeMin   *prometheus.GaugeVec
	gaugeMax   *prometheus.GaugeVec
	gaugeP95   *prometheus.GaugeVec
}

// NewSlidingWindowAggregator initializes the aggregator with Prometheus metrics.
func NewSlidingWindowAggregator(config SlidingWindowConfig, reg prometheus.Registerer) *SlidingWindowAggregator {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	agg := &SlidingWindowAggregator{
		config: config,
		events: make(map[string][]Event),

		gaugeSum: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "stream_window_sum",
				Help: "Sum of metric values in the current sliding window",
			},
			[]string{"partition"},
		),
		gaugeCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "stream_window_count",
				Help: "Total count of events in the current sliding window",
			},
			[]string{"partition"},
		),
		gaugeMin: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "stream_window_min",
				Help: "Minimum value in the current sliding window",
			},
			[]string{"partition"},
		),
		gaugeMax: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "stream_window_max",
				Help: "Maximum value in the current sliding window",
			},
			[]string{"partition"},
		),
		gaugeP95: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "stream_window_p95",
				Help: "95th percentile latency in the current sliding window",
			},
			[]string{"partition"},
		),
	}

	for _, collector := range []prometheus.Collector{agg.gaugeSum, agg.gaugeCount, agg.gaugeMin, agg.gaugeMax, agg.gaugeP95} {
		_ = reg.Register(collector)
	}

	return agg
}

// Ingest adds an event to the aggregator, verifying against event-time watermarks.
func (s *SlidingWindowAggregator) Ingest(evt Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update watermark to track latest event time
	if evt.Timestamp.After(s.watermark) {
		s.watermark = evt.Timestamp
	}

	// Check if event is too late beyond watermark tolerance
	if !s.watermark.IsZero() && evt.Timestamp.Before(s.watermark.Add(-s.config.LatenessTolerance)) {
		return fmt.Errorf("event dropped: timestamp %v is beyond lateness tolerance watermark %v",
			evt.Timestamp.Format(time.RFC3339),
			s.watermark.Add(-s.config.LatenessTolerance).Format(time.RFC3339))
	}

	s.events[evt.Partition] = append(s.events[evt.Partition], evt)
	return nil
}

// EvaluateWindow computes window metrics for all partitions up to referenceTime and updates Prometheus gauges.
func (s *SlidingWindowAggregator) EvaluateWindow(referenceTime time.Time) map[string]WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	windowStart := referenceTime.Add(-s.config.WindowSize)
	results := make(map[string]WindowResult)

	for partition, evts := range s.events {
		var validEvts []Event
		var values []float64
		var sum float64
		minVal := math.MaxFloat64
		maxVal := -math.MaxFloat64

		for _, e := range evts {
			// Keep events within window boundary
			if (e.Timestamp.After(windowStart) || e.Timestamp.Equal(windowStart)) &&
				(e.Timestamp.Before(referenceTime) || e.Timestamp.Equal(referenceTime)) {

				validEvts = append(validEvts, e)
				values = append(values, e.Value)
				sum += e.Value

				if e.Value < minVal {
					minVal = e.Value
				}
				if e.Value > maxVal {
					maxVal = e.Value
				}
			}
		}

		// Retain only events that are still relevant for future windows
		s.events[partition] = validEvts

		if len(values) == 0 {
			continue
		}

		sort.Float64s(values)
		p95Index := int(math.Ceil(0.95*float64(len(values)))) - 1
		if p95Index < 0 {
			p95Index = 0
		}
		if p95Index >= len(values) {
			p95Index = len(values) - 1
		}
		p95Val := values[p95Index]

		res := WindowResult{
			Partition: partition,
			WindowEnd: referenceTime,
			Sum:       sum,
			Count:     int64(len(values)),
			Min:       minVal,
			Max:       maxVal,
			P95:       p95Val,
		}

		results[partition] = res

		// Update Prometheus Gauge Metrics
		s.gaugeSum.WithLabelValues(partition).Set(res.Sum)
		s.gaugeCount.WithLabelValues(partition).Set(float64(res.Count))
		s.gaugeMin.WithLabelValues(partition).Set(res.Min)
		s.gaugeMax.WithLabelValues(partition).Set(res.Max)
		s.gaugeP95.WithLabelValues(partition).Set(res.P95)
	}

	return results
}
