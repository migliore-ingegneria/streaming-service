package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidTenantID indicates an empty or malformed tenant identifier.
	ErrInvalidTenantID = errors.New("invalid tenant identifier")
	// ErrInvalidPartition indicates an empty or invalid partition name.
	ErrInvalidPartition = errors.New("invalid partition name")
	// ErrMalformedQualifiedPartition indicates partition string does not match tenant:partition schema.
	ErrMalformedQualifiedPartition = errors.New("malformed qualified partition name")
	// ErrTenantNotFound indicates the requested tenant does not exist.
	ErrTenantNotFound = errors.New("tenant not found")
)

// QuotaType classifies the resource boundary that was violated.
type QuotaType string

const (
	// QuotaTypeMessages represents message count ingress limits.
	QuotaTypeMessages QuotaType = "messages"
	// QuotaTypeBytes represents raw payload byte ingress limits.
	QuotaTypeBytes QuotaType = "bytes"
)

// QuotaExceededError is returned when a tenant exceeds message or byte ingress limits.
// It maps directly to HTTP 429 Too Many Requests with recommended backoff guidance.
type QuotaExceededError struct {
	TenantID       string
	QuotaType      QuotaType
	Requested      int64
	Available      int64
	RetryAfter     time.Duration
	HTTPStatusCode int
}

func (e *QuotaExceededError) Error() string {
	return fmt.Sprintf("429 Too Many Requests: tenant '%s' exceeded %s quota (requested %d, available %d); retry after %s",
		e.TenantID, e.QuotaType, e.Requested, e.Available, e.RetryAfter.Round(time.Millisecond))
}

// TenantQuota defines sustained rates and burst ceilings for both messages and payload bytes.
type TenantQuota struct {
	// MaxMsgsPerSec is the sustained message throughput allowed per second.
	MaxMsgsPerSec int64
	// BurstMsgs is the maximum instantaneous burst of messages permitted.
	BurstMsgs int64
	// MaxBytesPerSec is the sustained byte throughput allowed per second.
	MaxBytesPerSec int64
	// BurstBytes is the maximum instantaneous burst of bytes permitted.
	BurstBytes int64
}

// DefaultTenantQuota provides standard baseline quota settings.
func DefaultTenantQuota() TenantQuota {
	return TenantQuota{
		MaxMsgsPerSec:  1000,
		BurstMsgs:      2000,
		MaxBytesPerSec: 5 * 1024 * 1024,  // 5 MB/s sustained
		BurstBytes:     10 * 1024 * 1024, // 10 MB burst
	}
}

// tokenBucket models a single thread-safe token bucket with fractional replenishment.
type tokenBucket struct {
	ratePerSec int64
	capacity   int64
	tokens     float64
	lastRefill time.Time
}

func newTokenBucket(ratePerSec, capacity int64) *tokenBucket {
	return &tokenBucket{
		ratePerSec: ratePerSec,
		capacity:   capacity,
		tokens:     float64(capacity),
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) tryConsume(now time.Time, count int64) (bool, int64, time.Duration) {
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	// Replenish tokens based on elapsed duration
	tb.tokens = tb.tokens + elapsed*float64(tb.ratePerSec)
	if tb.tokens > float64(tb.capacity) {
		tb.tokens = float64(tb.capacity)
	}

	available := int64(tb.tokens)
	if available >= count {
		tb.tokens -= float64(count)
		return true, int64(tb.tokens), 0
	}

	// Calculate needed backoff duration until required tokens are available
	needed := count - available
	var retryAfter time.Duration
	if tb.ratePerSec > 0 {
		waitSec := float64(needed) / float64(tb.ratePerSec)
		retryAfter = time.Duration(waitSec * float64(time.Second))
		if retryAfter < time.Millisecond {
			retryAfter = time.Millisecond
		}
	} else {
		retryAfter = time.Second
	}

	return false, available, retryAfter
}

// tenantContext encapsulates rate limiting state and isolation metadata for a tenant.
type tenantContext struct {
	mu          sync.Mutex
	quota       TenantQuota
	msgBucket   *tokenBucket
	bytesBucket *tokenBucket
}

// NamespaceManager provides tenant isolation, partition namespace qualification,
// and token bucket ingress quota enforcement.
type NamespaceManager struct {
	mu           sync.RWMutex
	defaultQuota TenantQuota
	tenants      map[string]*tenantContext
}

// NewNamespaceManager initializes a new namespace manager with default fallback quota.
func NewNamespaceManager(defaultQuota TenantQuota) *NamespaceManager {
	return &NamespaceManager{
		defaultQuota: defaultQuota,
		tenants:      make(map[string]*tenantContext),
	}
}

// RegisterTenant configures or overrides resource quotas for a specific tenant.
func (m *NamespaceManager) RegisterTenant(tenantID string, quota TenantQuota) error {
	if strings.TrimSpace(tenantID) == "" {
		return ErrInvalidTenantID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.tenants[tenantID] = &tenantContext{
		quota:       quota,
		msgBucket:   newTokenBucket(quota.MaxMsgsPerSec, quota.BurstMsgs),
		bytesBucket: newTokenBucket(quota.MaxBytesPerSec, quota.BurstBytes),
	}
	return nil
}

// getOrCreateTenantContext retrieves existing tenant context or registers default context.
func (m *NamespaceManager) getOrCreateTenantContext(tenantID string) (*tenantContext, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, ErrInvalidTenantID
	}

	m.mu.RLock()
	tc, exists := m.tenants[tenantID]
	m.mu.RUnlock()
	if exists {
		return tc, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-checked locking
	if tc, exists := m.tenants[tenantID]; exists {
		return tc, nil
	}

	tc = &tenantContext{
		quota:       m.defaultQuota,
		msgBucket:   newTokenBucket(m.defaultQuota.MaxMsgsPerSec, m.defaultQuota.BurstMsgs),
		bytesBucket: newTokenBucket(m.defaultQuota.MaxBytesPerSec, m.defaultQuota.BurstBytes),
	}
	m.tenants[tenantID] = tc
	return tc, nil
}

// QualifyPartition creates an isolated, namespaced partition key formatted as `tenant_id:partition`.
func (m *NamespaceManager) QualifyPartition(tenantID, partition string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	partition = strings.TrimSpace(partition)
	if tenantID == "" {
		return "", ErrInvalidTenantID
	}
	if partition == "" {
		return "", ErrInvalidPartition
	}
	return fmt.Sprintf("%s:%s", tenantID, partition), nil
}

// ParseTenantFromPartition extracts the tenant ID and raw partition name from a qualified partition key.
func (m *NamespaceManager) ParseTenantFromPartition(qualifiedPartition string) (string, string, error) {
	parts := strings.SplitN(qualifiedPartition, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrMalformedQualifiedPartition
	}
	return parts[0], parts[1], nil
}

// QualifyMetricName prefixes metric names with tenant isolation identifiers.
func (m *NamespaceManager) QualifyMetricName(tenantID, metricName string) string {
	sanitizedTenant := strings.ReplaceAll(strings.ToLower(tenantID), "-", "_")
	return fmt.Sprintf("tenant_%s_%s", sanitizedTenant, metricName)
}

// AllowIngress evaluates whether an ingress request containing msgCount messages
// and byteSize payload bytes is permitted under the tenant's token bucket quotas.
// If quota is exhausted, returns *QuotaExceededError with HTTP 429 status.
func (m *NamespaceManager) AllowIngress(ctx context.Context, tenantID string, msgCount int64, byteSize int64) error {
	tc, err := m.getOrCreateTenantContext(tenantID)
	if err != nil {
		return err
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	now := time.Now()

	// Check message quota
	if msgCount > 0 {
		ok, avail, retryAfter := tc.msgBucket.tryConsume(now, msgCount)
		if !ok {
			return &QuotaExceededError{
				TenantID:       tenantID,
				QuotaType:      QuotaTypeMessages,
				Requested:      msgCount,
				Available:      avail,
				RetryAfter:     retryAfter,
				HTTPStatusCode: http.StatusTooManyRequests,
			}
		}
	}

	// Check byte rate quota
	if byteSize > 0 {
		ok, avail, retryAfter := tc.bytesBucket.tryConsume(now, byteSize)
		if !ok {
			// Roll back message tokens consumed in this transaction if byte check fails
			if msgCount > 0 {
				tc.msgBucket.tokens += float64(msgCount)
			}
			return &QuotaExceededError{
				TenantID:       tenantID,
				QuotaType:      QuotaTypeBytes,
				Requested:      byteSize,
				Available:      avail,
				RetryAfter:     retryAfter,
				HTTPStatusCode: http.StatusTooManyRequests,
			}
		}
	}

	return nil
}

// GetTenantQuota returns the active quota configuration for a tenant.
func (m *NamespaceManager) GetTenantQuota(tenantID string) (TenantQuota, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tc, exists := m.tenants[tenantID]
	if !exists {
		return TenantQuota{}, false
	}
	return tc.quota, true
}

// RemoveTenant unregisters a tenant and purges its token buckets from memory.
func (m *NamespaceManager) RemoveTenant(tenantID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tenants[tenantID]; exists {
		delete(m.tenants, tenantID)
		return true
	}
	return false
}

// TenantCount returns the total number of registered tenants.
func (m *NamespaceManager) TenantCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tenants)
}
