package tenant

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIsolation(t *testing.T) {
	mgr := NewNamespaceManager(DefaultTenantQuota())

	// Valid partition qualification
	qualified, err := mgr.QualifyPartition("tenant-alpha", "events-v1")
	require.NoError(t, err)
	assert.Equal(t, "tenant-alpha:events-v1", qualified)

	// Valid partition parsing
	tenantID, rawPart, err := mgr.ParseTenantFromPartition(qualified)
	require.NoError(t, err)
	assert.Equal(t, "tenant-alpha", tenantID)
	assert.Equal(t, "events-v1", rawPart)

	// Malformed partition cases
	_, _, err = mgr.ParseTenantFromPartition("invalid-partition-without-colon")
	assert.ErrorIs(t, err, ErrMalformedQualifiedPartition)

	_, _, err = mgr.ParseTenantFromPartition(":events")
	assert.ErrorIs(t, err, ErrMalformedQualifiedPartition)

	// Empty tenant or partition
	_, err = mgr.QualifyPartition("", "events")
	assert.ErrorIs(t, err, ErrInvalidTenantID)

	_, err = mgr.QualifyPartition("tenant-1", "")
	assert.ErrorIs(t, err, ErrInvalidPartition)

	// Metric qualification
	metricName := mgr.QualifyMetricName("Tenant-Alpha", "ingest_latency_ms")
	assert.Equal(t, "tenant_tenant_alpha_ingest_latency_ms", metricName)
}

func TestQuotaEnforcement_MessageAndByteLimits(t *testing.T) {
	quota := TenantQuota{
		MaxMsgsPerSec:  10,
		BurstMsgs:      50,
		MaxBytesPerSec: 1024, // 1 KB/s
		BurstBytes:     4096, // 4 KB burst
	}
	mgr := NewNamespaceManager(quota)
	ctx := context.Background()

	// Initial ingress within burst bounds
	err := mgr.AllowIngress(ctx, "tenant-1", 30, 2048)
	require.NoError(t, err)

	// Subsequent ingress within remaining burst (20 msgs, 2048 bytes remaining)
	err = mgr.AllowIngress(ctx, "tenant-1", 15, 1024)
	require.NoError(t, err)

	// Request exceeding remaining message burst (remaining: 5 msgs)
	err = mgr.AllowIngress(ctx, "tenant-1", 10, 500)
	require.Error(t, err)

	var quotaErr *QuotaExceededError
	require.True(t, errors.As(err, &quotaErr))
	assert.Equal(t, "tenant-1", quotaErr.TenantID)
	assert.Equal(t, QuotaTypeMessages, quotaErr.QuotaType)
	assert.Equal(t, int64(10), quotaErr.Requested)
	assert.Equal(t, http.StatusTooManyRequests, quotaErr.HTTPStatusCode)
	assert.True(t, quotaErr.RetryAfter > 0)
	assert.Contains(t, quotaErr.Error(), "429 Too Many Requests")

	// Message rollback check: when message check passes but byte check fails
	// Current remaining: ~5 msgs, ~1024 bytes
	err = mgr.AllowIngress(ctx, "tenant-1", 2, 5000) // 5000 bytes exceeds 4096 max burst
	require.Error(t, err)
	require.True(t, errors.As(err, &quotaErr))
	assert.Equal(t, QuotaTypeBytes, quotaErr.QuotaType)
	assert.Equal(t, int64(5000), quotaErr.Requested)

	// Message tokens were rolled back, so valid 2-msg request still succeeds
	err = mgr.AllowIngress(ctx, "tenant-1", 2, 500)
	require.NoError(t, err)
}

func TestQuotaReplenishment(t *testing.T) {
	quota := TenantQuota{
		MaxMsgsPerSec:  50, // 50 msgs/sec -> 1 msg every 20ms
		BurstMsgs:      5,
		MaxBytesPerSec: 5000,
		BurstBytes:     500,
	}
	mgr := NewNamespaceManager(quota)
	ctx := context.Background()

	// Exhaust burst
	err := mgr.AllowIngress(ctx, "tenant-replenish", 5, 250)
	require.NoError(t, err)

	// Next immediate request rejected
	err = mgr.AllowIngress(ctx, "tenant-replenish", 1, 50)
	require.Error(t, err)

	// Wait 60ms -> replenishes ~3 tokens
	time.Sleep(60 * time.Millisecond)

	err = mgr.AllowIngress(ctx, "tenant-replenish", 2, 100)
	require.NoError(t, err)
}

func TestConcurrentTenantAccess(t *testing.T) {
	quota := TenantQuota{
		MaxMsgsPerSec:  1000,
		BurstMsgs:      5000,
		MaxBytesPerSec: 100000,
		BurstBytes:     500000,
	}
	mgr := NewNamespaceManager(quota)
	ctx := context.Background()

	var wg sync.WaitGroup
	var successfulOps int64
	numGoroutines := 20
	opsPerGoroutine := 100

	tenants := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		tenant := tenants[i%len(tenants)]
		go func(tName string) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if err := mgr.AllowIngress(ctx, tName, 1, 10); err == nil {
					atomic.AddInt64(&successfulOps, 1)
				}
			}
		}(tenant)
	}

	wg.Wait()
	assert.Equal(t, int64(numGoroutines*opsPerGoroutine), atomic.LoadInt64(&successfulOps))
	assert.Equal(t, len(tenants), mgr.TenantCount())
}

func TestTenantOverridesAndRemoval(t *testing.T) {
	defaultQuota := TenantQuota{
		MaxMsgsPerSec: 10,
		BurstMsgs:     20,
	}
	mgr := NewNamespaceManager(defaultQuota)
	ctx := context.Background()

	// Default tenant
	err := mgr.AllowIngress(ctx, "standard-tenant", 15, 0)
	require.NoError(t, err)

	// Custom VIP tenant with 10x capacity
	vipQuota := TenantQuota{
		MaxMsgsPerSec: 1000,
		BurstMsgs:     2000,
	}
	err = mgr.RegisterTenant("vip-tenant", vipQuota)
	require.NoError(t, err)

	err = mgr.AllowIngress(ctx, "vip-tenant", 500, 0)
	require.NoError(t, err)

	q, ok := mgr.GetTenantQuota("vip-tenant")
	require.True(t, ok)
	assert.Equal(t, int64(2000), q.BurstMsgs)

	// Removal
	assert.Equal(t, 2, mgr.TenantCount())
	assert.True(t, mgr.RemoveTenant("standard-tenant"))
	assert.False(t, mgr.RemoveTenant("standard-tenant"))
	assert.Equal(t, 1, mgr.TenantCount())
}
