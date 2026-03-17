package verify

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vmAPI(t *testing.T) promv1.API {
	t.Helper()
	addr := os.Getenv("VM_URL")
	if addr == "" {
		addr = "http://localhost:8428"
	}
	client, err := api.NewClient(api.Config{Address: addr})
	require.NoError(t, err)
	return promv1.NewAPI(client)
}

func queryScalar(t *testing.T, v1api promv1.API, promql string) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, warnings, err := v1api.Query(ctx, promql, time.Now())
	require.NoError(t, err)
	require.Empty(t, warnings)

	vec, ok := result.(model.Vector)
	require.True(t, ok, "expected vector result for query: %s", promql)
	require.NotEmpty(t, vec, "no data for query: %s", promql)
	return float64(vec[0].Value)
}

// --- 場景 A：NATS Queue ---
func TestScenarioA_NATSQueue(t *testing.T) {
	v := vmAPI(t)
	val := queryScalar(t, v,
		`traces_service_graph_request_total{client="order-service",server="payment-service",connection_type="nats",edge_relation="link"}`)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- 場景 B：MongoDB Change Stream ---
func TestScenarioB_MongoDBChangeStream(t *testing.T) {
	v := vmAPI(t)
	val := queryScalar(t, v,
		`traces_service_graph_request_total{client="order-service",server="sync-service",connection_type="mongodb",edge_relation="link"}`)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- 場景 C：NATS Fan-out ---
func TestScenarioC_NATSFanOut(t *testing.T) {
	v := vmAPI(t)
	for _, server := range []string{"payment-svc", "inventory-svc", "notification-svc"} {
		t.Run(server, func(t *testing.T) {
			val := queryScalar(t, v, fmt.Sprintf(
				`traces_service_graph_request_total{client="event-source",server="%s",edge_relation="link"}`, server))
			assert.GreaterOrEqual(t, val, float64(1))
		})
	}
}

// --- 場景 D：跨批次到達 ---
func TestScenarioD_CrossBatch(t *testing.T) {
	v := vmAPI(t)
	val := queryScalar(t, v,
		`traces_service_graph_request_total{client="delayed-producer",server="eager-consumer",edge_relation="link"}`)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- 場景 E：Retry ---
func TestScenarioE_Retry(t *testing.T) {
	v := vmAPI(t)
	val := queryScalar(t, v,
		`traces_service_graph_request_total{client="order-service",server="order-service",edge_relation="link"}`)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Histogram 驗證 ---
func TestHistograms(t *testing.T) {
	v := vmAPI(t)

	t.Run("server_histogram_count", func(t *testing.T) {
		val := queryScalar(t, v,
			`count(traces_service_graph_request_server_count{edge_relation="link"})`)
		assert.Greater(t, val, float64(0))
	})

	t.Run("client_histogram_count", func(t *testing.T) {
		val := queryScalar(t, v,
			`count(traces_service_graph_request_client_count{edge_relation="link"})`)
		assert.Greater(t, val, float64(0))
	})

	t.Run("server_histogram_sum_positive", func(t *testing.T) {
		val := queryScalar(t, v,
			`sum(traces_service_graph_request_server_sum{edge_relation="link"})`)
		assert.Greater(t, val, float64(0), "server duration sum should be > 0")
	})
}

// --- Mixed middleware（NATS + MongoDB） ---
func TestScenarioF_MixedMiddleware(t *testing.T) {
	v := vmAPI(t)

	t.Run("nats_leg", func(t *testing.T) {
		val := queryScalar(t, v,
			`traces_service_graph_request_total{client="order-service",server="payment-service",connection_type="nats",edge_relation="link"}`)
		assert.GreaterOrEqual(t, val, float64(1))
	})

	t.Run("mongodb_leg", func(t *testing.T) {
		val := queryScalar(t, v,
			`traces_service_graph_request_total{client="payment-service",server="sync-service",connection_type="mongodb",edge_relation="link"}`)
		assert.GreaterOrEqual(t, val, float64(1))
	})
}
