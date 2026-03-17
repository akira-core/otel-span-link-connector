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

// queryScalarEventually waits for the metric to appear (handles collector flush delay).
func queryScalarEventually(t *testing.T, v1api promv1.API, promql string, minVal float64) float64 {
	t.Helper()
	var val float64
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, warnings, err := v1api.Query(ctx, promql, time.Now())
		if err != nil || len(warnings) > 0 {
			return false
		}
		vec, ok := result.(model.Vector)
		if !ok || vec.Len() == 0 {
			return false
		}
		val = float64(vec[0].Value)
		return val >= minVal
	}, 35*time.Second, 2*time.Second, "metric %s did not reach >= %v", promql, minVal)
	return val
}

// --- Scenario A: NATS Queue ---
func TestScenarioA_NATSQueue(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="order-service",server="payment-service",connection_type="nats",edge_relation="link"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Scenario B: MongoDB Change Stream ---
func TestScenarioB_MongoDBChangeStream(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="order-service",server="sync-service",connection_type="mongodb",edge_relation="link"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Scenario C: NATS Fan-out ---
func TestScenarioC_NATSFanOut(t *testing.T) {
	v := vmAPI(t)
	for _, server := range []string{"payment-svc", "inventory-svc", "notification-svc"} {
		t.Run(server, func(t *testing.T) {
			val := queryScalarEventually(t, v, fmt.Sprintf(
				`traces_service_graph_request_total{client="event-source",server="%s",edge_relation="link"}`, server), 1)
			assert.GreaterOrEqual(t, val, float64(1))
		})
	}
}

// --- Scenario D: Cross-batch ---
func TestScenarioD_CrossBatch(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="delayed-producer",server="eager-consumer",edge_relation="link"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Scenario E: Retry ---
func TestScenarioE_Retry(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="order-service",server="order-service",edge_relation="link"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Histogram verification ---
func TestHistograms(t *testing.T) {
	v := vmAPI(t)
	t.Run("server_histogram_count", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`count(traces_service_graph_request_server_milliseconds_count{edge_relation="link"})`, 0.5)
		assert.Greater(t, val, float64(0))
	})

	t.Run("client_histogram_count", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`count(traces_service_graph_request_client_milliseconds_count{edge_relation="link"})`, 0.5)
		assert.Greater(t, val, float64(0))
	})

	t.Run("server_histogram_sum_positive", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`sum(traces_service_graph_request_server_milliseconds_sum{edge_relation="link"})`, 0.5)
		assert.Greater(t, val, float64(0), "server duration sum should be > 0")
	})
}

// --- Mixed middleware (NATS + MongoDB) ---
func TestScenarioF_MixedMiddleware(t *testing.T) {
	v := vmAPI(t)

	t.Run("nats_leg", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`traces_service_graph_request_total{client="order-service",server="payment-service",connection_type="nats",edge_relation="link"}`, 1)
		assert.GreaterOrEqual(t, val, float64(1))
	})

	t.Run("mongodb_leg", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`traces_service_graph_request_total{client="payment-service",server="sync-service",connection_type="mongodb",edge_relation="link"}`, 1)
		assert.GreaterOrEqual(t, val, float64(1))
	})
}

// --- Prefixed dimension labels ---
func TestPrefixedDimensions_LinkType(t *testing.T) {
	v := vmAPI(t)
	// link_type from link attrs should appear as client_link_type and server_link_type
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="order-service",server="payment-service",client_link_type="queue_enq_deq",server_link_type="queue_enq_deq"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

func TestPrefixedDimensions_MessagingSystem(t *testing.T) {
	v := vmAPI(t)
	// messaging.system from span attrs should appear as client_messaging_system and server_messaging_system
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{client="order-service",server="payment-service",client_messaging_system="nats",server_messaging_system=""}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

// --- Resource attribute dimension ---
func TestResourceAttributeDimension(t *testing.T) {
	v := vmAPI(t)

	t.Run("scenario_A_deployment_env", func(t *testing.T) {
		// order-service has deployment.environment=staging, payment-service has production
		val := queryScalarEventually(t, v,
			`traces_service_graph_request_total{client="order-service",server="payment-service",client_deployment_environment="staging",server_deployment_environment="production"}`, 1)
		assert.GreaterOrEqual(t, val, float64(1))
	})

	t.Run("scenario_E_self_ref_same_env", func(t *testing.T) {
		// order-service retry: both sides are staging
		val := queryScalarEventually(t, v,
			`traces_service_graph_request_total{client="order-service",server="order-service",client_deployment_environment="staging",server_deployment_environment="staging"}`, 1)
		assert.GreaterOrEqual(t, val, float64(1))
	})
}
