package verify

import (
	"context"
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
	}, 60*time.Second, 3*time.Second, "metric %s did not reach >= %v", promql, minVal)
	return val
}

func TestPhase1_ServiceGraphMetricsExist(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`count(traces_service_graph_request_total{edge_relation="link"})`, 1)
	assert.GreaterOrEqual(t, val, float64(1), "should have at least 1 service graph metric series")
}

func TestPhase1_K8sPodNameLabel(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`count(traces_service_graph_request_total{edge_relation="link",client_k8s_pod_name!=""})`, 1)
	assert.GreaterOrEqual(t, val, float64(1), "should have metrics with non-empty client_k8s_pod_name")
}

func TestPhase1_K8sNamespaceLabel(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`count(traces_service_graph_request_total{edge_relation="link",client_k8s_namespace_name="default"})`, 1)
	assert.GreaterOrEqual(t, val, float64(1), "should have metrics with client_k8s_namespace_name=default")
}

func TestPhase1_K8sJobNameLabel(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`count(traces_service_graph_request_total{edge_relation="link",client_k8s_job_name!=""})`, 1)
	assert.GreaterOrEqual(t, val, float64(1), "should have metrics with non-empty client_k8s_job_name")
}

func TestPhase1_SpanLinkPairExists(t *testing.T) {
	v := vmAPI(t)
	val := queryScalarEventually(t, v,
		`traces_service_graph_request_total{edge_relation="link",client_link_type="queue_enq_deq",server_link_type="queue_enq_deq"}`, 1)
	assert.GreaterOrEqual(t, val, float64(1))
}

func TestPhase1_Histograms(t *testing.T) {
	v := vmAPI(t)
	t.Run("server_histogram", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`count(traces_service_graph_request_server_milliseconds_count{edge_relation="link"})`, 0.5)
		assert.Greater(t, val, float64(0))
	})
	t.Run("client_histogram", func(t *testing.T) {
		val := queryScalarEventually(t, v,
			`count(traces_service_graph_request_client_milliseconds_count{edge_relation="link"})`, 0.5)
		assert.Greater(t, val, float64(0))
	})
}
