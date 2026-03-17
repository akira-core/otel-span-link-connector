// Integration test app: produces traces with span links for all test scenarios.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func main() {
	ctx := context.Background()

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	// OTLP trace exporter (shared by all tracer providers)
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		log.Fatalf("otlptracegrpc.New: %v", err)
	}
	// Tracer providers per service name so exported spans have correct resource service.name
	tracers := newTracers(exp)
	defer tracers.shutdown(ctx)
	otel.SetTracerProvider(tracers.defaultProvider())

	// 等待 collector OTLP 就緒後再送 trace
	time.Sleep(15 * time.Second)

	// Scenario A: NATS Queue (order-service -> payment-service)
	runScenarioA(ctx, natsURL, tracers)

	// Scenario B: MongoDB Change Stream (order-service -> sync-service)
	runScenarioB(ctx, mongoURI, tracers)

	// Scenario C: NATS Fan-out (event-source -> payment-svc, inventory-svc, notification-svc)
	runScenarioC(ctx, natsURL, tracers)

	// Scenario D: Cross-batch (consumer first, producer later)
	runScenarioD(ctx, tracers)

	// Scenario E: Retry (order-service -> order-service)
	runScenarioE(ctx, tracers)

	// Scenario F: Mixed (order-service -> payment-service -> sync-service via NATS then MongoDB)
	runScenarioF(ctx, natsURL, mongoURI, tracers)

	// Flush and allow export
	if err := tracers.forceFlush(ctx); err != nil {
		log.Printf("ForceFlush: %v", err)
	}
	time.Sleep(3 * time.Second)
	log.Println("testapp: all scenarios completed")
}

// tracers holds per-service TracerProviders so exported spans have correct resource service.name.
type tracers struct {
	providers []*sdktrace.TracerProvider
	m         map[string]trace.Tracer
}

func newTracers(exp sdktrace.SpanExporter) *tracers {
	type svcDef struct {
		name string
		env  string
	}
	services := []svcDef{
		{name: "order-service", env: "staging"},
		{name: "payment-service", env: "production"},
		{name: "sync-service", env: "production"},
		{name: "event-source", env: "staging"},
		{name: "payment-svc", env: "production"},
		{name: "inventory-svc", env: "production"},
		{name: "notification-svc", env: "production"},
		{name: "delayed-producer", env: "staging"},
		{name: "eager-consumer", env: "staging"},
	}
	t := &tracers{m: make(map[string]trace.Tracer)}
	for _, svc := range services {
		res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceNameKey.String(svc.name),
			attribute.String("deployment.environment", svc.env),
		))
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
		t.providers = append(t.providers, tp)
		t.m[svc.name] = tp.Tracer("testapp")
	}
	return t
}

func (t *tracers) tracer(serviceName string) trace.Tracer {
	return t.m[serviceName]
}

func (t *tracers) defaultProvider() *sdktrace.TracerProvider { return t.providers[0] }
func (t *tracers) shutdown(ctx context.Context) {
	for _, tp := range t.providers {
		_ = tp.Shutdown(ctx)
	}
}
func (t *tracers) forceFlush(ctx context.Context) error {
	for _, tp := range t.providers {
		if err := tp.ForceFlush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runScenarioA: producer (order-service) -> NATS -> consumer (payment-service), link_type=queue_enq_deq
func runScenarioA(ctx context.Context, natsURL string, tr *tracers) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Scenario A: NATS connect: %v", err)
		return
	}
	defer nc.Close()

	tracerProd := tr.tracer("order-service")
	tracerCons := tr.tracer("payment-service")
	// Producer span
	producerCtx, producerSpan := tracerProd.Start(ctx, "produce-order",
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", "orders"),
		),
	)
	producerSpan.End()
	producerSC := producerSpan.SpanContext()

	// Publish trace context in headers for consumer
	msg := nats.NewMsg("orders")
	msg.Data = []byte("order-1")
	msg.Header.Set("trace_id", producerSC.TraceID().String())
	msg.Header.Set("span_id", producerSC.SpanID().String())
	if err := nc.PublishMsg(msg); err != nil {
		log.Printf("Scenario A: Publish: %v", err)
		return
	}
	time.Sleep(200 * time.Millisecond)

	// Consumer span with link to producer
	link := trace.Link{
		SpanContext: producerSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "queue_enq_deq"),
		},
	}
	consumerCtx := context.Background()
	_, consumerSpan := tracerCons.Start(consumerCtx, "consume-order",
		trace.WithLinks(link),
	)
	time.Sleep(2 * time.Millisecond) // ensure server duration > 0 for histogram sum
	consumerSpan.End()

	_ = producerCtx
	time.Sleep(100 * time.Millisecond)
}

// runScenarioB: writer (order-service) -> MongoDB doc -> change stream listener (sync-service)
func runScenarioB(ctx context.Context, mongoURI string, tr *tracers) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Printf("Scenario B: mongo connect: %v", err)
		return
	}
	defer client.Disconnect(ctx)

	db := client.Database("testdb")
	coll := db.Collection("orders")
	// Ensure collection exists for change stream
	_, _ = coll.InsertOne(ctx, bson.M{"_init": true})

	tracerOrder := tr.tracer("order-service")
	tracerSync := tr.tracer("sync-service")
	writerCtx, writerSpan := tracerOrder.Start(ctx, "write-order",
		trace.WithAttributes(
			attribute.String("db.system", "mongodb"),
			attribute.String("db.namespace", "testdb.orders"),
		),
	)
	writerSC := writerSpan.SpanContext()
	writerSpan.End()

	doc := bson.M{
		"order_id":        "ord-1",
		"_otel_trace_id":  writerSC.TraceID().String(),
		"_otel_span_id":   writerSC.SpanID().String(),
		"created_at":      time.Now(),
	}
	if _, err := coll.InsertOne(ctx, doc); err != nil {
		log.Printf("Scenario B: InsertOne: %v", err)
		return
	}
	_ = writerCtx

	// Simulate change stream listener: we have trace id and span id from the doc we just wrote
	link := trace.Link{
		SpanContext: writerSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "change_stream"),
		},
	}
	_, listenerSpan := tracerSync.Start(ctx, "change-stream-handler",
		trace.WithAttributes(attribute.String("db.system", "mongodb")),
		trace.WithLinks(link),
	)
	time.Sleep(2 * time.Millisecond)
	listenerSpan.End()
	time.Sleep(100 * time.Millisecond)
}

// runScenarioC: one producer (event-source) -> NATS "events" -> 3 consumers
func runScenarioC(ctx context.Context, natsURL string, tr *tracers) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Scenario C: NATS connect: %v", err)
		return
	}
	defer nc.Close()

	tracerProd := tr.tracer("event-source")
	producerCtx, producerSpan := tracerProd.Start(ctx, "publish-event",
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", "events"),
		),
	)
	producerSC := producerSpan.SpanContext()
	producerSpan.End()

	msg := nats.NewMsg("events")
	msg.Data = []byte("event-1")
	msg.Header.Set("trace_id", producerSC.TraceID().String())
	msg.Header.Set("span_id", producerSC.SpanID().String())
	if err := nc.PublishMsg(msg); err != nil {
		log.Printf("Scenario C: Publish: %v", err)
		return
	}
	_ = producerCtx
	time.Sleep(200 * time.Millisecond)

	servers := []string{"payment-svc", "inventory-svc", "notification-svc"}
	for _, svc := range servers {
		tracerCons := tr.tracer(svc)
		link := trace.Link{
			SpanContext: producerSC,
			Attributes: []attribute.KeyValue{
				attribute.String("link_type", "queue_enq_deq"),
			},
		}
		_, consumerSpan := tracerCons.Start(context.Background(), "consume-event",
			trace.WithLinks(link),
		)
		time.Sleep(2 * time.Millisecond)
		consumerSpan.End()
	}
	time.Sleep(100 * time.Millisecond)
}

// runScenarioD: consumer (eager-consumer) with link to producer (delayed-producer), producer sent 2s later
func runScenarioD(ctx context.Context, tr *tracers) {
	tracerProd := tr.tracer("delayed-producer")
	tracerCons := tr.tracer("eager-consumer")
	// Create producer span context "in advance" so we can form the link (same process simulation:
	// we start producer span, get its context, then start consumer span with link, then end producer later)
	producerCtx, producerSpan := tracerProd.Start(ctx, "delayed-producer")
	producerSC := producerSpan.SpanContext()

	// Consumer span first (with link to producer)
	link := trace.Link{
		SpanContext: producerSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "queue_enq_deq"),
		},
	}
	_, consumerSpan := tracerCons.Start(context.Background(), "eager-consumer",
		trace.WithLinks(link),
	)
	time.Sleep(2 * time.Millisecond)
	consumerSpan.End()

	time.Sleep(2 * time.Second)
	producerSpan.End()
	_ = producerCtx
	time.Sleep(100 * time.Millisecond)
}

// runScenarioE: retry (order-service error span, then order-service retry span with link)
func runScenarioE(ctx context.Context, tr *tracers) {
	tracer := tr.tracer("order-service")
	_, failSpan := tracer.Start(ctx, "order-failed")
	failSpan.SetStatus(codes.Error, "failed")
	failSC := failSpan.SpanContext()
	failSpan.End()

	link := trace.Link{
		SpanContext: failSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "retry"),
		},
	}
	_, retrySpan := tracer.Start(ctx, "order-retry",
		trace.WithLinks(link),
	)
	time.Sleep(2 * time.Millisecond)
	retrySpan.End()
	time.Sleep(100 * time.Millisecond)
}

// runScenarioF: order-service -> NATS -> payment-service -> MongoDB -> sync-service
func runScenarioF(ctx context.Context, natsURL, mongoURI string, tr *tracers) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Scenario F: NATS connect: %v", err)
		return
	}
	defer nc.Close()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Printf("Scenario F: mongo connect: %v", err)
		return
	}
	defer client.Disconnect(ctx)
	db := client.Database("testdb")
	coll := db.Collection("events")

	tracerOrder := tr.tracer("order-service")
	tracerPayment := tr.tracer("payment-service")
	tracerSync := tr.tracer("sync-service")
	// Leg 1: order-service (producer) -> payment-service (consumer via NATS)
	orderCtx, orderSpan := tracerOrder.Start(ctx, "order-publish",
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", "orders"),
		),
	)
	orderSC := orderSpan.SpanContext()
	orderSpan.End()

	msg := nats.NewMsg("orders")
	msg.Data = []byte("order-f")
	msg.Header.Set("trace_id", orderSC.TraceID().String())
	msg.Header.Set("span_id", orderSC.SpanID().String())
	_ = nc.PublishMsg(msg)
	_ = orderCtx
	time.Sleep(150 * time.Millisecond)

	link1 := trace.Link{
		SpanContext: orderSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "queue_enq_deq"),
		},
	}
	_, paymentSpan := tracerPayment.Start(context.Background(), "payment-consume",
		trace.WithAttributes(attribute.String("messaging.system", "nats")),
		trace.WithLinks(link1),
	)
	time.Sleep(2 * time.Millisecond)
	paymentSpan.End()

	// Leg 2: payment-service (writer) -> sync-service (change stream listener)
	_, writeSpan := tracerPayment.Start(ctx, "payment-write",
		trace.WithAttributes(
			attribute.String("db.system", "mongodb"),
			attribute.String("db.namespace", "testdb.events"),
		),
	)
	writeSC := writeSpan.SpanContext()
	writeSpan.End()

	_, _ = coll.InsertOne(ctx, bson.M{
		"payload":         "sync-1",
		"_otel_trace_id":  writeSC.TraceID().String(),
		"_otel_span_id":   writeSC.SpanID().String(),
		"created_at":      time.Now(),
	})

	link2 := trace.Link{
		SpanContext: writeSC,
		Attributes: []attribute.KeyValue{
			attribute.String("link_type", "change_stream"),
		},
	}
	_, syncSpan := tracerSync.Start(ctx, "sync-handler",
		trace.WithAttributes(attribute.String("db.system", "mongodb")),
		trace.WithLinks(link2),
	)
	time.Sleep(2 * time.Millisecond)
	syncSpan.End()
	time.Sleep(100 * time.Millisecond)
}
