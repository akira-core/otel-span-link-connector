package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	ctx := context.Background()

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "testapp"
	}
	role := os.Getenv("ROLE") // "producer" or "consumer"
	linkedTraceID := os.Getenv("LINKED_TRACE_ID")
	linkedSpanID := os.Getenv("LINKED_SPAN_ID")

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "otel-collector.default.svc.cluster.local:4317"
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("failed to create exporter: %v", err)
	}

	// Use a single schema (semconv 1.26) to avoid Merge conflict with resource.Default() (schema 1.39).
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		attribute.String("deployment.environment", "kind-test"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	tracer := tp.Tracer("kind-e2e-testapp")

	switch role {
	case "producer":
		runProducer(ctx, tracer)
	case "consumer":
		runConsumer(ctx, tracer, linkedTraceID, linkedSpanID)
	default:
		runBothInProcess(ctx, tracer)
	}

	// Use a bounded context so flush/shutdown don't block forever if collector is unreachable.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer flushCancel()

	log.Println("flushing traces...")
	if err := tp.ForceFlush(flushCtx); err != nil {
		log.Printf("flush error: %v", err)
	}
	time.Sleep(2 * time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := tp.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("done")
}

func runProducer(ctx context.Context, tracer trace.Tracer) {
	ctx, span := tracer.Start(ctx, "produce-order",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", "orders"),
		),
	)
	span.AddEvent("order created")
	time.Sleep(50 * time.Millisecond)
	span.End()

	fmt.Printf("TRACE_ID=%s\n", span.SpanContext().TraceID().String())
	fmt.Printf("SPAN_ID=%s\n", span.SpanContext().SpanID().String())
}

func runConsumer(ctx context.Context, tracer trace.Tracer, linkedTraceIDHex, linkedSpanIDHex string) {
	if linkedTraceIDHex == "" || linkedSpanIDHex == "" {
		log.Println("LINKED_TRACE_ID and LINKED_SPAN_ID required for consumer role")
		return
	}

	linkedTraceID, err := trace.TraceIDFromHex(linkedTraceIDHex)
	if err != nil {
		log.Fatalf("invalid LINKED_TRACE_ID: %v", err)
	}
	linkedSpanID, err := trace.SpanIDFromHex(linkedSpanIDHex)
	if err != nil {
		log.Fatalf("invalid LINKED_SPAN_ID: %v", err)
	}

	linkSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: linkedTraceID,
		SpanID:  linkedSpanID,
		TraceFlags: trace.FlagsSampled,
	})

	_, span := tracer.Start(ctx, "consume-order",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.Link{
			SpanContext: linkSC,
			Attributes: []attribute.KeyValue{
				attribute.String("link_type", "queue_enq_deq"),
			},
		}),
	)
	span.AddEvent("order processed")
	time.Sleep(80 * time.Millisecond)
	span.End()
}

// runBothInProcess creates producer and consumer spans in one process with a span link.
// Useful for single-Job testing to guarantee both spans reach the collector.
func runBothInProcess(ctx context.Context, tracer trace.Tracer) {
	// Producer span (service A: order-service)
	_, producerSpan := tracer.Start(ctx, "produce-order",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", "orders"),
		),
	)
	time.Sleep(30 * time.Millisecond)
	producerSpan.End()

	producerSC := producerSpan.SpanContext()

	// Consumer span with link back to producer
	_, consumerSpan := tracer.Start(ctx, "consume-order",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.Link{
			SpanContext: producerSC,
			Attributes: []attribute.KeyValue{
				attribute.String("link_type", "queue_enq_deq"),
			},
		}),
	)
	time.Sleep(50 * time.Millisecond)
	consumerSpan.End()

	// Second pair: DB-style link
	_, writerSpan := tracer.Start(ctx, "write-document",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "mongodb"),
			attribute.String("db.namespace", "orders"),
		),
	)
	time.Sleep(20 * time.Millisecond)
	writerSpan.End()

	writerSC := writerSpan.SpanContext()

	_, readerSpan := tracer.Start(ctx, "change-stream-read",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.Link{
			SpanContext: writerSC,
			Attributes: []attribute.KeyValue{
				attribute.String("link_type", "change_stream"),
			},
		}),
	)
	time.Sleep(40 * time.Millisecond)
	readerSpan.End()

	log.Printf("produced 2 span-link pairs in-process")
}
