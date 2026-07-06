# Stage 1: Build full contrib collector with OCB + spanlinkservicegraph
FROM golang:1.25 AS builder

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.155.0

WORKDIR /src
COPY . .

WORKDIR /build
COPY integration-tests/kind/collector/builder-config.yaml .

RUN builder --config builder-config.yaml || true

RUN cd /build/otelcol-dev && \
    go mod edit -replace github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector=/src/connector/spanlinkservicegraphconnector && \
    go mod tidy && \
    CGO_ENABLED=0 go build -v -o otelcol-spanlink-contrib .

# Stage 2: Minimal runtime image (static binary on busybox)
FROM busybox:1.38
COPY --from=builder /build/otelcol-dev/otelcol-spanlink-contrib /otelcol
ENTRYPOINT ["/otelcol"]
