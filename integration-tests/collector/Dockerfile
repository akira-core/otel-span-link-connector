# Stage 1: Build custom collector with OCB (OCB v0.147 requires Go >= 1.25)
FROM golang:1.25 AS builder

# Install OpenTelemetry Collector Builder
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.147.0

# Copy full repo so connector source is available
WORKDIR /src
COPY . .

# Copy builder manifest and run OCB
WORKDIR /build
COPY integration-tests/collector/builder-config.yaml .

# OCB generates otelcol-dev then runs go mod tidy, which fails fetching our private connector.
# Generate only (allow tidy to fail), then add local replace and build.
RUN builder --config builder-config.yaml || true

# Replace connector with local source, then tidy and build
RUN cd /build/otelcol-dev && \
    go mod edit -replace github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector=/src/connector/spanlinkservicegraphconnector && \
    go mod tidy && \
    go build -o otelcol-spanlink .

# Stage 2: Minimal runtime image
FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=builder /build/otelcol-dev/otelcol-spanlink /otelcol-spanlink
COPY --from=builder /src/integration-tests/collector/otelcol-config.yaml /etc/otelcol/config.yaml
USER nonroot:nonroot
ENTRYPOINT ["/otelcol-spanlink", "--config=/etc/otelcol/config.yaml"]
