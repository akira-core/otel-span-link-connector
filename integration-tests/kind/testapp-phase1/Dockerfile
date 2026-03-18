FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o testapp .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/testapp /testapp
ENTRYPOINT ["/testapp"]
