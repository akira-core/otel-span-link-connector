FROM golang:1.24
WORKDIR /verify
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy
ENTRYPOINT ["go", "test", "-v", "-count=1", "-timeout=60s", "./..."]
