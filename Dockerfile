# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Cache dependencies separately — only re-downloads when go.mod changes
COPY go.mod ./
RUN go mod download 2>/dev/null || true

# Copy source and tidy (generates go.sum, fetches anything missing)
COPY . .
RUN go mod tidy

# Build with all available CPUs
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /telssh .

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

RUN adduser -D -u 1000 botuser

WORKDIR /app

# Create writable data directory owned by botuser
RUN mkdir -p /app/data && chown -R botuser:botuser /app /app/data

COPY --from=builder /telssh .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh && chown botuser:botuser telssh entrypoint.sh

USER botuser

ENTRYPOINT ["./entrypoint.sh"]
CMD ["-config", "/app/data/config.yaml"]
