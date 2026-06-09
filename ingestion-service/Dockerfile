# syntax=docker/dockerfile:1.7

FROM golang:1.22-alpine AS builder

WORKDIR /src

# Install CA certs and tzdata so the final image can copy them if needed.
RUN apk add --no-cache ca-certificates tzdata

# Download dependencies first for better Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Build a static Linux binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/ingestion-service ./

# Minimal, non-root runtime image.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

# Copy binary and CA bundle.
COPY --from=builder /out/ingestion-service /app/ingestion-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Service defaults (can be overridden at runtime).
ENV PORT=8080 \
    KAFKA_BROKERS=localhost:9092 \
    KAFKA_TOPIC=raw-transactions

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/app/ingestion-service"]
