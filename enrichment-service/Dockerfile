# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/enrichment-service ./

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/enrichment-service /app/enrichment-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENV KAFKA_BROKERS=localhost:9092 \
    KAFKA_INPUT_TOPIC=raw-transactions \
    KAFKA_OUTPUT_TOPIC=enriched-transactions \
    KAFKA_GROUP_ID=enrichment-service \
    PROCESS_TIMEOUT_SECONDS=10

USER nonroot:nonroot

ENTRYPOINT ["/app/enrichment-service"]
