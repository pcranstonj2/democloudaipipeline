# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/scoring-orchestrator ./

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/scoring-orchestrator /app/scoring-orchestrator
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENV KAFKA_BROKERS=localhost:9092 \
    KAFKA_INPUT_TOPIC=enriched-transactions \
    KAFKA_OUTPUT_TOPIC=scored-transactions \
    KAFKA_GROUP_ID=scoring-orchestrator \
    MODEL_SERVICE_URL=http://model-service:8010/predict \
    WORKER_COUNT=8 \
    JOB_QUEUE_SIZE=1024 \
    PROCESS_TIMEOUT_SECONDS=15 \
    HTTP_TIMEOUT_SECONDS=3 \
    RETRY_MAX_ATTEMPTS=5 \
    RETRY_BACKOFF_MIN_MS=100 \
    RETRY_BACKOFF_MAX_MS=2000

USER nonroot:nonroot

ENTRYPOINT ["/app/scoring-orchestrator"]
