package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	defaultKafkaBrokers = "localhost:9092"
	defaultKafkaTopic   = "raw-transactions"
	defaultHTTPPort     = "8080"
	kafkaMaxAttempts    = 10
	kafkaBackoffMin     = 100 * time.Millisecond
	kafkaBackoffMax     = 1 * time.Second
	maxBodyBytes        = 1 << 20
)

type transactionRequest struct {
	TransactionID string  `json:"transaction_id"`
	CustomerID    string  `json:"customer_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	MerchantID    string  `json:"merchant_id"`
	Timestamp     string  `json:"timestamp"`
}

type rawTransactionEvent struct {
	TransactionID string    `json:"transaction_id"`
	CustomerID    string    `json:"customer_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	MerchantID    string    `json:"merchant_id"`
	EventTime     time.Time `json:"event_time"`
	IngestedAt    time.Time `json:"ingested_at"`
	TraceID       string    `json:"trace_id"`
}

type server struct {
	writer *kafka.Writer
	topic  string
}

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", defaultKafkaBrokers)
	kafkaTopic := getEnv("KAFKA_TOPIC", defaultKafkaTopic)
	port := getEnv("PORT", defaultHTTPPort)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(kafkaBrokers, ",")...),
		Topic:        kafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  kafkaMaxAttempts,
		WriteBackoffMin: kafkaBackoffMin,
		WriteBackoffMax: kafkaBackoffMax,
		Async:        false,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("kafka writer close error: %v", err)
		}
	}()

	s := &server{writer: writer, topic: kafkaTopic}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /transaction", s.handleTransaction)
	mux.HandleFunc("/healthz", handleHealthz)

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           withJSONContentType(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("ingestion service listening on :%s, topic=%s", port, kafkaTopic)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func (s *server) handleTransaction(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var req transactionRequest
	if err := decodeAndValidateRequest(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	eventTime, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		writeError(w, http.StatusBadRequest, "timestamp must be RFC3339 format")
		return
	}

	event := rawTransactionEvent{
		TransactionID: req.TransactionID,
		CustomerID:    req.CustomerID,
		Amount:        req.Amount,
		Currency:      strings.ToUpper(req.Currency),
		MerchantID:    req.MerchantID,
		EventTime:     eventTime.UTC(),
		IngestedAt:    time.Now().UTC(),
		TraceID:       requestTraceID(r.Header.Get("X-Trace-ID")),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to serialize event")
		return
	}

	msg := kafka.Message{
		Key:   []byte(event.TransactionID),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction.raw")},
			{Key: "trace_id", Value: []byte(event.TraceID)},
		},
	}

	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("kafka publish failed, transaction_id=%s err=%v", event.TransactionID, err)
		writeError(w, http.StatusServiceUnavailable, "failed to publish transaction")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":         "accepted",
		"topic":          s.topic,
		"transaction_id": event.TransactionID,
		"trace_id":       event.TraceID,
	})
}

func decodeAndValidateRequest(r *http.Request, out *transactionRequest) error {
	if r.Header.Get("Content-Type") != "" && !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		return errors.New("content-type must be application/json")
	}

	limitedReader := io.LimitReader(r.Body, maxBodyBytes)
	dec := json.NewDecoder(limitedReader)
	dec.DisallowUnknownFields()

	if err := dec.Decode(out); err != nil {
		return errors.New("invalid JSON payload")
	}

	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return errors.New("invalid JSON payload")
	}

	if strings.TrimSpace(out.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}
	if strings.TrimSpace(out.CustomerID) == "" {
		return errors.New("customer_id is required")
	}
	if strings.TrimSpace(out.MerchantID) == "" {
		return errors.New("merchant_id is required")
	}
	if strings.TrimSpace(out.Currency) == "" {
		return errors.New("currency is required")
	}
	if len(strings.TrimSpace(out.Currency)) != 3 {
		return errors.New("currency must be 3-letter ISO code")
	}
	if out.Amount <= 0 {
		return errors.New("amount must be greater than 0")
	}
	if strings.TrimSpace(out.Timestamp) == "" {
		return errors.New("timestamp is required")
	}

	return nil
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func withJSONContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("response encode error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func requestTraceID(existing string) string {
	if strings.TrimSpace(existing) != "" {
		return existing
	}
	return uuid.NewString()
}
