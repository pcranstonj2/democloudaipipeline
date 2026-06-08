package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultKafkaBrokers      = "localhost:9092"
	defaultInputTopic        = "raw-transactions"
	defaultOutputTopic       = "enriched-transactions"
	defaultConsumerGroup     = "enrichment-service"
	defaultProcessTimeoutSec = 10

	producerMaxAttempts = 10
	producerBackoffMin  = 100 * time.Millisecond
	producerBackoffMax  = 1 * time.Second
)

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

type geoMetadata struct {
	Country string `json:"country"`
	Region  string `json:"region"`
	City    string `json:"city"`
}

type deviceMetadata struct {
	Type      string `json:"type"`
	RiskLevel string `json:"risk_level"`
}

type velocityMetadata struct {
	Window1mCount int  `json:"window_1m_count"`
	Window5mCount int  `json:"window_5m_count"`
	HighVelocity  bool `json:"high_velocity"`
}

type enrichedTransactionEvent struct {
	TransactionID string           `json:"transaction_id"`
	CustomerID    string           `json:"customer_id"`
	Amount        float64          `json:"amount"`
	Currency      string           `json:"currency"`
	MerchantID    string           `json:"merchant_id"`
	EventTime     time.Time        `json:"event_time"`
	IngestedAt    time.Time        `json:"ingested_at"`
	EnrichedAt    time.Time        `json:"enriched_at"`
	TraceID       string           `json:"trace_id"`
	Geo           geoMetadata      `json:"geo"`
	Device        deviceMetadata   `json:"device"`
	Velocity      velocityMetadata `json:"velocity"`
}

type velocityTracker struct {
	mu        sync.Mutex
	eventByID map[string][]time.Time
}

func newVelocityTracker() *velocityTracker {
	return &velocityTracker{eventByID: make(map[string][]time.Time)}
}

func (v *velocityTracker) recordAndCompute(customerID string, ts time.Time) velocityMetadata {
	v.mu.Lock()
	defer v.mu.Unlock()

	cutoff5m := ts.Add(-5 * time.Minute)
	history := v.eventByID[customerID]
	filtered := history[:0]
	for _, t := range history {
		if !t.Before(cutoff5m) {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, ts)
	v.eventByID[customerID] = filtered

	cutoff1m := ts.Add(-1 * time.Minute)
	count1m := 0
	for _, t := range filtered {
		if !t.Before(cutoff1m) {
			count1m++
		}
	}

	count5m := len(filtered)
	return velocityMetadata{
		Window1mCount: count1m,
		Window5mCount: count5m,
		HighVelocity:  count1m >= 5 || count5m >= 15,
	}
}

func main() {
	brokers := parseCSV(getEnv("KAFKA_BROKERS", defaultKafkaBrokers))
	inputTopic := getEnv("KAFKA_INPUT_TOPIC", defaultInputTopic)
	outputTopic := getEnv("KAFKA_OUTPUT_TOPIC", defaultOutputTopic)
	groupID := getEnv("KAFKA_GROUP_ID", defaultConsumerGroup)
	processTimeout := time.Duration(getEnvInt("PROCESS_TIMEOUT_SECONDS", defaultProcessTimeoutSec)) * time.Second

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          inputTopic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("kafka reader close error: %v", err)
		}
	}()

	writer := &kafka.Writer{
		Addr:            kafka.TCP(brokers...),
		Topic:           outputTopic,
		Balancer:        &kafka.Hash{},
		RequiredAcks:    kafka.RequireOne,
		MaxAttempts:     producerMaxAttempts,
		WriteBackoffMin: producerBackoffMin,
		WriteBackoffMax: producerBackoffMax,
		Async:           false,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("kafka writer close error: %v", err)
		}
	}()

	tracker := newVelocityTracker()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("enrichment service started group=%s input_topic=%s output_topic=%s brokers=%s", groupID, inputTopic, outputTopic, strings.Join(brokers, ","))

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown signal received")
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("fetch loop canceled")
				return
			}
			log.Printf("fetch failed: %v", err)
			continue
		}

		if err := processMessage(ctx, writer, tracker, outputTopic, msg, processTimeout); err != nil {
			log.Printf("process failed topic=%s partition=%d offset=%d err=%v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed topic=%s partition=%d offset=%d err=%v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}
	}
}

func processMessage(parent context.Context, writer *kafka.Writer, tracker *velocityTracker, outputTopic string, msg kafka.Message, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	raw, err := decodeRawTransaction(msg.Value)
	if err != nil {
		return fmt.Errorf("decode raw payload: %w", err)
	}

	enriched := enrich(raw, tracker)
	payload, err := json.Marshal(enriched)
	if err != nil {
		return fmt.Errorf("marshal enriched payload: %w", err)
	}

	traceID := enriched.TraceID
	if strings.TrimSpace(traceID) == "" {
		traceID = "missing"
	}

	out := kafka.Message{
		Topic: outputTopic,
		Key:   []byte(enriched.TransactionID),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction.enriched")},
			{Key: "trace_id", Value: []byte(traceID)},
		},
	}

	if err := writer.WriteMessages(ctx, out); err != nil {
		return fmt.Errorf("publish enriched event: %w", err)
	}

	log.Printf("enriched transaction_id=%s customer_id=%s velocity_1m=%d velocity_5m=%d high_velocity=%t", enriched.TransactionID, enriched.CustomerID, enriched.Velocity.Window1mCount, enriched.Velocity.Window5mCount, enriched.Velocity.HighVelocity)
	return nil
}

func decodeRawTransaction(payload []byte) (rawTransactionEvent, error) {
	var raw rawTransactionEvent
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return rawTransactionEvent{}, err
	}

	if strings.TrimSpace(raw.TransactionID) == "" {
		return rawTransactionEvent{}, errors.New("transaction_id is required")
	}
	if strings.TrimSpace(raw.CustomerID) == "" {
		return rawTransactionEvent{}, errors.New("customer_id is required")
	}
	if strings.TrimSpace(raw.MerchantID) == "" {
		return rawTransactionEvent{}, errors.New("merchant_id is required")
	}
	if raw.Amount <= 0 {
		return rawTransactionEvent{}, errors.New("amount must be greater than 0")
	}
	if raw.EventTime.IsZero() {
		raw.EventTime = time.Now().UTC()
	}
	if raw.IngestedAt.IsZero() {
		raw.IngestedAt = time.Now().UTC()
	}

	return raw, nil
}

func enrich(raw rawTransactionEvent, tracker *velocityTracker) enrichedTransactionEvent {
	seed := raw.CustomerID + ":" + raw.MerchantID
	geo := deriveGeo(seed)
	device := deriveDevice(seed)
	velocity := tracker.recordAndCompute(raw.CustomerID, raw.EventTime.UTC())

	return enrichedTransactionEvent{
		TransactionID: raw.TransactionID,
		CustomerID:    raw.CustomerID,
		Amount:        raw.Amount,
		Currency:      strings.ToUpper(strings.TrimSpace(raw.Currency)),
		MerchantID:    raw.MerchantID,
		EventTime:     raw.EventTime.UTC(),
		IngestedAt:    raw.IngestedAt.UTC(),
		EnrichedAt:    time.Now().UTC(),
		TraceID:       raw.TraceID,
		Geo:           geo,
		Device:        device,
		Velocity:      velocity,
	}
}

func deriveGeo(seed string) geoMetadata {
	h := hashByte(seed)

	countries := []string{"GB", "US", "DE", "SG", "AE", "NL"}
	regions := []string{"London", "New York", "Frankfurt", "Singapore", "Dubai", "Amsterdam"}
	cities := []string{"London", "New York", "Berlin", "Singapore", "Dubai", "Rotterdam"}

	i := int(h % byte(len(countries)))
	return geoMetadata{
		Country: countries[i],
		Region:  regions[i],
		City:    cities[i],
	}
}

func deriveDevice(seed string) deviceMetadata {
	h := hashByte("device:" + seed)
	types := []string{"mobile", "desktop", "tablet"}
	risk := []string{"low", "medium", "high"}

	return deviceMetadata{
		Type:      types[int(h%byte(len(types)))],
		RiskLevel: risk[int((h/3)%byte(len(risk)))],
	}
}

func hashByte(s string) byte {
	sum := sha1.Sum([]byte(s))
	return sum[0]
}

func parseCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{defaultKafkaBrokers}
	}
	return out
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}
