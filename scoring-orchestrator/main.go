package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultKafkaBrokers  = "localhost:9092"
	defaultInputTopic    = "enriched-transactions"
	defaultOutputTopic   = "scored-transactions"
	defaultConsumerGroup = "scoring-orchestrator"

	defaultModelServiceURL = "http://127.0.0.1:8010/predict"

	defaultWorkerCount         = 8
	defaultJobQueueSize        = 1024
	defaultProcessTimeoutSec   = 15
	defaultHTTPTimeoutSec      = 3
	defaultMaxRetryAttempts    = 5
	defaultRetryBackoffMinMS   = 100
	defaultRetryBackoffMaxMS   = 2000
	defaultReaderCommitSeconds = 1
)

type config struct {
	brokers []string

	inputTopic  string
	outputTopic string
	groupID     string

	modelServiceURL string

	workerCount int
	jobQueue    int

	processTimeout time.Duration
	httpTimeout    time.Duration

	maxRetryAttempts int
	backoffMin       time.Duration
	backoffMax       time.Duration
}

type enrichedTransactionEvent struct {
	TransactionID string    `json:"transaction_id"`
	CustomerID    string    `json:"customer_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	MerchantID    string    `json:"merchant_id"`
	EventTime     time.Time `json:"event_time"`
	IngestedAt    time.Time `json:"ingested_at"`
	EnrichedAt    time.Time `json:"enriched_at"`
	TraceID       string    `json:"trace_id"`
	Geo           struct {
		Country string `json:"country"`
		Region  string `json:"region"`
		City    string `json:"city"`
	} `json:"geo"`
	Device struct {
		Type      string `json:"type"`
		RiskLevel string `json:"risk_level"`
	} `json:"device"`
	Velocity struct {
		Window1mCount int  `json:"window_1m_count"`
		Window5mCount int  `json:"window_5m_count"`
		HighVelocity  bool `json:"high_velocity"`
	} `json:"velocity"`
}

type predictRequest struct {
	Amount          float64 `json:"amount"`
	Velocity1m      int     `json:"velocity_1m"`
	Velocity5m      int     `json:"velocity_5m"`
	DeviceRiskScore float64 `json:"device_risk_score"`
	GeoRiskScore    float64 `json:"geo_risk_score"`
}

type predictResponse struct {
	RiskScore    float64 `json:"risk_score"`
	IsAnomaly    bool    `json:"is_anomaly"`
	ModelVersion string  `json:"model_version"`
}

type scoredTransactionEvent struct {
	TransactionID string    `json:"transaction_id"`
	CustomerID    string    `json:"customer_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	MerchantID    string    `json:"merchant_id"`
	EventTime     time.Time `json:"event_time"`
	IngestedAt    time.Time `json:"ingested_at"`
	EnrichedAt    time.Time `json:"enriched_at"`
	ScoredAt      time.Time `json:"scored_at"`
	TraceID       string    `json:"trace_id"`
	RiskScore     float64   `json:"risk_score"`
	IsAnomaly     bool      `json:"is_anomaly"`
	ModelVersion  string    `json:"model_version"`
	Decision      string    `json:"decision"`
}

type job struct {
	message kafka.Message
}

func main() {
	cfg := loadConfig()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.brokers,
		Topic:          cfg.inputTopic,
		GroupID:        cfg.groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: defaultReaderCommitSeconds * time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("kafka reader close error: %v", err)
		}
	}()

	writer := &kafka.Writer{
		Addr:            kafka.TCP(cfg.brokers...),
		Topic:           cfg.outputTopic,
		Balancer:        &kafka.Hash{},
		RequiredAcks:    kafka.RequireOne,
		MaxAttempts:     cfg.maxRetryAttempts,
		WriteBackoffMin: cfg.backoffMin,
		WriteBackoffMax: cfg.backoffMax,
		Async:           false,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("kafka writer close error: %v", err)
		}
	}()

	httpClient := &http.Client{Timeout: cfg.httpTimeout}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	jobs := make(chan job, cfg.jobQueue)
	workerDone := make(chan struct{}, cfg.workerCount)

	for i := 0; i < cfg.workerCount; i++ {
		go worker(ctx, i+1, cfg, reader, writer, httpClient, jobs, workerDone)
	}

	log.Printf("scoring orchestrator started group=%s input=%s output=%s workers=%d model_url=%s", cfg.groupID, cfg.inputTopic, cfg.outputTopic, cfg.workerCount, cfg.modelServiceURL)

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			waitForWorkers(workerDone, cfg.workerCount)
			log.Printf("shutdown complete")
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				close(jobs)
				waitForWorkers(workerDone, cfg.workerCount)
				log.Printf("fetch canceled")
				return
			}
			log.Printf("fetch failed: %v", err)
			continue
		}

		select {
		case jobs <- job{message: msg}:
		case <-ctx.Done():
			close(jobs)
			waitForWorkers(workerDone, cfg.workerCount)
			return
		}
	}
}

func worker(
	ctx context.Context,
	id int,
	cfg config,
	reader *kafka.Reader,
	writer *kafka.Writer,
	httpClient *http.Client,
	jobs <-chan job,
	done chan<- struct{},
) {
	defer func() { done <- struct{}{} }()

	for j := range jobs {
		processCtx, cancel := context.WithTimeout(ctx, cfg.processTimeout)
		err := processOne(processCtx, cfg, writer, httpClient, j.message)
		cancel()

		if err != nil {
			log.Printf("worker=%d process failed partition=%d offset=%d err=%v", id, j.message.Partition, j.message.Offset, err)
			continue
		}

		if err := reader.CommitMessages(ctx, j.message); err != nil {
			log.Printf("worker=%d commit failed partition=%d offset=%d err=%v", id, j.message.Partition, j.message.Offset, err)
			continue
		}
	}
}

func processOne(ctx context.Context, cfg config, writer *kafka.Writer, httpClient *http.Client, msg kafka.Message) error {
	enriched, err := decodeEnriched(msg.Value)
	if err != nil {
		return fmt.Errorf("decode enriched event: %w", err)
	}

	predictReq := predictRequest{
		Amount:          enriched.Amount,
		Velocity1m:      enriched.Velocity.Window1mCount,
		Velocity5m:      enriched.Velocity.Window5mCount,
		DeviceRiskScore: deviceRiskScore(enriched.Device.RiskLevel),
		GeoRiskScore:    geoRiskScore(enriched.Geo.Country),
	}

	var predictRes predictResponse
	err = retryWithExponentialBackoff(ctx, cfg.maxRetryAttempts, cfg.backoffMin, cfg.backoffMax, func(attempt int) error {
		res, predictErr := callModelPredict(ctx, httpClient, cfg.modelServiceURL, predictReq)
		if predictErr != nil {
			return predictErr
		}
		predictRes = res
		return nil
	})
	if err != nil {
		return fmt.Errorf("model-service predict failed after retries: %w", err)
	}

	scored := scoredTransactionEvent{
		TransactionID: enriched.TransactionID,
		CustomerID:    enriched.CustomerID,
		Amount:        enriched.Amount,
		Currency:      enriched.Currency,
		MerchantID:    enriched.MerchantID,
		EventTime:     enriched.EventTime,
		IngestedAt:    enriched.IngestedAt,
		EnrichedAt:    enriched.EnrichedAt,
		ScoredAt:      time.Now().UTC(),
		TraceID:       enriched.TraceID,
		RiskScore:     predictRes.RiskScore,
		IsAnomaly:     predictRes.IsAnomaly,
		ModelVersion:  predictRes.ModelVersion,
		Decision:      decisionFromRisk(predictRes.RiskScore),
	}

	payload, err := json.Marshal(scored)
	if err != nil {
		return fmt.Errorf("marshal scored event: %w", err)
	}

	outMsg := kafka.Message{
		Topic: cfg.outputTopic,
		Key:   []byte(scored.TransactionID),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction.scored")},
			{Key: "trace_id", Value: []byte(defaultString(scored.TraceID, "missing"))},
		},
	}

	err = retryWithExponentialBackoff(ctx, cfg.maxRetryAttempts, cfg.backoffMin, cfg.backoffMax, func(attempt int) error {
		return writer.WriteMessages(ctx, outMsg)
	})
	if err != nil {
		return fmt.Errorf("publish scored event failed after retries: %w", err)
	}

	log.Printf("scored transaction_id=%s risk_score=%.4f anomaly=%t model=%s", scored.TransactionID, scored.RiskScore, scored.IsAnomaly, scored.ModelVersion)
	return nil
}

func retryWithExponentialBackoff(ctx context.Context, maxAttempts int, minBackoff, maxBackoff time.Duration, fn func(attempt int) error) error {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if minBackoff <= 0 {
		minBackoff = 100 * time.Millisecond
	}
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}

	var lastErr error
	backoff := minBackoff

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}

		log.Printf("retry attempt=%d/%d err=%v", attempt, maxAttempts, err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return lastErr
}

func callModelPredict(ctx context.Context, client *http.Client, url string, reqBody predictRequest) (predictResponse, error) {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return predictResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return predictResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return predictResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return predictResponse{}, fmt.Errorf("model-service status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out predictResponse
	dec := json.NewDecoder(resp.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return predictResponse{}, err
	}

	return out, nil
}

func decodeEnriched(payload []byte) (enrichedTransactionEvent, error) {
	var e enrichedTransactionEvent
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return enrichedTransactionEvent{}, err
	}

	if strings.TrimSpace(e.TransactionID) == "" {
		return enrichedTransactionEvent{}, errors.New("transaction_id is required")
	}
	if strings.TrimSpace(e.CustomerID) == "" {
		return enrichedTransactionEvent{}, errors.New("customer_id is required")
	}
	if e.Amount <= 0 {
		return enrichedTransactionEvent{}, errors.New("amount must be greater than 0")
	}

	return e, nil
}

func decisionFromRisk(score float64) string {
	if score >= 0.85 {
		return "deny"
	}
	if score >= 0.60 {
		return "review"
	}
	return "allow"
}

func deviceRiskScore(level string) float64 {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "high":
		return 0.9
	case "medium":
		return 0.55
	case "low":
		fallthrough
	default:
		return 0.2
	}
}

func geoRiskScore(country string) float64 {
	switch strings.ToUpper(strings.TrimSpace(country)) {
	case "AE", "SG":
		return 0.35
	case "GB", "US", "DE", "NL":
		return 0.2
	default:
		return 0.5
	}
}

func parseCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return []string{defaultKafkaBrokers}
	}
	return out
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
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
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func loadConfig() config {
	maxRetry := getEnvInt("RETRY_MAX_ATTEMPTS", defaultMaxRetryAttempts)
	backoffMin := time.Duration(getEnvInt("RETRY_BACKOFF_MIN_MS", defaultRetryBackoffMinMS)) * time.Millisecond
	backoffMax := time.Duration(getEnvInt("RETRY_BACKOFF_MAX_MS", defaultRetryBackoffMaxMS)) * time.Millisecond

	return config{
		brokers: parseCSV(getEnv("KAFKA_BROKERS", defaultKafkaBrokers)),

		inputTopic:  getEnv("KAFKA_INPUT_TOPIC", defaultInputTopic),
		outputTopic: getEnv("KAFKA_OUTPUT_TOPIC", defaultOutputTopic),
		groupID:     getEnv("KAFKA_GROUP_ID", defaultConsumerGroup),

		modelServiceURL: getEnv("MODEL_SERVICE_URL", defaultModelServiceURL),

		workerCount: getEnvInt("WORKER_COUNT", defaultWorkerCount),
		jobQueue:    getEnvInt("JOB_QUEUE_SIZE", defaultJobQueueSize),

		processTimeout: time.Duration(getEnvInt("PROCESS_TIMEOUT_SECONDS", defaultProcessTimeoutSec)) * time.Second,
		httpTimeout:    time.Duration(getEnvInt("HTTP_TIMEOUT_SECONDS", defaultHTTPTimeoutSec)) * time.Second,

		maxRetryAttempts: maxRetry,
		backoffMin:       backoffMin,
		backoffMax:       backoffMax,
	}
}

func waitForWorkers(done <-chan struct{}, count int) {
	for i := 0; i < count; i++ {
		<-done
	}
}
