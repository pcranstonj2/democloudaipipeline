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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

const (
	defaultKafkaBrokers  = "localhost:9092"
	defaultInputTopic    = "enriched-transactions"
	defaultOutputTopic   = "scored-transactions"
	defaultConsumerGroup = "drift-monitor-scorer"

	defaultModelServiceURL = "http://127.0.0.1:8010/predict"

	defaultWorkerCount       = 8
	defaultQueueSize         = 1024
	defaultProcessTimeoutSec = 15
	defaultHTTPTimeoutSec    = 3

	defaultRetryAttempts   = 5
	defaultBackoffMinMS    = 100
	defaultBackoffMaxMS    = 2000
	defaultCommitIntervalS = 1
	defaultMetricsPort     = "9093"
)

var (
	driftMessagesConsumedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "drift_monitor_messages_consumed_total",
		Help: "Total messages consumed by drift-monitor.",
	})
	driftMessagesPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "drift_monitor_messages_published_total",
		Help: "Total scored messages published by drift-monitor by decision.",
	}, []string{"decision"})
	driftProcessingErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "drift_monitor_processing_errors_total",
		Help: "Total processing errors in drift-monitor.",
	})
	driftCommitErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "drift_monitor_kafka_commit_errors_total",
		Help: "Total Kafka commit errors in drift-monitor.",
	})
	driftPublishFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "drift_monitor_kafka_publish_failures_total",
		Help: "Total failed Kafka publish attempts in drift-monitor.",
	})
	driftPredictFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "drift_monitor_model_predict_failures_total",
		Help: "Total failed model-service /predict attempts in drift-monitor.",
	})
	driftProcessingDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "drift_monitor_processing_duration_seconds",
		Help:    "Duration of drift-monitor processing per message.",
		Buckets: prometheus.DefBuckets,
	})
	driftPredictDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "drift_monitor_model_predict_duration_seconds",
		Help:    "Duration of model-service /predict calls from drift-monitor.",
		Buckets: prometheus.DefBuckets,
	})
)

type config struct {
	brokers []string

	inputTopic  string
	outputTopic string
	groupID     string

	modelServiceURL string

	workerCount int
	queueSize   int

	processTimeout time.Duration
	httpTimeout    time.Duration

	maxAttempts int
	backoffMin  time.Duration
	backoffMax  time.Duration
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
	msg kafka.Message
}

func main() {
	cfg := loadConfig()
	metricsPort := getEnv("METRICS_PORT", defaultMetricsPort)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.brokers,
		Topic:          cfg.inputTopic,
		GroupID:        cfg.groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: defaultCommitIntervalS * time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("reader close error: %v", err)
		}
	}()

	writer := &kafka.Writer{
		// Keep topic on the writer only; setting it on both writer and message causes a runtime error.
		Addr:            kafka.TCP(cfg.brokers...),
		Topic:           cfg.outputTopic,
		Balancer:        &kafka.Hash{},
		RequiredAcks:    kafka.RequireOne,
		MaxAttempts:     cfg.maxAttempts,
		WriteBackoffMin: cfg.backoffMin,
		WriteBackoffMax: cfg.backoffMax,
		Async:           false,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("writer close error: %v", err)
		}
	}()

	httpClient := &http.Client{Timeout: cfg.httpTimeout}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go startMetricsServer(ctx, metricsPort)

	jobs := make(chan job, cfg.queueSize)
	done := make(chan struct{}, cfg.workerCount)

	for i := 0; i < cfg.workerCount; i++ {
		go worker(ctx, i+1, cfg, reader, writer, httpClient, jobs, done)
	}

	log.Printf("scorer started input=%s output=%s group=%s workers=%d", cfg.inputTopic, cfg.outputTopic, cfg.groupID, cfg.workerCount)

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			waitWorkers(done, cfg.workerCount)
			return
		default:
		}

		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				close(jobs)
				waitWorkers(done, cfg.workerCount)
				return
			}
			log.Printf("fetch failed: %v", err)
			continue
		}
		driftMessagesConsumedTotal.Inc()

		select {
		case jobs <- job{msg: m}:
		case <-ctx.Done():
			close(jobs)
			waitWorkers(done, cfg.workerCount)
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
		opCtx, cancel := context.WithTimeout(ctx, cfg.processTimeout)
		err := processMessage(opCtx, cfg, writer, httpClient, j.msg)
		cancel()

		if err != nil {
			driftProcessingErrorsTotal.Inc()
			log.Printf("worker=%d process failed partition=%d offset=%d err=%v", id, j.msg.Partition, j.msg.Offset, err)
			continue
		}

		if err := reader.CommitMessages(ctx, j.msg); err != nil {
			driftCommitErrorsTotal.Inc()
			log.Printf("worker=%d commit failed partition=%d offset=%d err=%v", id, j.msg.Partition, j.msg.Offset, err)
		}
	}
}

func processMessage(ctx context.Context, cfg config, writer *kafka.Writer, httpClient *http.Client, msg kafka.Message) error {
	start := time.Now()
	defer func() {
		driftProcessingDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	enriched, err := decodeEnriched(msg.Value)
	if err != nil {
		return fmt.Errorf("decode enriched: %w", err)
	}

	request := predictRequest{
		Amount:          enriched.Amount,
		Velocity1m:      enriched.Velocity.Window1mCount,
		Velocity5m:      enriched.Velocity.Window5mCount,
		DeviceRiskScore: deviceRiskScore(enriched.Device.RiskLevel),
		GeoRiskScore:    geoRiskScore(enriched.Geo.Country),
	}

	var prediction predictResponse
	err = retryExponential(ctx, cfg.maxAttempts, cfg.backoffMin, cfg.backoffMax, func(attempt int) error {
		predictStart := time.Now()
		res, callErr := callPredict(ctx, httpClient, cfg.modelServiceURL, request)
		driftPredictDurationSeconds.Observe(time.Since(predictStart).Seconds())
		if callErr != nil {
			driftPredictFailuresTotal.Inc()
			return callErr
		}
		prediction = res
		return nil
	})
	if err != nil {
		return fmt.Errorf("predict failed after retries: %w", err)
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
		RiskScore:     prediction.RiskScore,
		IsAnomaly:     prediction.IsAnomaly,
		ModelVersion:  prediction.ModelVersion,
		Decision:      decision(prediction.RiskScore),
	}

	body, err := json.Marshal(scored)
	if err != nil {
		return fmt.Errorf("marshal scored: %w", err)
	}

	// Do not set Topic here because writer.Topic is already configured.
	out := kafka.Message{
		Key:   []byte(scored.TransactionID),
		Value: body,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction.scored")},
			{Key: "trace_id", Value: []byte(withDefault(scored.TraceID, "missing"))},
		},
	}

	err = retryExponential(ctx, cfg.maxAttempts, cfg.backoffMin, cfg.backoffMax, func(attempt int) error {
		publishErr := writer.WriteMessages(ctx, out)
		if publishErr != nil {
			driftPublishFailuresTotal.Inc()
		}
		return publishErr
	})
	if err != nil {
		return fmt.Errorf("publish failed after retries: %w", err)
	}

	driftMessagesPublishedTotal.WithLabelValues(scored.Decision).Inc()
	log.Printf("scored transaction_id=%s risk_score=%.4f anomaly=%t", scored.TransactionID, scored.RiskScore, scored.IsAnomaly)
	return nil
}

func retryExponential(ctx context.Context, maxAttempts int, backoffMin, backoffMax time.Duration, fn func(attempt int) error) error {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if backoffMin <= 0 {
		backoffMin = 100 * time.Millisecond
	}
	if backoffMax < backoffMin {
		backoffMax = backoffMin
	}

	var lastErr error
	backoff := backoffMin

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
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}

	return lastErr
}

func callPredict(ctx context.Context, client *http.Client, url string, req predictRequest) (predictResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return predictResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return predictResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		return predictResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return predictResponse{}, fmt.Errorf("model-service status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out predictResponse
	decoder := json.NewDecoder(resp.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return predictResponse{}, err
	}

	return out, nil
}

func decodeEnriched(payload []byte) (enrichedTransactionEvent, error) {
	var out enrichedTransactionEvent
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return enrichedTransactionEvent{}, err
	}

	if strings.TrimSpace(out.TransactionID) == "" {
		return enrichedTransactionEvent{}, errors.New("transaction_id is required")
	}
	if strings.TrimSpace(out.CustomerID) == "" {
		return enrichedTransactionEvent{}, errors.New("customer_id is required")
	}
	if out.Amount <= 0 {
		return enrichedTransactionEvent{}, errors.New("amount must be greater than 0")
	}

	return out, nil
}

func decision(risk float64) string {
	if risk >= 0.85 {
		return "deny"
	}
	if risk >= 0.60 {
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

func withDefault(v, fallback string) string {
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
	return config{
		brokers: parseCSV(getEnv("KAFKA_BROKERS", defaultKafkaBrokers)),

		inputTopic:  getEnv("KAFKA_INPUT_TOPIC", defaultInputTopic),
		outputTopic: getEnv("KAFKA_OUTPUT_TOPIC", defaultOutputTopic),
		groupID:     getEnv("KAFKA_GROUP_ID", defaultConsumerGroup),

		modelServiceURL: getEnv("MODEL_SERVICE_URL", defaultModelServiceURL),

		workerCount: getEnvInt("WORKER_COUNT", defaultWorkerCount),
		queueSize:   getEnvInt("JOB_QUEUE_SIZE", defaultQueueSize),

		processTimeout: time.Duration(getEnvInt("PROCESS_TIMEOUT_SECONDS", defaultProcessTimeoutSec)) * time.Second,
		httpTimeout:    time.Duration(getEnvInt("HTTP_TIMEOUT_SECONDS", defaultHTTPTimeoutSec)) * time.Second,

		maxAttempts: getEnvInt("RETRY_MAX_ATTEMPTS", defaultRetryAttempts),
		backoffMin:  time.Duration(getEnvInt("RETRY_BACKOFF_MIN_MS", defaultBackoffMinMS)) * time.Millisecond,
		backoffMax:  time.Duration(getEnvInt("RETRY_BACKOFF_MAX_MS", defaultBackoffMaxMS)) * time.Millisecond,
	}
}

func waitWorkers(done <-chan struct{}, count int) {
	for i := 0; i < count; i++ {
		<-done
	}
}

func startMetricsServer(ctx context.Context, port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server failed: %v", err)
	}
}
