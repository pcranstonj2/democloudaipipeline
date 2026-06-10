package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type responseBody map[string]any

type inputTx struct {
	TransactionID string `json:"transaction_id"`
}

func main() {
	uri := flag.String("uri", "http://localhost:8080/transaction", "HTTP endpoint to POST each transaction line")
	inputFile := flag.String("input-file", filepath.Join("data", "test-transactions.txt"), "Path to newline-delimited JSON input file")
	delayMs := flag.Int("delay-ms", 0, "Delay in milliseconds between requests")
	stopOnError := flag.Bool("stop-on-error", false, "Stop immediately on first send error")
	quiet := flag.Bool("quiet", false, "Suppress per-transaction output")
	progressEvery := flag.Int("progress-every", 100, "Print progress every N successful sends when quiet mode is enabled; use 0 to disable")
	showResponse := flag.Bool("show-response", false, "Print response JSON for each successful send")
	timeoutMs := flag.Int("timeout-ms", 5000, "Per-request timeout in milliseconds")
	flag.Parse()

	if err := run(*uri, *inputFile, *delayMs, *stopOnError, *quiet, *progressEvery, *showResponse, *timeoutMs); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(uri, inputFile string, delayMs int, stopOnError, quiet bool, progressEvery int, showResponse bool, timeoutMs int) error {
	if strings.TrimSpace(uri) == "" {
		return fmt.Errorf("uri is required")
	}
	if strings.TrimSpace(inputFile) == "" {
		return fmt.Errorf("input-file is required")
	}
	if delayMs < 0 {
		return fmt.Errorf("delay-ms must be >= 0")
	}
	if progressEvery < 0 {
		return fmt.Errorf("progress-every must be >= 0")
	}
	if timeoutMs <= 0 {
		return fmt.Errorf("timeout-ms must be > 0")
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()

	client := &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")

	sent := 0
	failed := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		respText, err := postLine(client, uri, headers, line)
		if err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "warning: failed to send line: %s\n", line)
			fmt.Fprintf(os.Stderr, "warning: error: %v\n", err)
			if stopOnError {
				return err
			}
		} else {
			sent++
			if !quiet {
				txID := extractTransactionID(line)
				fmt.Printf("[%d] Sent transaction: %s\n", sent, txID)
				if showResponse && respText != "" {
					fmt.Printf("      Response: %s\n", respText)
				}
			}
			if quiet && progressEvery > 0 && sent%progressEvery == 0 {
				fmt.Printf("Progress: Sent=%d Failed=%d\n", sent, failed)
			}
		}

		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read input file: %w", err)
	}

	fmt.Printf("Done. Sent=%d Failed=%d\n", sent, failed)
	return nil
}

func postLine(client *http.Client, uri string, headers http.Header, line string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, uri, strings.NewReader(line))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header = headers.Clone()

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("status=%d body=%s", resp.StatusCode, compactJSON(body))
	}

	if len(bytes.TrimSpace(body)) == 0 {
		return "", nil
	}

	return compactJSON(body), nil
}

func extractTransactionID(line string) string {
	var tx inputTx
	if err := json.Unmarshal([]byte(line), &tx); err != nil {
		return "<unknown>"
	}
	if strings.TrimSpace(tx.TransactionID) == "" {
		return "<unknown>"
	}
	return tx.TransactionID
}

func compactJSON(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return ""
	}

	var js responseBody
	if err := json.Unmarshal(trimmed, &js); err != nil {
		return string(trimmed)
	}

	compact, err := json.Marshal(js)
	if err != nil {
		return string(trimmed)
	}
	return string(compact)
}
