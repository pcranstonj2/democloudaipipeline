# Real-Time Transaction Scoring Pipeline

An end-to-end, event-driven AI platform demo for real-time transaction risk scoring.

This project shows how to build and operate a distributed pipeline that:

- Ingests transactions via HTTP
- Processes events through Kafka topics
- Enriches features in a separate microservice
- Calls a model-serving API for inference
- Publishes scored results for downstream consumers
- Supports observability with Grafana dashboards and Kubernetes manifests

## Architecture Summary

The system uses a microservice architecture with Kafka as the event backbone.

Core services:

1. Ingestion Service (Go)
- Exposes POST /transaction
- Validates payloads
- Publishes to raw-transactions

2. Enrichment Service (Go)
- Consumes raw-transactions
- Adds geo, device, and velocity metadata
- Publishes to enriched-transactions

3. Model Service (FastAPI + scikit-learn)
- Trains and loads an IsolationForest model
- Exposes POST /predict
- Returns risk score and anomaly flag

4. Scoring Orchestrator (Go)
- Consumes enriched-transactions
- Calls model-service /predict asynchronously
- Uses exponential backoff on transient failures
- Publishes to scored-transactions

5. Drift Monitor (Go)
- Includes async Kafka consume/process/publish pattern
- Supports model-call and publish retry/backoff behavior

6. Infrastructure and Monitoring
- Docker Compose for local orchestration
- Kubernetes manifests for Deployments, Services, ConfigMaps, HPAs
- Grafana dashboard JSON + provisioning config

## End-to-End Event Flow

1. Client sends transaction to ingestion-service.
2. ingestion-service validates and publishes event to raw-transactions.
3. enrichment-service consumes raw event and adds feature metadata.
4. Enriched event is published to enriched-transactions.
5. scoring-orchestrator consumes enriched event and calls model-service /predict.
6. Scored event is published to scored-transactions.
7. Downstream tools can consume scored output for alerts, case management, or analytics.

## Repository Layout

- ingestion-service: Go HTTP API producer to Kafka
- enrichment-service: Go Kafka consumer/producer enrichment worker
- model-service: FastAPI model API + model training script
- scoring-orchestrator: Go async scorer with retry/backoff
- drift-monitor: Go async monitor/scorer workflow implementation
- infrastructure/grafana: dashboard JSON + provisioning files
- infrastructure/k8s: Kubernetes manifests for the platform

## Prerequisites

For local development:

- Docker and Docker Compose
- Go 1.22+ (ingestion) and Go 1.26+ (other Go services)
- Python 3.11+ (model-service local runs)
- Optional: kubectl + a Kubernetes cluster (kind/minikube/AKS)

## Quick Start (Docker Compose)

Run the full local stack from repository root:

```bash
docker compose up --build
```

Stop the stack:

```bash
docker compose down
```

Exposed endpoints:

- ingestion-service: http://localhost:8080
- model-service: http://localhost:8010
- Kafka external listener (host): localhost:29092

## Test the Pipeline

### 1. Health checks

```bash
curl http://localhost:8080/healthz
curl http://localhost:8010/healthz
```

### 2. Submit a transaction

```bash
curl -X POST http://localhost:8080/transaction \
	-H "Content-Type: application/json" \
	-d '{
		"transaction_id": "txn-1001",
		"customer_id": "cust-42",
		"amount": 125.75,
		"currency": "USD",
		"merchant_id": "m-778",
		"timestamp": "2026-06-09T12:00:00Z"
	}'
```

Expected ingestion response: HTTP 202 Accepted when Kafka is reachable.

## Run Services Individually (Without Compose)

### Ingestion service

```bash
cd ingestion-service
go run .
```

### Enrichment service

```bash
cd enrichment-service
go run .
```

### Model service

```bash
cd model-service
python -m pip install -r requirements.txt
python train_model.py
python -m uvicorn app:app --host 0.0.0.0 --port 8010
```

### Scoring orchestrator

```bash
cd scoring-orchestrator
go run .
```

### Drift monitor

```bash
cd drift-monitor
go run .
```

## Kafka Topics

- raw-transactions
	- Produced by ingestion-service
	- Consumed by enrichment-service

- enriched-transactions
	- Produced by enrichment-service
	- Consumed by scoring-orchestrator and drift-monitor

- scored-transactions
	- Produced by scoring-orchestrator
	- Consumed by downstream systems

## Kubernetes Deployment

Apply all manifests:

```bash
kubectl apply -f infrastructure/k8s/all-services.yaml
```

Resources included:

- Namespace
- ConfigMaps
- Deployments
- Services
- HorizontalPodAutoscalers

Note: Image names in manifests use local tags (for example ingestion-service:latest). Update these to your registry for shared clusters.

## Azure AKS Deployment

This repository includes a PowerShell helper script to provision Azure infrastructure,
build service images into ACR, and deploy the Kubernetes manifest to AKS.

Script location:

- infrastructure/azure/deploy-aks.ps1

### Prerequisites

- Azure CLI logged in (`az login`)
- kubectl installed
- Permissions to create Resource Group, ACR, and AKS resources

### One-command deployment

From the repository root in PowerShell:

```powershell
.\infrastructure\azure\deploy-aks.ps1 `
	-ResourceGroup rg-transaction-pipeline `
	-Location eastus `
	-AksName aks-transaction-pipeline `
	-AcrName acrtxpipeline
```

What the script does:

1. Creates resource group, ACR, and AKS (unless skipped).
2. Builds and pushes service images to ACR.
3. Rewrites local image tags in the Kubernetes manifest to ACR image URLs.
4. Applies the manifest to AKS.
5. Patches `ingestion-service` to `LoadBalancer` for external access.

### Optional script flags

- `-SkipInfrastructure`: use an existing AKS and ACR.
- `-SkipImageBuild`: skip ACR builds and deploy using existing tags.
- `-Namespace`: override default namespace (`transaction-pipeline`).

### Get the public ingestion endpoint

```bash
kubectl get service ingestion-service -n transaction-pipeline
```

Use the returned external IP with port 8080.

### Secrets template

Use the template at `infrastructure/azure/app-secrets-template.yaml` to keep sensitive values
out of ConfigMaps as you introduce credentials.

## Grafana Dashboard Auto-Loading

Dashboard assets:

- infrastructure/grafana/transaction-scoring-dashboard.json
- infrastructure/grafana/provisioning/dashboards/transaction-scoring-provider.yml

Provider path is configured as:

- /var/lib/grafana/dashboards

Mount both dashboard JSON and provisioning file into your Grafana container for automatic import on startup.

## Reliability and Scaling Notes

- Kafka producer retry/backoff is explicitly configured in Go services.
- scoring-orchestrator and drift-monitor use worker pools for async processing.
- Exponential backoff retry is applied around model inference and publish operations.
- Kubernetes HPAs are included to scale service replicas by CPU utilization.

## Troubleshooting

1. ingestion-service returns 503 on POST /transaction
- Kafka is likely not reachable. Verify broker address and port.

2. model-service fails startup
- Ensure model artifact exists. Run python train_model.py in model-service.

3. No scored events
- Confirm enriched-transactions receives events.
- Confirm MODEL_SERVICE_URL points to model-service /predict.
- Check scoring-orchestrator logs for retry failures.

4. Docker compose command not found
- Install Docker Desktop (or Docker Engine + Compose plugin) and ensure docker is in PATH.

## Next Improvements

- Add OpenTelemetry traces across all services
- Add dead-letter topic handling and replay tooling
- Add contract/schema management (Avro + Schema Registry)
- Add CI pipeline for build/test/security scans
