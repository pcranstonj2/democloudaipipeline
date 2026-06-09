import os
import time
from pathlib import Path
from typing import List

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

DEFAULT_MODEL_PATH = Path(__file__).resolve().parent / "models" / "isolation_forest.joblib"
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))

app = FastAPI(title="Model Service", version="1.0.0")


class PredictRequest(BaseModel):
    amount: float = Field(..., gt=0)
    velocity_1m: int = Field(..., ge=0)
    velocity_5m: int = Field(..., ge=0)
    device_risk_score: float = Field(..., ge=0, le=1)
    geo_risk_score: float = Field(..., ge=0, le=1)


class PredictResponse(BaseModel):
    risk_score: float
    is_anomaly: bool
    model_version: str


_model_bundle = None

model_predict_requests_total = Counter(
    "model_predict_requests_total",
    "Total number of model predict requests by outcome.",
    ["outcome"],
)
model_predict_latency_seconds = Histogram(
    "model_predict_latency_seconds",
    "Latency of model /predict endpoint.",
)


def load_model_bundle():
    global _model_bundle
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model file not found at {MODEL_PATH}. Run train_model.py first."
        )
    _model_bundle = joblib.load(MODEL_PATH)


@app.on_event("startup")
def startup_event() -> None:
    load_model_bundle()
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "model_loaded": _model_bundle is not None}


@app.post("/predict", response_model=PredictResponse)
def predict(payload: PredictRequest) -> PredictResponse:
    start_time = time.perf_counter()
    outcome = "error"
    try:
        if _model_bundle is None:
            raise HTTPException(status_code=503, detail="Model is not loaded")

        model = _model_bundle["model"]
        model_version = _model_bundle.get("version", "unknown")

        features: List[float] = [
            payload.amount,
            float(payload.velocity_1m),
            float(payload.velocity_5m),
            payload.device_risk_score,
            payload.geo_risk_score,
        ]

        x = np.array([features], dtype=float)

        score_sample = float(model.score_samples(x)[0])
        is_anomaly = bool(model.predict(x)[0] == -1)

        # Lower score_samples values indicate more anomalous points; map to 0..1 risk.
        risk_score = float(1.0 / (1.0 + np.exp(5.0 * score_sample)))

        outcome = "success"
        return PredictResponse(
            risk_score=round(risk_score, 6),
            is_anomaly=is_anomaly,
            model_version=model_version,
        )
    finally:
        model_predict_requests_total.labels(outcome=outcome).inc()
        model_predict_latency_seconds.observe(time.perf_counter() - start_time)
