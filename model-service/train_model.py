from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

RANDOM_STATE = 42
OUTPUT_PATH = Path(__file__).resolve().parent / "models" / "isolation_forest.joblib"


def generate_training_data() -> np.ndarray:
    rng = np.random.default_rng(RANDOM_STATE)

    normal = np.column_stack(
        [
            rng.normal(loc=120.0, scale=35.0, size=5000),  # amount
            rng.poisson(lam=1.5, size=5000),  # velocity_1m
            rng.poisson(lam=4.0, size=5000),  # velocity_5m
            rng.uniform(0.0, 0.45, size=5000),  # device_risk_score
            rng.uniform(0.0, 0.40, size=5000),  # geo_risk_score
        ]
    )

    anomaly = np.column_stack(
        [
            rng.normal(loc=600.0, scale=120.0, size=250),
            rng.poisson(lam=10.0, size=250),
            rng.poisson(lam=20.0, size=250),
            rng.uniform(0.6, 1.0, size=250),
            rng.uniform(0.6, 1.0, size=250),
        ]
    )

    x = np.vstack([normal, anomaly]).astype(float)

    # Keep values in realistic bounds.
    x[:, 0] = np.clip(x[:, 0], 0.01, None)
    x[:, 1] = np.clip(x[:, 1], 0.0, None)
    x[:, 2] = np.clip(x[:, 2], 0.0, None)
    x[:, 3] = np.clip(x[:, 3], 0.0, 1.0)
    x[:, 4] = np.clip(x[:, 4], 0.0, 1.0)

    return x


def train_and_save_model() -> None:
    x_train = generate_training_data()

    model = IsolationForest(
        n_estimators=250,
        contamination=0.05,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(x_train)

    model_bundle = {
        "model": model,
        "version": f"isoforest-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "feature_order": [
            "amount",
            "velocity_1m",
            "velocity_5m",
            "device_risk_score",
            "geo_risk_score",
        ],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model_bundle, OUTPUT_PATH)

    print(f"Saved model to: {OUTPUT_PATH}")
    print(f"Training rows: {x_train.shape[0]}")
    print(f"Feature count: {x_train.shape[1]}")


if __name__ == "__main__":
    train_and_save_model()
