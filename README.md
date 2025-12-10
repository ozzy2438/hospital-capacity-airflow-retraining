# Hospital Capacity Airflow Retraining Pipeline

Apache Airflow DAG for automated ML retraining with drift detection and auto-promotion.

## Features

- **Monthly scheduled retraining** (1st of each month)
- **Drift detection** using KS-test and PSI
- **Auto-promotion criteria**:
  - AUC improvement > 1%
  - No recall regression > 10%
  - No significant drift detected

## DAG Tasks

1. `extract_training_data` - Load last 12 months from warehouse
2. `train_candidate_model` - Train RandomForest on fresh data
3. `calculate_drift` - KS-test and PSI for feature/target drift
4. `evaluate_models` - Compare production vs candidate metrics
5. `maybe_promote_model` - Auto-promote if criteria met

## Setup

```bash
pip install apache-airflow scikit-learn scipy pandas joblib
```

Copy `dags/model_retraining_dag.py` to your Airflow dags folder.

## Configuration

Update paths in DAG:
- `MODEL_DIR`: Model storage location
- `DATA_DB`: SQLite database path

## Monitoring

Check Airflow UI for:
- Drift alerts
- Model promotion decisions
- Performance metrics logged to `metrics.json`
