from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import numpy as np
from scipy import stats
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, brier_score_loss
import sqlite3
import os
import json
import shutil

# Configuration
MODEL_DIR = '/opt/airflow/models'
DATA_DB = '/opt/airflow/data/hospital_capacity.db'
PROD_MODEL_PATH = os.path.join(MODEL_DIR, 'production_model.pkl')
CANDIDATE_MODEL_PATH = os.path.join(MODEL_DIR, 'candidate_model.pkl')
METRICS_PATH = os.path.join(MODEL_DIR, 'metrics.json')

# Auto-promotion thresholds
AUC_IMPROVEMENT_THRESHOLD = 0.01
RECALL_REGRESSION_THRESHOLD = 0.10
DRIFT_KS_THRESHOLD = 0.01
DRIFT_PSI_THRESHOLD = 0.25

def extract_training_data(**context):
    conn = sqlite3.connect(DATA_DB)
    query = "SELECT * FROM hospital_capacity_features WHERE date >= date('now', '-12 months') ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Extracted {len(df)} rows for training")
    context['ti'].xcom_push(key='training_data', value=df.to_json())
    return len(df)

def train_candidate_model(**context):
    ti = context['ti']
    df_json = ti.xcom_pull(key='training_data', task_ids='extract_training_data')
    df = pd.read_json(df_json)
    feature_cols = [col for col in df.columns if col not in ['date', 'hospital_id', 'high_capacity_flag']]
    X = df[feature_cols]
    y = df['high_capacity_flag']
    model = RandomForestClassifier(n_estimators=100, max_depth=10, min_samples_split=20, random_state=42, n_jobs=-1)
    model.fit(X, y)
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, CANDIDATE_MODEL_PATH)
    print(f"Trained candidate model on {len(X)} samples")
    ti.xcom_push(key='feature_cols', value=feature_cols)
    return CANDIDATE_MODEL_PATH

def calculate_drift(**context):
    ti = context['ti']
    df_json = ti.xcom_pull(key='training_data', task_ids='extract_training_data')
    df = pd.read_json(df_json)
    split_idx = len(df) // 2
    df_reference = df.iloc[split_idx:]
    df_current = df.iloc[:split_idx]
    drift_results = {}
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    for col in numerical_cols:
        if col in df_reference.columns and col in df_current.columns:
            ks_stat, p_value = stats.ks_2samp(df_reference[col].dropna(), df_current[col].dropna())
            drift_results[col] = {'ks_statistic': ks_stat, 'p_value': p_value, 'drift_detected': p_value < DRIFT_KS_THRESHOLD}
    def calculate_psi(expected, actual, bins=10):
        expected_percents, bin_edges = np.histogram(expected, bins=bins)
        actual_percents, _ = np.histogram(actual, bins=bin_edges)
        expected_percents = expected_percents / len(expected) + 1e-6
        actual_percents = actual_percents / len(actual) + 1e-6
        psi = np.sum((actual_percents - expected_percents) * np.log(actual_percents / expected_percents))
        return psi
    psi_scores = {}
    for col in numerical_cols:
        if col in df_reference.columns and col in df_current.columns:
            psi = calculate_psi(df_reference[col].dropna(), df_current[col].dropna())
            psi_scores[col] = psi
    drift_summary = {'ks_tests': drift_results, 'psi_scores': psi_scores, 'significant_drift': any(r['drift_detected'] for r in drift_results.values()), 'max_psi': max(psi_scores.values()) if psi_scores else 0}
    ti.xcom_push(key='drift_summary', value=drift_summary)
    print(f"Drift detection complete. Significant drift: {drift_summary['significant_drift']}")
    return drift_summary

def evaluate_models(**context):
    ti = context['ti']
    df_json = ti.xcom_pull(key='training_data', task_ids='extract_training_data')
    feature_cols = ti.xcom_pull(key='feature_cols', task_ids='train_candidate_model')
    df = pd.read_json(df_json)
    X = df[feature_cols]
    y = df['high_capacity_flag']
    candidate_model = joblib.load(CANDIDATE_MODEL_PATH)
    y_pred_candidate = candidate_model.predict(X)
    y_proba_candidate = candidate_model.predict_proba(X)[:, 1]
    candidate_metrics = {'auc': roc_auc_score(y, y_proba_candidate), 'precision': precision_score(y, y_pred_candidate), 'recall': recall_score(y, y_pred_candidate), 'brier_score': brier_score_loss(y, y_proba_candidate)}
    if os.path.exists(PROD_MODEL_PATH):
        prod_model = joblib.load(PROD_MODEL_PATH)
        y_pred_prod = prod_model.predict(X)
        y_proba_prod = prod_model.predict_proba(X)[:, 1]
        prod_metrics = {'auc': roc_auc_score(y, y_proba_prod), 'precision': precision_score(y, y_pred_prod), 'recall': recall_score(y, y_pred_prod), 'brier_score': brier_score_loss(y, y_proba_prod)}
    else:
        prod_metrics = None
    evaluation = {'candidate': candidate_metrics, 'production': prod_metrics}
    ti.xcom_push(key='evaluation', value=evaluation)
    print(f"Candidate AUC: {candidate_metrics['auc']:.4f}")
    if prod_metrics:
        print(f"Production AUC: {prod_metrics['auc']:.4f}")
    return evaluation

def maybe_promote_model(**context):
    ti = context['ti']
    evaluation = ti.xcom_pull(key='evaluation', task_ids='evaluate_models')
    drift_summary = ti.xcom_pull(key='drift_summary', task_ids='calculate_drift')
    candidate_metrics = evaluation['candidate']
    prod_metrics = evaluation['production']
    should_promote = False
    reasons = []
    if prod_metrics is None:
        should_promote = True
        reasons.append('No production model exists')
    else:
        auc_improvement = candidate_metrics['auc'] - prod_metrics['auc']
        if auc_improvement > AUC_IMPROVEMENT_THRESHOLD:
            reasons.append(f"AUC improved by {auc_improvement:.4f}")
        recall_change = candidate_metrics['recall'] - prod_metrics['recall']
        if recall_change < -RECALL_REGRESSION_THRESHOLD:
            reasons.append(f"Recall regressed by {abs(recall_change):.4f} - BLOCKING")
            should_promote = False
        if drift_summary['significant_drift']:
            reasons.append('Significant drift detected - BLOCKING')
            should_promote = False
        if drift_summary['max_psi'] > DRIFT_PSI_THRESHOLD:
            reasons.append(f"High PSI detected: {drift_summary['max_psi']:.4f} - BLOCKING")
            should_promote = False
        if auc_improvement > AUC_IMPROVEMENT_THRESHOLD and recall_change >= -RECALL_REGRESSION_THRESHOLD and not drift_summary['significant_drift']:
            should_promote = True
    if should_promote:
        shutil.copy(CANDIDATE_MODEL_PATH, PROD_MODEL_PATH)
        print(f"Model PROMOTED to production")
        print(f"Reasons: {', '.join(reasons)}")
    else:
        print(f"Model NOT promoted")
        print(f"Reasons: {', '.join(reasons)}")
    metrics_log = {'timestamp': context['execution_date'].isoformat(), 'promoted': should_promote, 'reasons': reasons, 'candidate_metrics': candidate_metrics, 'production_metrics': prod_metrics, 'drift_summary': drift_summary}
    with open(METRICS_PATH, 'w') as f:
        json.dump(metrics_log, f, indent=2)
    return should_promote

default_args = {'owner': 'ml-team', 'depends_on_past': False, 'email_on_failure': True, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=5)}

dag = DAG('hospital_capacity_model_retraining', default_args=default_args, description='Monthly retraining pipeline with drift detection and auto-promotion', schedule_interval='0 0 1 * *', start_date=days_ago(1), catchup=False, tags=['ml', 'retraining', 'hospital-capacity'])

task_extract = PythonOperator(task_id='extract_training_data', python_callable=extract_training_data, dag=dag)
task_train = PythonOperator(task_id='train_candidate_model', python_callable=train_candidate_model, dag=dag)
task_drift = PythonOperator(task_id='calculate_drift', python_callable=calculate_drift, dag=dag)
task_evaluate = PythonOperator(task_id='evaluate_models', python_callable=evaluate_models, dag=dag)
task_promote = PythonOperator(task_id='maybe_promote_model', python_callable=maybe_promote_model, dag=dag)

task_extract >> task_train >> [task_drift, task_evaluate] >> task_promote