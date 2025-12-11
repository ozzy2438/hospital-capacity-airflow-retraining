from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error
import pickle
import os

def load_new_data(**context):
    """Load new data for retraining"""
    print("Loading new data from database/warehouse")
    # Replace with actual data loading logic
    return True

def detect_drift(**context):
    """Calculate PSI and KS statistics for drift detection"""
    # PSI > 0.2 or KS p-value < 0.05 triggers retraining
    psi_score = 0.15  # Example
    ks_pvalue = 0.08  # Example
    
    drift_detected = psi_score > 0.2 or ks_pvalue < 0.05
    context['ti'].xcom_push(key='drift_detected', value=drift_detected)
    context['ti'].xcom_push(key='psi_score', value=psi_score)
    return drift_detected

def train_new_model(**context):
    """Train new model on updated data"""
    print("Training new model...")
    # Replace with actual training logic
    # model = YourModel()
    # model.fit(X_train, y_train)
    # pickle.dump(model, open('/tmp/new_model.pkl', 'wb'))
    return True

def evaluate_models(**context):
    """Evaluate old vs new model performance"""
    # Load old and new models
    # old_model = pickle.load(open('/models/production_model.pkl', 'rb'))
    # new_model = pickle.load(open('/tmp/new_model.pkl', 'rb'))
    
    # Evaluate on validation set
    old_accuracy = 0.87  # Example
    new_accuracy = 0.89  # Example
    
    old_f1 = 0.85
    new_f1 = 0.88
    
    old_mae = 12.5
    new_mae = 10.2
    
    # Store metrics
    context['ti'].xcom_push(key='old_accuracy', value=old_accuracy)
    context['ti'].xcom_push(key='new_accuracy', value=new_accuracy)
    context['ti'].xcom_push(key='old_f1', value=old_f1)
    context['ti'].xcom_push(key='new_f1', value=new_f1)
    
    print(f"Old Model - Accuracy: {old_accuracy}, F1: {old_f1}, MAE: {old_mae}")
    print(f"New Model - Accuracy: {new_accuracy}, F1: {new_f1}, MAE: {new_mae}")
    
    return True

def decide_promotion(**context):
    """Decide whether to promote new model based on performance criteria"""
    ti = context['ti']
    old_accuracy = ti.xcom_pull(key='old_accuracy', task_ids='evaluate_models')
    new_accuracy = ti.xcom_pull(key='new_accuracy', task_ids='evaluate_models')
    
    # Auto-promote if new model beats old by >= 2%
    improvement = (new_accuracy - old_accuracy) / old_accuracy * 100
    
    if improvement >= 2.0:
        print(f"Auto-promoting: {improvement:.2f}% improvement")
        return 'promote_model'
    else:
        print(f"Skipping promotion: only {improvement:.2f}% improvement")
        return 'skip_promotion'

def promote_model(**context):
    """Move new model to production"""
    print("Promoting new model to production...")
    # os.rename('/tmp/new_model.pkl', '/models/production_model.pkl')
    
    # Log promotion event
    ti = context['ti']
    new_accuracy = ti.xcom_pull(key='new_accuracy', task_ids='evaluate_models')
    print(f"Model promoted with accuracy: {new_accuracy}")
    return True

def send_alert(**context):
    """Send Slack/email notification"""
    ti = context['ti']
    drift_detected = ti.xcom_pull(key='drift_detected', task_ids='detect_drift')
    psi_score = ti.xcom_pull(key='psi_score', task_ids='detect_drift')
    
    message = f"Retraining completed. Drift detected: {drift_detected}, PSI: {psi_score}"
    print(f"Alert: {message}")
    return True

default_args = {
    'owner': 'ml-ops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['ml-team@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'model_retraining_pipeline',
    default_args=default_args,
    description='Monthly model retraining with drift detection and auto-promotion',
    schedule_interval='0 0 1 * * ',  # Monthly on 1st at midnight
    catchup=False,
    tags=['ml', 'retraining', 'production'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    load_data = PythonOperator(
        task_id='load_new_data',
        python_callable=load_new_data,
    )
    
    drift_check = PythonOperator(
        task_id='detect_drift',
        python_callable=detect_drift,
    )
    
    train = PythonOperator(
        task_id='train_new_model',
        python_callable=train_new_model,
    )
    
    evaluate = PythonOperator(
        task_id='evaluate_models',
        python_callable=evaluate_models,
    )
    
    promotion_decision = BranchPythonOperator(
        task_id='decide_promotion',
        python_callable=decide_promotion,
    )
    
    promote = PythonOperator(
        task_id='promote_model',
        python_callable=promote_model,
    )
    
    skip = DummyOperator(task_id='skip_promotion')
    
    alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
        trigger_rule='none_failed',
    )
    
    end = DummyOperator(task_id='end', trigger_rule='none_failed')
    
    # Define workflow
    start >> load_data >> drift_check >> train >> evaluate >> promotion_decision
    promotion_decision >> [promote, skip]
    [promote, skip] >> alert >> end
