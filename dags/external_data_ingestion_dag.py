from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd
import requests
import sqlite3
import os

# Configuration
DATA_DB = '/opt/airflow/data/hospital_capacity.db'
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')  # Set in Airflow Variables
WEATHER_API_URL = 'https://api.openweathermap.org/data/2.5/weather'

def fetch_weather_data(**context):
    """Fetch weather data from OpenWeatherMap API"""
    regions = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    weather_records = []
    
    for region in regions:
        response = requests.get(WEATHER_API_URL, params={'q': region, 'appid': WEATHER_API_KEY, 'units': 'metric'})
        if response.status_code == 200:
            data = response.json()
            weather_records.append({
                'date': datetime.now().date(),
                'region': region,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'weather_condition': data['weather'][0]['main']
            })
    
    df = pd.DataFrame(weather_records)
    context['ti'].xcom_push(key='weather_data', value=df.to_json())
    print(f"Fetched weather data for {len(weather_records)} regions")
    return len(weather_records)

def fetch_public_health_data(**context):
    """Fetch public health metrics (placeholder for CDC/state APIs)"""
    # Example: CDC COVID-19 API or state health department APIs
    # Replace with actual API endpoint
    health_records = []
    regions = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    
    for region in regions:
        # Placeholder data - replace with actual API call
        health_records.append({
            'date': datetime.now().date(),
            'region': region,
            'case_count': 0,  # Replace with API data
            'positivity_rate': 0.0,
            'hospitalization_rate': 0.0
        })
    
    df = pd.DataFrame(health_records)
    context['ti'].xcom_push(key='health_data', value=df.to_json())
    print(f"Fetched health data for {len(health_records)} regions")
    return len(health_records)

def fetch_events_holidays(**context):
    """Fetch local events and holidays"""
    # Use Calendarific API or similar
    events_records = []
    
    # Placeholder - replace with actual API
    events_records.append({
        'date': datetime.now().date(),
        'region': 'National',
        'is_holiday': False,
        'event_type': None,
        'event_name': None
    })
    
    df = pd.DataFrame(events_records)
    context['ti'].xcom_push(key='events_data', value=df.to_json())
    print(f"Fetched {len(events_records)} event records")
    return len(events_records)

def load_external_data(**context):
    """Load all external data into database"""
    ti = context['ti']
    
    # Get data from XCom
    weather_json = ti.xcom_pull(key='weather_data', task_ids='fetch_weather_data')
    health_json = ti.xcom_pull(key='health_data', task_ids='fetch_public_health_data')
    events_json = ti.xcom_pull(key='events_data', task_ids='fetch_events_holidays')
    
    conn = sqlite3.connect(DATA_DB)
    
    # Load weather data
    if weather_json:
        df_weather = pd.read_json(weather_json)
        df_weather.to_sql('weather_data', conn, if_exists='append', index=False)
        print(f"Loaded {len(df_weather)} weather records")
    
    # Load health data
    if health_json:
        df_health = pd.read_json(health_json)
        df_health.to_sql('public_health_data', conn, if_exists='append', index=False)
        print(f"Loaded {len(df_health)} health records")
    
    # Load events data
    if events_json:
        df_events = pd.read_json(events_json)
        df_events.to_sql('events_holidays', conn, if_exists='append', index=False)
        print(f"Loaded {len(df_events)} event records")
    
    conn.close()
    return True

def create_feature_table(**context):
    """Join external data with hospital capacity features"""
    conn = sqlite3.connect(DATA_DB)
    
    query = """
    CREATE TABLE IF NOT EXISTS hospital_capacity_features_enriched AS
    SELECT 
        h.*,
        w.temperature,
        w.humidity,
        w.weather_condition,
        ph.case_count,
        ph.positivity_rate,
        ph.hospitalization_rate,
        e.is_holiday,
        e.event_type
    FROM hospital_capacity_features h
    LEFT JOIN weather_data w ON h.date = w.date AND h.region = w.region
    LEFT JOIN public_health_data ph ON h.date = ph.date AND h.region = ph.region
    LEFT JOIN events_holidays e ON h.date = e.date
    """
    
    conn.execute(query)
    conn.commit()
    
    # Verify
    result = pd.read_sql_query("SELECT COUNT(*) as count FROM hospital_capacity_features_enriched", conn)
    conn.close()
    
    print(f"Created enriched feature table with {result['count'][0]} rows")
    return result['count'][0]

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'external_data_ingestion',
    default_args=default_args,
    description='Daily ingestion of weather, health, and events data',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['data-ingestion', 'external-data']
)

task_weather = PythonOperator(task_id='fetch_weather_data', python_callable=fetch_weather_data, dag=dag)
task_health = PythonOperator(task_id='fetch_public_health_data', python_callable=fetch_public_health_data, dag=dag)
task_events = PythonOperator(task_id='fetch_events_holidays', python_callable=fetch_events_holidays, dag=dag)
task_load = PythonOperator(task_id='load_external_data', python_callable=load_external_data, dag=dag)
task_features = PythonOperator(task_id='create_feature_table', python_callable=create_feature_table, dag=dag)

[task_weather, task_health, task_events] >> task_load >> task_features