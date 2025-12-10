# External Data Sources Guide

## 1. Weather Data - OpenWeatherMap

**API:** https://openweathermap.org/api
**Cost:** Free tier (1000 calls/day)

**Setup:**
```bash
# Get API key from openweathermap.org
export WEATHER_API_KEY='your_key_here'
```

**Features:** temperature, humidity, pressure, weather_condition
**Join:** date + region

---

## 2. Public Health Data

### CDC COVID-19 API
**API:** https://data.cdc.gov/resource/pwn4-m3yp.json
**Cost:** Free

**Setup:**
```python
import requests
url = 'https://data.cdc.gov/resource/pwn4-m3yp.json'
response = requests.get(url, params={'state': 'NY'})
```

### FluView (Influenza Surveillance)
**API:** https://gis.cdc.gov/grasp/fluview/fluportaldashboard.html
**Alternative:** Use `cdcfluview` Python package

**Features:** case_count, positivity_rate, hospitalization_rate
**Join:** date + region

---

## 3. Events & Holidays

### Calendarific API
**API:** https://calendarific.com/api-documentation
**Cost:** Free tier (1000 calls/month)

**Setup:**
```bash
export CALENDARIFIC_API_KEY='your_key_here'
```

**Features:** is_holiday, event_type, event_name
**Join:** date

---

## 4. Demographics (Optional)

### US Census Bureau API
**API:** https://www.census.gov/data/developers/data-sets.html
**Cost:** Free

**Features:** population, age_distribution, median_income
**Join:** region (one-time enrichment)

---

## Implementation Steps

1. **Get API Keys:**
   - OpenWeatherMap: https://home.openweathermap.org/api_keys
   - Calendarific: https://calendarific.com/account/api

2. **Set Airflow Variables:**
   ```bash
   airflow variables set WEATHER_API_KEY 'your_key'
   airflow variables set CALENDARIFIC_API_KEY 'your_key'
   ```

3. **Enable DAG:**
   - Copy `dags/external_data_ingestion_dag.py` to Airflow
   - Runs daily at 6 AM
   - Creates `hospital_capacity_features_enriched` table

4. **Update Retraining DAG:**
   - Change query from `hospital_capacity_features` to `hospital_capacity_features_enriched`
   - Model will automatically use new features

---

## Data Flow

```
API Sources → Airflow DAG → SQLite Tables → Feature Join → Model Training
```

**Tables:**
- `weather_data`
- `public_health_data`
- `events_holidays`
- `hospital_capacity_features_enriched` (joined)
