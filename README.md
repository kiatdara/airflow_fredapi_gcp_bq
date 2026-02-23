# airflow_fredapi_gcp_bq

An automated data pipeline that fetches daily S&P 500 data from the FRED API and loads it into Google BigQuery using Apache Airflow on Google Cloud VM. The data is visualized through a Looker Studio dashboard.

---

## Technologies Used

- Apache Airflow — manages workflow and scheduling
- FRED API — data source for S&P 500 from the Federal Reserve
- Google BigQuery — data warehouse for storing the data
- Python — used to write DAG logic and data transformation
- Google Cloud VM — server for running Airflow
- Looker Studio — dashboard for data visualization

---

## Pipeline Flow

1. Airflow triggers the DAG `fred_sp500_daily` on a daily schedule
2. Task `fetch_sp500_to_bq` fetches the latest S&P 500 price from FRED API
3. Data is written into the `sp500_daily` table in BigQuery
4. Looker Studio connects to BigQuery and refreshes the dashboard automatically

---

## Project Structure

```
airflow_fredapi_gcp_bq/
│
├── dags/
│   └── fred_sp500_day.py       # Main DAG for fetching S&P 500 data
│
└── README.md                   # Project documentation
```

---

## BigQuery Schema

Dataset: `fred_data`  
Table: `sp500_daily`

| Column | Type  | Description         |
|--------|-------|---------------------|
| date   | DATE  | Trading date        |
| sp500  | FLOAT | S&P 500 index value |

---

## Setup & Usage

1. Configure Airflow Connection for Google Cloud Platform in the Airflow UI
2. Place the DAG file at `/opt/airflow/dags/`
3. Enable the `fred_sp500_daily` DAG in Airflow UI
4. Verify data in BigQuery with the following query

```sql
SELECT * FROM `your_project.fred_data.sp500_daily`
ORDER BY date DESC
LIMIT 10
```

---

## Looker Studio Dashboard

The pipeline is connected to a Looker Studio dashboard for real-time visualization of S&P 500 trends.

To connect Looker Studio to BigQuery:

1. Go to [Looker Studio](https://lookerstudio.google.com)
2. Create a new report and select BigQuery as the data source
3. Choose project `rational-lambda-487313-t2`, dataset `fred_data`, table `sp500_daily`
4. Build charts using `date` as the time dimension and `sp500` as the metric

---

## Notes

- FRED API updates S&P 500 data after market close (approximately 9-10 AM Thailand time next day)
- No data will be available on US market holidays or weekends
- Looker Studio dashboard refreshes automatically when new data arrives in BigQuery
