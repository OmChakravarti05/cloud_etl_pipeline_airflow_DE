from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd

RAW_PATH = "/opt/airflow/dags/raw_data.csv"
PROCESSED_PATH = "/opt/airflow/dags/processed_data.csv"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="first_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning", "etl"],
    default_args=default_args,
)
def first_etl_pipeline():

    @task
    def extract():
        import requests
        url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(RAW_PATH, "wb") as f:
            f.write(response.content)
        return RAW_PATH

    @task
    def transform(raw_path: str):
        df = pd.read_csv(raw_path)
        df = df.dropna()
        df.columns = [c.strip().lower() for c in df.columns]
        df.to_csv(PROCESSED_PATH, index=False)
        return PROCESSED_PATH

    @task
    def load(processed_path: str):
        import psycopg2
        from psycopg2.extras import execute_values

        df = pd.read_csv(processed_path)

        conn = psycopg2.connect(
            host="data-postgres",
            dbname="pipeline_data",
            user="data",
            password="data",
            connect_timeout=10,
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS tips (
                            total_bill FLOAT,
                            tip FLOAT,
                            sex TEXT,
                            smoker TEXT,
                            day TEXT,
                            time TEXT,
                            size INTEGER
                        );
                    """)
                    cur.execute("TRUNCATE TABLE tips;")
                    rows = [tuple(row) for row in df.itertuples(index=False)]
                    execute_values(
                        cur,
                        "INSERT INTO tips (total_bill, tip, sex, smoker, day, time, size) VALUES %s",
                        rows,
                    )
            print(f"Loaded {len(rows)} rows into Postgres.")
        finally:
            conn.close()

    raw = extract()
    processed = transform(raw)
    load(processed)

first_etl_pipeline()