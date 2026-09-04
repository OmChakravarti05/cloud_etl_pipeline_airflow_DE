from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd

RAW_JSON_PATH = "/opt/airflow/dags/crypto_raw.json"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="crypto_price_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "api", "json"],
    default_args=default_args,
)
def crypto_price_pipeline():

    @task
    def extract():
        import requests, json
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin,ethereum,solana,dogecoin",
            "vs_currencies": "usd,eur",
            "include_market_cap": "true",
            "include_24hr_change": "true",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        with open(RAW_JSON_PATH, "w") as f:
            json.dump(response.json(), f)
        return RAW_JSON_PATH

    @task
    def transform(raw_path: str):
        import json
        with open(raw_path) as f:
            data = json.load(f)

        # Flatten nested JSON: {"bitcoin": {"usd": ..., "eur": ...}, ...}
        # into a proper table: one row per coin
        df = pd.json_normalize(data).T
        df.columns = ["value"]
        df = df.reset_index()
        df[["coin", "field"]] = df["index"].str.split(".", n=1, expand=True)
        df = df.pivot(index="coin", columns="field", values="value").reset_index()
        df["fetched_at"] = datetime.utcnow().isoformat()
        return df.to_dict(orient="records")

    @task
    def load(records: list):
        import psycopg2
        from psycopg2.extras import execute_values

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
                        CREATE TABLE IF NOT EXISTS crypto_prices (
                            coin TEXT,
                            usd FLOAT,
                            eur FLOAT,
                            usd_market_cap FLOAT,
                            usd_24h_change FLOAT,
                            fetched_at TIMESTAMP
                        );
                    """)
                    rows = [
                        (
                            r.get("coin"), r.get("usd"), r.get("eur"),
                            r.get("usd_market_cap"), r.get("usd_24h_change"),
                            r.get("fetched_at"),
                        )
                        for r in records
                    ]
                    execute_values(
                        cur,
                        "INSERT INTO crypto_prices (coin, usd, eur, usd_market_cap, usd_24h_change, fetched_at) VALUES %s",
                        rows,
                    )
            print(f"Loaded {len(rows)} coin price records.")
        finally:
            conn.close()

    raw = extract()
    records = transform(raw)
    load(records)

crypto_price_pipeline()