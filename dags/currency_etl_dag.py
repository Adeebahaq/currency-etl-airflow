"""Daily currency-exchange ETL: Frankfurter API -> transform -> MongoDB.

Three tasks, connected with named XComs:
    extract_rates   --(key="raw_rates")-->         transform_rates
    transform_rates --(key="transformed_rates")--> load_to_mongodb
"""
import os
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
QUOTES = "USD,GBP,JPY"
SOURCE = "Frankfurter API"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")


def fetch_rates():
    """Call the API. v2 returns a list: [{date, base, quote, rate}, ...]."""
    response = requests.get(
        API_URL, params={"base": BASE_CURRENCY, "quotes": QUOTES}, timeout=30
    )
    response.raise_for_status()
    return response.json()


def to_documents(raw_rates):
    """Turn the API rows into the documents required by the assignment."""
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return [
        {
            "base_currency": row["base"],
            "target_currency": row["quote"],
            "exchange_rate": float(row["rate"]),
            "rate_date": row["date"],
            "source": SOURCE,
            "retrieved_at": retrieved_at,
        }
        for row in raw_rates
    ]


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["etl", "mongodb", "currency"],
)
def currency_exchange_etl():
    @task
    def extract_rates(ti=None):
        raw = fetch_rates()
        ti.xcom_push(key="raw_rates", value=raw)  # named XCom
        print(f"Extracted {len(raw)} rates: {raw}")

    @task
    def transform_rates(ti=None):
        raw = ti.xcom_pull(task_ids="extract_rates", key="raw_rates")
        if not raw:
            raise ValueError("No raw rates found in XCom key 'raw_rates'")
        docs = to_documents(raw)
        ti.xcom_push(key="transformed_rates", value=docs)  # named XCom
        print(f"Transformed {len(docs)} documents: {docs}")

    @task
    def load_to_mongodb(ti=None):
        docs = ti.xcom_pull(task_ids="transform_rates", key="transformed_rates")
        if not docs:
            raise ValueError("No documents found in XCom key 'transformed_rates'")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]
            collection.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
            )
            # Upsert so re-running a day never creates duplicates
            ops = [
                UpdateOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                    },
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ]
            result = collection.bulk_write(ops)
            print(f"Inserted {result.upserted_count}, updated {result.modified_count}")
        finally:
            client.close()

    extract_rates() >> transform_rates() >> load_to_mongodb()


currency_exchange_etl()
