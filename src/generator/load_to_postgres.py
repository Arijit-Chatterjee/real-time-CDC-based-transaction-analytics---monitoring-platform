"""Load generated customers and merchants into PostgreSQL."""

import json
import os
from pathlib import Path

import psycopg

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://streamguard:1234@127.0.0.1:5433/streamguard",
)


def read_jsonl(file_path: Path) -> list[dict]:
    """Read a JSON Lines file into a list of dictionaries."""
    with file_path.open(encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def load_customers(conn: psycopg.Connection) -> None:
    """Load customers from JSONL into PostgreSQL."""
    customers = read_jsonl(DATA_DIR / "customers.jsonl")

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO customers (
                customer_id, created_at, home_region, account_status
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            [
                (
                    row["customer_id"],
                    row["created_at"],
                    row["home_region"],
                    row["account_status"],
                )
                for row in customers
            ],
        )

    print(f"Customers processed: {len(customers)}")


def load_merchants(conn: psycopg.Connection) -> None:
    """Load merchants from JSONL into PostgreSQL."""
    merchants = read_jsonl(DATA_DIR / "merchants.jsonl")

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO merchants (
                merchant_id, created_at, merchant_name,
                merchant_category, merchant_region
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (merchant_id) DO NOTHING
            """,
            [
                (
                    row["merchant_id"],
                    row["created_at"],
                    row["merchant_name"],
                    row["merchant_category"],
                    row["merchant_region"],
                )
                for row in merchants
            ],
        )

    print(f"Merchants processed: {len(merchants)}")


def load_transactions(conn: psycopg.Connection) -> None:
    """Load generated transactions into PostgreSQL."""
    transactions = read_jsonl(DATA_DIR / "transactions.jsonl")

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO transactions (
                transaction_id, customer_id, merchant_id, amount,
                currency, event_time, created_at, updated_at,
                region, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [
                (
                    row["transaction_id"],
                    row["customer_id"],
                    row["merchant_id"],
                    row["amount"],
                    row["currency"],
                    row["event_time"],
                    row["created_at"],
                    row["updated_at"],
                    row["region"],
                    row["status"],
                )
                for row in transactions
            ],
        )

    print(f"Transactions processed: {len(transactions)}")


def main() -> None:
    """Load reference data and transactions into PostgreSQL."""
    with psycopg.connect(DATABASE_URL) as conn:
        load_customers(conn)
        load_merchants(conn)
        load_transactions(conn)


if __name__ == "__main__":
    main()
