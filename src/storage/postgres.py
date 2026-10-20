import os
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def connect() -> psycopg.Connection:
    """Open a PostgreSQL connection using environment-based credentials."""
    password = os.environ.get("STREAMGUARD_DB_PASSWORD")
    if not password:
        raise RuntimeError(
            "Set the STREAMGUARD_DB_PASSWORD environment variable before connecting."
        )

    return psycopg.connect(
        host=os.environ.get("STREAMGUARD_DB_HOST", "localhost"),
        port=int(os.environ.get("STREAMGUARD_DB_PORT", "5433")),
        dbname=os.environ.get("STREAMGUARD_DB_NAME", "streamguard"),
        user=os.environ.get("STREAMGUARD_DB_USER", "streamguard"),
        password=password,
    )


def persist_event(
    conn: psycopg.Connection,
    *,
    event: dict[str, Any],
    topic: str,
    partition_id: int,
    kafka_offset: int,
    message_key: str | None,
    result: dict[str, Any] | None,
    threshold_amount: Decimal = Decimal("50000.00"),
) -> bool:
    """
    Persist a Debezium event and any fraud alert atomically.

    Returns True when the event is new, or False when it was already recorded.
    The caller must commit the Kafka offset only after this function succeeds.
    """
    payload = event.get("payload") or {}
    transaction = result["transaction"] if result is not None else None

    operation = payload.get("op")
    is_suspicious = bool(result and result["is_suspicious"])
    risk_reason = result.get("risk_reason") if result else None

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO public.streamguard_event_ledger (
                topic,
                partition_id,
                kafka_offset,
                message_key,
                operation,
                transaction_id,
                customer_id,
                merchant_id,
                amount,
                currency,
                is_suspicious,
                risk_reason,
                event_payload
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (topic, partition_id, kafka_offset) DO NOTHING
            RETURNING event_id
            """,
            (
                topic,
                partition_id,
                kafka_offset,
                message_key,
                operation,
                transaction.get("transaction_id") if transaction else None,
                transaction.get("customer_id") if transaction else None,
                transaction.get("merchant_id") if transaction else None,
                transaction.get("amount") if transaction else None,
                transaction.get("currency") if transaction else None,
                is_suspicious,
                risk_reason,
                Jsonb(event),
            ),
        ).fetchone()

        # A duplicate event is already durable. Do not create another alert.
        if inserted is None:
            return False

        event_id = inserted[0]

        if is_suspicious:
            if transaction is None:
                raise ValueError("A suspicious event must contain a transaction.")

            conn.execute(
                """
                INSERT INTO public.streamguard_fraud_alerts (
                    event_id,
                    transaction_id,
                    customer_id,
                    merchant_id,
                    amount,
                    currency,
                    threshold_amount,
                    risk_reason
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    transaction["transaction_id"],
                    transaction["customer_id"],
                    transaction["merchant_id"],
                    transaction["amount"],
                    transaction["currency"],
                    threshold_amount,
                    risk_reason,
                ),
            )

    return True
