from typing import Any

from src.streaming.decoder import decode_amount

INR_FRAUD_THRESHOLD = 50000


def process_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Extract a Debezium transaction event, decode its amount, and flag suspicious transactions."""
    payload = event.get("payload", {})
    operation = payload.get("op")
    transaction = payload.get("after")

    # Deletes have no "after" row.
    if transaction is None:
        return None

    transaction = transaction.copy()
    transaction["amount"] = decode_amount(transaction["amount"])

    currency = str(transaction.get("currency", "")).strip()
    amount = transaction["amount"]

    # Apply the INR threshold only to INR transactions.
    is_suspicious = currency == "INR" and amount > INR_FRAUD_THRESHOLD

    return {
        "operation": operation,
        "transaction": transaction,
        "is_suspicious": is_suspicious,
        "risk_reason": (
            f"INR transaction exceeds ₹{INR_FRAUD_THRESHOLD:,.2f}" if is_suspicious else None
        ),
    }
