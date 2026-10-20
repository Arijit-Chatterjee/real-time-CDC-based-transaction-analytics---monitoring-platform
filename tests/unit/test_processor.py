from decimal import Decimal

from src.streaming.processor import process_event


def test_process_snapshot_event():
    event = {
        "payload": {
            "op": "r",
            "after": {
                "transaction_id": "txn-001",
                "amount": "BCkG",
                "currency": "INR",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["operation"] == "r"
    assert result["transaction"]["amount"] == Decimal("2726.46")


def test_process_delete_event():
    event = {
        "payload": {
            "op": "d",
            "after": None,
        }
    }

    assert process_event(event) is None
