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


def test_process_inr_transaction_above_threshold_is_suspicious():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-high",
                "amount": "AExLQQ==",
                "currency": "INR",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["is_suspicious"] is True
    assert result["risk_reason"] == "INR transaction exceeds ₹50,000.00"


def test_process_transaction_at_threshold_is_not_suspicious():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-threshold",
                "amount": "AAT0UA==",
                "currency": "INR",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["is_suspicious"] is False
    assert result["risk_reason"] is None


def test_process_non_inr_transaction_above_threshold_is_not_suspicious():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-usd",
                "amount": "AExLQQ==",
                "currency": "USD",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["is_suspicious"] is False
    assert result["risk_reason"] is None


def test_process_event_does_not_mutate_original_event():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-001",
                "amount": "BCkG",
                "currency": "INR",
            },
        }
    }

    original_amount = event["payload"]["after"]["amount"]

    process_event(event)

    assert event["payload"]["after"]["amount"] == original_amount


def test_inr_transaction_at_threshold_is_not_suspicious():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-threshold",
                "amount": "AExLQA==",
                "currency": "INR",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["is_suspicious"] is False
    assert result["risk_reason"] is None


def test_non_inr_transaction_above_threshold_is_not_suspicious():
    event = {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": "txn-usd",
                "amount": "AExLQQ==",
                "currency": "USD",
            },
        }
    }

    result = process_event(event)

    assert result is not None
    assert result["is_suspicious"] is False
    assert result["risk_reason"] is None
