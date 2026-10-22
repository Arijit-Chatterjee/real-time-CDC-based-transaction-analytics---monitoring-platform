from decimal import Decimal
from unittest.mock import MagicMock, call  # noqa: F401

import pytest

from src.storage.postgres import connect, persist_event


@pytest.fixture
def fake_connection():
    conn = MagicMock()
    conn.transaction.return_value.__enter__.return_value = None
    conn.transaction.return_value.__exit__.return_value = False
    conn.execute.return_value.fetchone.return_value = (123,)
    return conn


@pytest.fixture
def sample_event():
    return {
        "payload": {
            "op": "c",
            "after": {
                "transaction_id": 10,
                "customer_id": 20,
                "merchant_id": 30,
            },
        }
    }


@pytest.fixture
def sample_result():
    return {
        "operation": "c",
        "transaction": {
            "transaction_id": 10,
            "customer_id": 20,
            "merchant_id": 30,
            "amount": Decimal("60000.00"),
            "currency": "INR",
        },
        "is_suspicious": True,
        "risk_reason": "INR transaction exceeds ₹50,000.00",
    }


def persist(conn, event, result):
    return persist_event(
        conn,
        event=event,
        topic="streamguard.public.transactions",
        partition_id=0,
        kafka_offset=42,
        message_key="10",
        result=result,
    )


def test_new_suspicious_event_records_ledger_and_alert(
    fake_connection, sample_event, sample_result
):
    assert persist(fake_connection, sample_event, sample_result) is True

    assert fake_connection.transaction.call_count == 1
    assert fake_connection.execute.call_count == 2

    ledger_call, alert_call = fake_connection.execute.call_args_list
    assert "streamguard_event_ledger" in ledger_call.args[0]
    assert "ON CONFLICT" in ledger_call.args[0]
    assert "streamguard_fraud_alerts" in alert_call.args[0]
    assert alert_call.args[1][0] == 123
    assert alert_call.args[1][6] == Decimal("50000.00")


def test_duplicate_event_does_not_create_fraud_alert(fake_connection, sample_event, sample_result):
    fake_connection.execute.return_value.fetchone.return_value = None

    assert persist(fake_connection, sample_event, sample_result) is False

    assert fake_connection.transaction.call_count == 1
    assert fake_connection.execute.call_count == 1
    assert "streamguard_event_ledger" in fake_connection.execute.call_args.args[0]


def test_non_suspicious_event_does_not_create_fraud_alert(
    fake_connection, sample_event, sample_result
):
    sample_result["is_suspicious"] = False
    sample_result["risk_reason"] = None

    assert persist(fake_connection, sample_event, sample_result) is True

    assert fake_connection.execute.call_count == 1


def test_alert_failure_propagates_and_rolls_back(fake_connection, sample_event, sample_result):
    fake_connection.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=(123,))),
        RuntimeError("Simulated alert insert failure"),
    ]

    with pytest.raises(RuntimeError, match="Simulated alert insert failure"):
        persist(fake_connection, sample_event, sample_result)

    fake_connection.transaction.return_value.__exit__.assert_called_once()
    assert fake_connection.execute.call_count == 2


def test_connect_requires_database_password(monkeypatch):
    monkeypatch.delenv("STREAMGUARD_DB_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="STREAMGUARD_DB_PASSWORD"):
        connect()
