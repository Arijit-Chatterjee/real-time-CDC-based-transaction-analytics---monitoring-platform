import json
import random
from datetime import datetime
from uuid import UUID

import pytest

from src.generator.generate_transactions import (
    generate_customers,
    generate_merchants,
    generate_transactions,
    save_records,
)


def test_generator_creates_expected_counts():
    random.seed(42)
    customers = generate_customers(5)
    merchants = generate_merchants(3)
    transactions = generate_transactions(customers, merchants, 10)

    assert len(customers) == 5
    assert len(merchants) == 3
    assert len(transactions) == 10


def test_transactions_reference_existing_records():
    random.seed(42)
    customers = generate_customers(5)
    merchants = generate_merchants(3)
    transactions = generate_transactions(customers, merchants, 10)

    customer_ids = {customer["customer_id"] for customer in customers}
    merchant_ids = {merchant["merchant_id"] for merchant in merchants}

    for transaction in transactions:
        assert transaction["customer_id"] in customer_ids
        assert transaction["merchant_id"] in merchant_ids


def test_transactions_have_valid_uuids_and_timestamps():
    random.seed(42)
    customers = generate_customers(5)
    merchants = generate_merchants(3)
    transactions = generate_transactions(customers, merchants, 10)

    for transaction in transactions:
        # Check that the transaction ID is a valid UUID.
        UUID(transaction["transaction_id"])

        # Check that both timestamps are valid ISO 8601 values.
        event_time = datetime.fromisoformat(transaction["event_time"])
        created_at = datetime.fromisoformat(transaction["created_at"])

        # Both timestamps must include timezone information.
        assert event_time.utcoffset() is not None
        assert created_at.utcoffset() is not None


def test_generator_can_create_more_merchants_than_available_names():
    merchants = generate_merchants(10)

    assert len(merchants) == 10
    assert merchants[0]["merchant_name"] != merchants[5]["merchant_name"]


def test_transactions_reject_empty_customers():
    with pytest.raises(ValueError, match="customers cannot be empty"):
        generate_transactions([], generate_merchants(3), 10)


def test_transactions_reject_empty_merchants():
    with pytest.raises(ValueError, match="merchants cannot be empty"):
        generate_transactions(generate_customers(5), [], 10)


def test_transactions_reject_negative_count():
    with pytest.raises(ValueError, match="count cannot be negative"):
        generate_transactions(generate_customers(5), generate_merchants(3), -1)


def test_save_records_writes_valid_jsonl(tmp_path):
    customers = generate_customers(2)
    merchants = generate_merchants(2)
    transactions = generate_transactions(customers, merchants, 3)

    output_file = tmp_path / "transactions.jsonl"
    save_records(transactions, str(output_file))

    lines = output_file.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 3

    saved_transactions = [json.loads(line) for line in lines]
    assert saved_transactions == transactions


def test_transaction_event_times_are_one_second_apart():
    random.seed(42)
    customers = generate_customers(5)
    merchants = generate_merchants(3)
    transactions = generate_transactions(customers, merchants, 3)

    event_times = [
        datetime.fromisoformat(transaction["event_time"]) for transaction in transactions
    ]

    assert (event_times[1] - event_times[0]).total_seconds() == 1
    assert (event_times[2] - event_times[1]).total_seconds() == 1


def test_save_transactions_appends_without_overwriting(tmp_path):
    output_file = tmp_path / "transactions.jsonl"

    first_batch = generate_transactions(
        generate_customers(2),
        generate_merchants(2),
        3,
    )
    second_batch = generate_transactions(
        generate_customers(2),
        generate_merchants(2),
        2,
    )

    save_records(first_batch, str(output_file))
    save_records(second_batch, str(output_file), append=True)

    lines = output_file.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 5

    saved_transactions = [json.loads(line) for line in lines]
    expected_transactions = first_batch + second_batch
    assert saved_transactions == expected_transactions


def test_transaction_amounts_stay_within_expected_range():
    random.seed(42)
    customers = generate_customers(5)
    merchants = generate_merchants(3)
    transactions = generate_transactions(customers, merchants, 100)

    amounts = [transaction["amount"] for transaction in transactions]

    assert all(100 <= amount <= 50000 for amount in amounts)


def test_generated_merchants_have_unique_ids_and_names():
    merchants = generate_merchants(500)

    merchant_ids = [merchant["merchant_id"] for merchant in merchants]
    merchant_names = [merchant["merchant_name"] for merchant in merchants]

    assert len(merchant_ids) == 500
    assert len(set(merchant_ids)) == 500
    assert len(set(merchant_names)) == 500


def test_transactions_reference_existing_customers_and_merchants():
    customers = generate_customers(10)
    merchants = generate_merchants(5)

    transactions = generate_transactions(
        customers,
        merchants,
        100,
    )

    customer_ids = {customer["customer_id"] for customer in customers}
    merchant_ids = {merchant["merchant_id"] for merchant in merchants}

    for transaction in transactions:
        assert transaction["customer_id"] in customer_ids
        assert transaction["merchant_id"] in merchant_ids
