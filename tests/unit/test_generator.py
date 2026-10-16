import random

from src.generator.generate_transactions import (
    generate_customers,
    generate_merchants,
    generate_transactions,
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
