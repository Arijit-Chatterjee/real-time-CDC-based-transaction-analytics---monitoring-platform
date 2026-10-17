"""Generate synthetic retail data for StreamGuard."""

import argparse
import json
import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4


def generate_customers(count: int = 5) -> list[dict]:
    """Create fake customers for testing."""
    regions = ["Kolkata", "Mumbai", "Delhi", "Chennai"]

    customers = []

    for customer_id in range(1, count + 1):
        customer = {
            "customer_id": customer_id,
            "home_region": random.choice(regions),
            "account_status": "active",
            "created_at": datetime.now(UTC).isoformat(),
        }

        customers.append(customer)

    return customers


def generate_merchants(count: int = 500) -> list[dict]:
    """Create fake merchants with unique IDs and names."""
    merchant_names = [
        ("Fresh Mart", "grocery"),
        ("Tech World", "electronics"),
        ("Style Hub", "clothing"),
        ("Daily Needs", "convenience"),
        ("Book Corner", "books"),
    ]

    regions = ["Kolkata", "Mumbai", "Delhi", "Chennai"]
    merchants = []

    for merchant_id in range(1, count + 1):
        base_name, category = merchant_names[(merchant_id - 1) % len(merchant_names)]

        merchant = {
            "merchant_id": merchant_id,
            "merchant_name": f"{base_name} {merchant_id:03d}",
            "merchant_category": category,
            "merchant_region": random.choice(regions),
            "created_at": datetime.now(UTC).isoformat(),
        }

        merchants.append(merchant)

    return merchants


def generate_transactions(
    customers: list[dict],
    merchants: list[dict],
    count: int = 10,
) -> list[dict]:
    """Create fake retail transactions."""
    if not customers:
        raise ValueError("customers cannot be empty")

    if not merchants:
        raise ValueError("merchants cannot be empty")

    if count < 0:
        raise ValueError("count cannot be negative")
    transactions = []
    base_time = datetime.now(UTC)
    for transaction_id in range(1, count + 1):
        customer = random.choice(customers)
        merchant = random.choice(merchants)

        transaction = {
            "transaction_id": str(uuid4()),
            "customer_id": customer["customer_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": round(
                random.uniform(100, 50000 if random.random() < 0.05 else 5000),
                2,
            ),
            "currency": "INR",
            "region": customer["home_region"],
            "status": random.choice(["approved", "declined"]),
            "event_time": (base_time + timedelta(seconds=transaction_id)).isoformat(),
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        transactions.append(transaction)

    return transactions


def save_records(
    records: list[dict],
    output_path: str,
    append: bool = False,
) -> None:
    """Save records as JSON Lines."""
    mode = "a" if append else "w"

    with open(output_path, mode, encoding="utf-8") as file:
        file.writelines(json.dumps(record) + "\n" for record in records)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail transactions for StreamGuard."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100000,
        help="Number of transactions to generate (default: 100000).",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=10000,
        help="Number of customers to generate (default: 10000).",
    )
    parser.add_argument(
        "--merchants",
        type=int,
        default=500,
        help="Number of merchants to generate (default: 500).",
    )
    parser.add_argument(
        "--output",
        default="data/transactions.jsonl",
        help="Path to the output JSONL file.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append transactions to the output file instead of overwriting it.",
    )
    args = parser.parse_args()
    if args.customers <= 0:
        parser.error("--customers must be greater than 0")

    if args.merchants <= 0:
        parser.error("--merchants must be greater than 0")

    if args.count < 0:
        parser.error("--count cannot be negative")

    random.seed(42)

    customers = generate_customers(args.customers)
    merchants = generate_merchants(args.merchants)
    transactions = generate_transactions(
        customers,
        merchants,
        args.count,
    )
    save_records(customers, "data/customers.jsonl")
    save_records(merchants, "data/merchants.jsonl")
    save_records(transactions, args.output)

    print("Saved customers to data/customers.jsonl")
    print("Saved merchants to data/merchants.jsonl")
    print(f"Customers generated: {len(customers)}")
    print(f"Merchants generated: {len(merchants)}")
    print(f"Transactions generated: {len(transactions)}")

    save_records(transactions, args.output, append=args.append)
    print(f"Saved transactions to {args.output}")


if __name__ == "__main__":
    main()
